import express from 'express'
import cors from 'cors'
import { supabaseAdmin } from './supabaseAdmin.js'
import { extractText, chunkText } from './extract.js'
import { embedTexts } from './embeddings.js'
import { getProvider } from './aiProvider.js'
import { createRoomToken } from './livekit.js'

const app = express()
app.use(cors())
app.use(express.json())

// --- Ingestion: called right after a frontend upload finishes ---
app.post('/ingest', async (req, res) => {
  const { fileId } = req.body
  res.json({ status: 'started' })

  try {
    const { data: file } = await supabaseAdmin.from('files').select('*').eq('id', fileId).single()
    if (!file) return

    await supabaseAdmin.from('files').update({ ingestion_status: 'processing' }).eq('id', fileId)

    const { data: fileBlob, error: downloadError } = await supabaseAdmin.storage
      .from('repository-files')
      .download(file.storage_key)
    if (downloadError) throw downloadError

    const buffer = Buffer.from(await fileBlob.arrayBuffer())
    const text = await extractText(buffer, file.file_type, file.original_filename)
    const chunks = chunkText(text)

    if (chunks.length === 0) throw new Error('No extractable text found')

    const embeddings = await embedTexts(chunks, 'retrieval.passage')

    const rows = chunks.map((content, i) => ({
      file_id: file.id,
      unit_id: file.unit_id,
      owner_id: file.uploader_id,
      content,
      embedding: embeddings[i],
      source_location: `chunk ${i + 1}`,
    }))

    const { error: insertError } = await supabaseAdmin.from('chunks').insert(rows)
    if (insertError) throw insertError

    await supabaseAdmin.from('files').update({ ingestion_status: 'ready' }).eq('id', fileId)
  } catch (err) {
    console.error('Ingestion failed:', err)
    await supabaseAdmin
      .from('files')
      .update({ ingestion_status: 'failed', failure_reason: String(err.message).slice(0, 200) })
      .eq('id', fileId)
  }
})

// --- Chat: RAG retrieval + attachment handling + streamed generation ---
app.post('/chat/message', async (req, res) => {
  const {
    sessionId, userId, unitId, message, model,
    attachmentStorageKey, attachmentName, attachmentType,
  } = req.body
  const t0 = Date.now()

  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')

  const abortController = new AbortController()
  res.on('close', () => {
    // res 'close' also fires after a normal res.end() — only treat this as
    // a real user-initiated stop if the response hadn't already finished.
    if (!res.writableEnded) abortController.abort()
  })

  try {
    const [queryEmbedding] = await embedTexts([message], 'retrieval.query')

    const { data: matches } = await supabaseAdmin.rpc('match_chunks', {
      query_embedding: queryEmbedding,
      match_owner_id: userId,
      match_unit_id: unitId || null,
      match_count: 5,
    })

    const context = (matches || []).map((m, i) => `[${i + 1}] ${m.content}`).join('\n\n')

    // Extract text from a freshly-attached file, if any — treated as
    // one-off context for this message, not saved into the searchable repository.
    let attachmentContext = ''
    if (attachmentStorageKey) {
      try {
        const { data: attBlob, error: attErr } = await supabaseAdmin.storage
          .from('chat-attachments')
          .download(attachmentStorageKey)
        if (!attErr) {
          const attBuffer = Buffer.from(await attBlob.arrayBuffer())
          const fileTypeMap = { 'application/pdf': 'pdf', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx', 'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'pptx' }
          const inferredType = fileTypeMap[attachmentType] || (attachmentType?.startsWith('image/') ? 'image' : 'pdf')
          const attText = await extractText(attBuffer, inferredType, attachmentName)
          attachmentContext = attText.slice(0, 6000) // keep prompt size reasonable
        }
      } catch (err) {
        console.error('Attachment extraction failed:', err)
      }
    }

    const systemPrompt = `You are a helpful study assistant for a student. ${context ? `Below are some relevant excerpts from the student's own uploaded notes — use them when relevant, and mention when you're drawing on them.\n\nRelevant notes:\n${context}` : "No specifically relevant repository notes were found for this question."}${attachmentContext ? `\n\nThe student just attached a file ("${attachmentName}") to this message. Its content:\n${attachmentContext}` : ''}\n\nFor anything the notes/attachment don't cover, or general questions (including casual conversation), answer normally using your own knowledge — don't refuse just because nothing matched.`

    const { data: history } = await supabaseAdmin
      .from('chat_messages')
      .select('role, content')
      .eq('session_id', sessionId)
      .order('created_at', { ascending: true })
      .limit(10)

    const messages = [...(history || []), { role: 'user', content: message }]

    await supabaseAdmin.from('chat_messages').insert({
      session_id: sessionId,
      role: 'user',
      content: message,
      attachment_storage_key: attachmentStorageKey || null,
      attachment_name: attachmentName || null,
      attachment_type: attachmentType || null,
    })

        // Auto-title on the first message of a session — only if it's still the default
    const { data: sessionRow, error: sessionFetchError } = await supabaseAdmin
      .from('chat_sessions')
      .select('title')
      .eq('id', sessionId)
      .single()

    console.log('Title check — current title:', sessionRow?.title, 'fetch error:', sessionFetchError)

    if (sessionRow?.title === 'New chat') {
      const generatedTitle = message.length > 48 ? `${message.slice(0, 48).trim()}…` : message
      const { error: titleUpdateError } = await supabaseAdmin
        .from('chat_sessions')
        .update({ title: generatedTitle })
        .eq('id', sessionId)
      console.log('Title update attempted:', generatedTitle, 'error:', titleUpdateError)
    }

    const provider = getProvider()
    let fullText = ''

    for await (const token of provider(systemPrompt, messages, model, abortController.signal)) {
      fullText += token
      res.write(`data: ${JSON.stringify({ token })}\n\n`)
    }

    // Only save/finish normally if the client didn't abort mid-stream
    if (!abortController.signal.aborted) {
      await supabaseAdmin.from('chat_messages').insert({
        session_id: sessionId,
        role: 'assistant',
        content: fullText,
        retrieved_chunk_ids: (matches || []).map((m) => m.id),
      })
      res.write(`data: ${JSON.stringify({ done: true })}\n\n`)
      res.end()
    }
  } catch (err) {
    if (err.name === 'AbortError') return // client stopped generation — nothing to report
    console.error('Chat error:', err)
    const errMessage =
      err.status === 429
        ? 'This model is temporarily overloaded. Try again in a moment, or pick a different model.'
        : err.status === 400
        ? "That model ID isn't valid anymore. Try a different one from the model picker."
        : 'Something went wrong generating a response.'
    try {
      res.write(`data: ${JSON.stringify({ error: errMessage })}\n\n`)
      res.end()
    } catch {
      // response already closed, nothing to do
    }
  }
})

app.post('/livekit/token', async (req, res) => {
  const { roomId, userId, userName } = req.body
  try {
    const token = await createRoomToken({ roomName: roomId, userId, userName })
    res.json({ token, url: process.env.LIVEKIT_URL })
  } catch (err) {
    console.error('Token generation failed:', err)
    res.status(500).json({ error: 'Could not create call token' })
  }
})

const port = process.env.PORT || 8787
app.listen(port, () => console.log(`Ingestion/chat service running on :${port}`))