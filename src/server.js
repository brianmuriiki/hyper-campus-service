import express from 'express'
import cors from 'cors'
import { supabaseAdmin } from './supabaseAdmin.js'
import { extractText, chunkText } from './extract.js'
import { embedTexts } from './embeddings.js'
import { getProvider } from './aiProvider.js'

const app = express()
app.use(cors())
app.use(express.json())

// --- Ingestion: called right after a frontend upload finishes ---
app.post('/ingest', async (req, res) => {
  const { fileId } = req.body
  res.json({ status: 'started' }) // respond immediately, process in background

  try {
    const { data: file } = await supabaseAdmin
      .from('files')
      .select('*')
      .eq('id', fileId)
      .single()
    if (!file) return

    await supabaseAdmin
      .from('files')
      .update({ ingestion_status: 'processing' })
      .eq('id', fileId)

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

    await supabaseAdmin
      .from('files')
      .update({ ingestion_status: 'ready' })
      .eq('id', fileId)
  } catch (err) {
    console.error('Ingestion failed:', err)
    await supabaseAdmin
      .from('files')
      .update({ ingestion_status: 'failed', failure_reason: String(err.message).slice(0, 200) })
      .eq('id', fileId)
  }
})

// --- Chat: RAG retrieval + streamed generation ---
app.post('/chat/message', async (req, res) => {
  const { sessionId, userId, unitId, message, model } = req.body
  const t0 = Date.now()

  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')

  try {
    const [queryEmbedding] = await embedTexts([message], 'retrieval.query')
    console.log(`Embedding took ${Date.now() - t0}ms`)

    const { data: matches } = await supabaseAdmin.rpc('match_chunks', {
      query_embedding: queryEmbedding,
      match_owner_id: userId,
      match_unit_id: unitId || null,
      match_count: 5,
    })

    const context = (matches || [])
      .map((m, i) => `[${i + 1}] ${m.content}`)
      .join('\n\n')

    const systemPrompt = context
      ? `You are a helpful study assistant for a student. Below are some relevant excerpts from the student's own uploaded notes — use them when they're relevant to the question, and mention when you're drawing on them (e.g. "based on your notes..."). For anything the notes don't cover, or general questions unrelated to their notes (including casual conversation), answer normally using your own knowledge — don't refuse or claim you can't help just because the notes don't mention it.\n\nRelevant notes:\n${context}`
      : `You are a helpful study assistant for a student. No specifically relevant notes were found for this question, so just answer normally using your own knowledge. If the question sounds like it's about their coursework and they haven't uploaded anything relevant yet, you can gently mention that uploading their notes to Repository would let you reference their own material — but still answer the question itself.`

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
    })

    const tBeforeGen = Date.now()
    const provider = getProvider()
    let fullText = ''
    let firstTokenTime = null

    for await (const token of provider(systemPrompt, messages, model)) {
      if (!firstTokenTime) {
        firstTokenTime = Date.now()
        console.log(`Time to first token: ${firstTokenTime - tBeforeGen}ms (${firstTokenTime - t0}ms total from request start)`)
      }
      fullText += token
      res.write(`data: ${JSON.stringify({ token })}\n\n`)
    }
    console.log(`Full response took ${Date.now() - tBeforeGen}ms generation, ${Date.now() - t0}ms total`)

    await supabaseAdmin.from('chat_messages').insert({
      session_id: sessionId,
      role: 'assistant',
      content: fullText,
      retrieved_chunk_ids: (matches || []).map((m) => m.id),
    })

    res.write(`data: ${JSON.stringify({ done: true, chunkIds: (matches || []).map((m) => m.id) })}\n\n`)
    res.end()
  } catch (err) {
    console.error('Chat error:', err)
    res.write(`data: ${JSON.stringify({ error: 'Something went wrong.' })}\n\n`)
    res.end()
  }
})

const port = process.env.PORT || 8787
app.listen(port, () => console.log(`Ingestion/chat service running on :${port}`))