import 'dotenv/config'

// Some free models (notably certain Nemotron variants) prepend a raw safety
// classifier verdict directly into the visible content, e.g.:
//   "User Safety: safe\nResponse Safety: safe\n\n<actual answer>"
// OpenRouter doesn't strip this for us, so we filter it out of the stream.
const SAFETY_PREAMBLE_PATTERN = /^(User Safety:\s*\w+\s*\n)(Response Safety:\s*\w+\s*\n)?\n*/i

async function* callOpenRouter(systemPrompt, messages, model, signal) {
  const response = await fetch('https://openrouter.ai/api/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
      'HTTP-Referer': 'https://your-app-domain.com',
      'X-Title': 'HYPER-CAMPUS',
    },
    body: JSON.stringify({
      model,
      messages: [{ role: 'system', content: systemPrompt }, ...messages],
      stream: true,
    }),
    signal,
  })

  if (!response.ok) {
    const errBody = await response.text().catch(() => '')
    const err = new Error(`OpenRouter ${response.status} for model "${model}": ${errBody}`)
    err.status = response.status
    throw err
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    const lines = buffer.split('\n')
    buffer = lines.pop()

    for (const line of lines) {
      if (!line.startsWith('data: ')) continue
      const payload = line.slice(6)
      if (payload === '[DONE]') return
      try {
        const parsed = JSON.parse(payload)
        const token = parsed.choices?.[0]?.delta?.content
        if (token) yield token
      } catch {
        // ignore malformed SSE lines
      }
    }
  }
}

// Wraps any provider's raw token stream and strips a leading safety-classifier
// preamble, if present, before anything reaches the client. Buffers just enough
// of the start of the response to check — everything after that streams through untouched.
async function* stripSafetyPreamble(tokenStream) {
  let checkBuffer = ''
  let checked = false

  for await (const token of tokenStream) {
    if (checked) {
      yield token
      continue
    }

    checkBuffer += token

    // Wait for enough text to confidently check, or a clear break in the pattern
    if (checkBuffer.length < 60 && !checkBuffer.includes('\n\n')) continue

    const match = checkBuffer.match(SAFETY_PREAMBLE_PATTERN)
    checked = true
    if (match) {
      const stripped = checkBuffer.slice(match[0].length)
      if (stripped) yield stripped
    } else {
      yield checkBuffer
    }
  }

  // Handle the case where the whole response was shorter than the check threshold
  if (!checked && checkBuffer) {
    const match = checkBuffer.match(SAFETY_PREAMBLE_PATTERN)
    yield match ? checkBuffer.slice(match[0].length) : checkBuffer
  }
}

async function* openRouterProvider(systemPrompt, messages, model, signal) {
  const requested = model || process.env.OPENROUTER_MODEL || 'openrouter/free'

  try {
    yield* stripSafetyPreamble(callOpenRouter(systemPrompt, messages, requested, signal))
  } catch (err) {
    if (err.name === 'AbortError') throw err

    const isRecoverable = err.status === 400 || err.status === 429
    if (!isRecoverable || requested === 'openrouter/free') throw err

    console.log(`"${requested}" failed (${err.status}), silently falling back to Auto`)
    yield* stripSafetyPreamble(callOpenRouter(systemPrompt, messages, 'openrouter/free', signal))
  }
}

const PROVIDERS = { openrouter: openRouterProvider }

export function getProvider() {
  const name = process.env.PROVIDER || 'openrouter'
  return PROVIDERS[name]
}