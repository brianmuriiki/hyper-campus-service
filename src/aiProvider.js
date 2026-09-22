import 'dotenv/config'

// Every provider implements the same shape: an async generator yielding text chunks.
// Swap which one runs via the PROVIDER env var — no other code needs to change.

async function* openRouterProvider(systemPrompt, messages, model) {
  const response = await fetch('https://openrouter.ai/api/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
      'HTTP-Referer': 'https://your-app-domain.com',
      'X-Title': 'HYPER-CAMPUS',
    },
    body: JSON.stringify({
      model: model || process.env.OPENROUTER_MODEL || 'openrouter/free',
      messages: [{ role: 'system', content: systemPrompt }, ...messages],
      stream: true,
    }),
  })

  console.log('OpenRouter response status:', response.status, response.ok)
  if (!response.ok) {
    const errText = await response.text()
    console.log('OpenRouter error body:', errText)
    return
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

const PROVIDERS = { openrouter: openRouterProvider }

export function getProvider() {
  const name = process.env.PROVIDER || 'openrouter'
  return PROVIDERS[name]
}