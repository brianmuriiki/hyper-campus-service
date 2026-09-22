import 'dotenv/config'

// jina-embeddings-v3 supports Matryoshka truncation via `dimensions`,
// requesting 768 keeps this compatible with the existing vector(768) column
// (no new migration needed after the earlier Nomic-era resize).
export async function embedTexts(texts, taskType = 'retrieval.passage') {
  const response = await fetch('https://api.jina.ai/v1/embeddings', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${process.env.JINA_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'jina-embeddings-v3',
      task: taskType,
      dimensions: 768,
      input: texts,
    }),
  })

  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`Jina API error: ${response.status} ${body}`)
  }

  const data = await response.json()
  // Jina returns results possibly out of order; sort by index to be safe
  return data.data.sort((a, b) => a.index - b.index).map((d) => d.embedding)
}