import officeParser from 'officeparser'
import Tesseract from 'tesseract.js'

export async function extractText(buffer, fileType, filename) {
  if (fileType === 'pdf' || fileType === 'docx' || fileType === 'pptx') {
    return await officeParser.parseOfficeAsync(buffer)
  }

  if (fileType === 'image') {
    const { data } = await Tesseract.recognize(buffer, 'eng')
    return data.text
  }

  throw new Error(`Unsupported file type: ${fileType}`)
}

// Simple fixed-size chunking with overlap
export function chunkText(text, chunkSize = 800, overlap = 100) {
  const chunks = []
  let start = 0
  while (start < text.length) {
    const end = Math.min(start + chunkSize, text.length)
    const chunk = text.slice(start, end).trim()
    if (chunk.length > 20) chunks.push(chunk)
    start += chunkSize - overlap
  }
  return chunks
}