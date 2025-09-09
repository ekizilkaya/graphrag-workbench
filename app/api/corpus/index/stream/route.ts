// import { NextRequest } from 'next/server'
import { spawn } from 'node:child_process'
import path from 'node:path'
import fs from 'node:fs/promises'
import { convertPdfToText, convertGraphParquetToJson } from '@/lib/server/converters'

interface UploadEntry {
  name: string
  size: number
  mtime: number
  type: 'txt'|'pdf'
  status?: string
  prepared_at?: number
  indexed_at?: number
  removed_at?: number
}

export async function GET() {
  const encoder = new TextEncoder()
  const appDir = process.cwd()
  const root = appDir
  
  // OpenAI-only mode; ignore query parameter

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const send = (type: string, payload: Record<string, unknown>) => controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type, ...payload })}\n\n`))
      send('status', { message: 'Preparing dataset…' })
      // Setup environment based on mode
      const env = { ...process.env } as NodeJS.ProcessEnv

      // Pre-convert PDFs -> TXT if needed
      (async () => {
        try {
          const inputDir = path.join(root, 'input')
          const pdfArchive = path.join(inputDir, '_pdfs')
          await fs.mkdir(pdfArchive, { recursive: true }).catch(() => {})
          // 1) Convert any PDFs in input/
          const ents = await fs.readdir(inputDir, { withFileTypes: true }).catch(() => [])
          const pdfs = ents.filter(e => e.isFile() && e.name.toLowerCase().endsWith('.pdf')).map(e => e.name)
          for (const pdf of pdfs) {
            const base = pdf.replace(/\.pdf$/i, '')
            const txtPath = path.join(inputDir, `${base}.txt`)
            let need = false
            try { await fs.stat(txtPath); } catch { need = true }
            let converted = false
            if (need) {
              send('log', { line: `Converting PDF to text: ${pdf}` })
              try {
                await convertPdfToText(path.join(inputDir, pdf), inputDir)
                converted = true
              } catch (e) {
                send('log', { line: `PDF conversion failed: ${pdf} — ${String(e)}` })
              }
            }
            // Move PDF out of ingest folder only if converted or already had .txt
            if (converted || !need) {
              try {
                await fs.rename(path.join(inputDir, pdf), path.join(pdfArchive, pdf))
                send('log', { line: `Moved PDF to ${path.join('input', '_pdfs', pdf)}` })
              } catch {}
            }
          }

          // 2) Attempt conversion for any PDFs in input/_pdfs missing corresponding .txt
          const archived = await fs.readdir(pdfArchive, { withFileTypes: true }).catch(() => [])
          const archivedPdfs = archived.filter(e => e.isFile() && e.name.toLowerCase().endsWith('.pdf')).map(e => e.name)
          for (const pdf of archivedPdfs) {
            const base = pdf.replace(/\.pdf$/i, '')
            const txtPath = path.join(inputDir, `${base}.txt`)
            let need = false
            try { await fs.stat(txtPath); } catch { need = true }
            if (need) {
              send('log', { line: `Recovering: converting archived PDF to text: ${pdf}` })
              try {
                await convertPdfToText(path.join(pdfArchive, pdf), inputDir)
              } catch (e) {
                send('log', { line: `Archived PDF conversion failed: ${pdf} — ${String(e)}` })
              }
            }
          }

          // 3) Warn if no .txt files present
          try {
            const list = await fs.readdir(inputDir, { withFileTypes: true }).catch(() => [])
            const txts = list.filter(e => e.isFile() && e.name.toLowerCase().endsWith('.txt'))
            if (txts.length === 0) {
              send('log', { line: `No .txt files found in ${path.join('input')}. Upload PDFs or TXT files.` })
            }
          } catch {}
          // Update uploads registry statuses to 'scanning'
          const uploadsPath = path.join(root, 'output', 'uploads.json')
          const raw = await fs.readFile(uploadsPath, 'utf-8').catch(() => '[]')
          const parsed = JSON.parse(raw) as unknown
          const reg: UploadEntry[] = Array.isArray(parsed) ? parsed as UploadEntry[] : []
          const now = Date.now()
          for (const r of reg) { if (r.status !== 'removed') r.status = 'scanning'; r.prepared_at = now }
          await fs.mkdir(path.dirname(uploadsPath), { recursive: true })
          await fs.writeFile(uploadsPath, JSON.stringify(reg, null, 2))
        } catch (e) {
          send('log', { line: `dataset prep warning: ${String(e)}` })
        }
      })().then(async () => {
        // Start indexing after dataset is prepared
        const configFile = 'settings.yaml'
        const modeLabel = 'OpenAI (settings.yaml)'
        send('status', { message: `Indexing started with ${modeLabel}…` })
        const cmd = `"C:/Users/EMRE/anaconda3/envs/graphrag_env/python.exe" -m graphrag index --config ${configFile}`
        const child = spawn('bash', ['-lc', cmd], { cwd: root, env })
        const appendLog = async (line: string) => {
          try {
            const p = path.join(root, 'logs_history.log')
            await fs.appendFile(p, `${new Date().toISOString()}\t${line.replace(/\r?\n/g, '')}\n`)
          } catch {}
        }
        child.stdout.on('data', (chunk: Buffer) => {
          const text = chunk.toString()
          send('log', { line: text })
          text.split(/\r?\n/).forEach((ln) => { if (ln.trim()) appendLog(ln) })
          if (/Starting pipeline|Running standard indexing|Executing pipeline/.test(text)) send('progress', { value: 5 })
          if (/create_communities/.test(text)) send('progress', { value: 40 })
          if (/create_community_reports/.test(text)) send('progress', { value: 65 })
          if (/generate_text_embeddings/.test(text)) send('progress', { value: 85 })
          if (/Indexing pipeline complete|All workflows completed successfully/.test(text)) send('progress', { value: 100 })
        })
        child.stderr.on('data', (chunk: Buffer) => {
          const text = chunk.toString()
          send('log', { line: text })
          text.split(/\r?\n/).forEach((ln) => { if (ln.trim()) appendLog(ln) })
        })
        child.on('close', async (code) => {
          send('status', { message: 'Indexing finished' })
          try {
            const outDir = path.join(root, 'output')
            const { converted } = await convertGraphParquetToJson(outDir)
            send('log', { line: `Converted ${converted} parquet files to JSON` })
          } catch (e) {
            send('log', { line: `conversion error: ${String(e)}` })
          }
          send('done', { ok: true })
          try {
            if (code === 0) {
              const uploadsPath = path.join(root, 'output', 'uploads.json')
              const raw = await fs.readFile(uploadsPath, 'utf-8').catch(() => '[]')
              const parsed = JSON.parse(raw) as unknown
              const reg: UploadEntry[] = Array.isArray(parsed) ? parsed as UploadEntry[] : []
              const now = Date.now()
              for (const r of reg) { r.status = 'indexed'; r.indexed_at = now }
              await fs.writeFile(uploadsPath, JSON.stringify(reg, null, 2))
            }
          } catch {}
          controller.close()
        })
        child.on('error', (err) => {
          send('log', { line: `error: ${String(err)}` })
          send('done', { ok: false })
          controller.close()
        })
      }).catch(() => {
        // If prep fails, attempt to run index anyway to surface errors
        const configFile = 'settings.yaml'
        const modeLabel = 'OpenAI (settings.yaml)'
        send('status', { message: `Indexing started with ${modeLabel}…` })
        const cmd = `"C:/Users/EMRE/anaconda3/envs/graphrag_env/python.exe" -m graphrag index --config ${configFile}`
        spawn('bash', ['-lc', cmd], { cwd: root, env })
      });
    }
  })

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
    },
  })
}
