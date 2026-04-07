import { Worker } from 'bullmq';
import unzipper from 'unzipper';
import { BedrockRuntimeClient, InvokeModelCommand } from '@aws-sdk/client-bedrock-runtime';
import pdf from 'pdf-parse';
import { PDFDocument } from 'pdf-lib';
import {
  getWorkerConnection, isUsingInMemoryQueue,
  broadcastToJob, updateJobStatus, registerJobHandler
} from '../services/queueService.js';
import { getFromS3, uploadToS3, getJsonFromS3, putJsonToS3 } from '../services/s3Service.js';
import { generateAuditReport } from '../services/excelService.js';

const CONCURRENCY     = 50;
const MAX_DIRECT_SIZE = 10 * 1024 * 1024;
const TEXT_CHUNK_SIZE = 40000;
const MAX_PAGES_BATCH = 5;
const REGION          = process.env.AWS_REGION || 'ap-south-1';
const bedrockClient   = new BedrockRuntimeClient({ region: REGION });
let worker = null;

/* ── STREAM TO BUFFER ── */
async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}

/* ── LOGS ── */
const logCache = new Map();
async function saveJobLog(jobId, time, message, type = 'info') {
  if (!logCache.has(jobId)) logCache.set(jobId, []);
  logCache.get(jobId).push({ time, message, type, timestamp: Date.now() });
  if (logCache.get(jobId).length >= 20) await flushJobLogs(jobId);
}
async function flushJobLogs(jobId) {
  const logs = logCache.get(jobId);
  if (!logs?.length) return;
  try {
    let existing = [];
    try { existing = (await getJsonFromS3(`jobs/${jobId}/processing/logs.json`)).logs || []; } catch {}
    await putJsonToS3(`jobs/${jobId}/processing/logs.json`, {
      logs: [...existing, ...logs].slice(-1000),
      lastUpdated: new Date().toISOString()
    });
    logCache.set(jobId, []);
  } catch (err) { console.error('[Logs] flush failed:', err.message); }
}
const finalizeJobLogs = (jobId) => flushJobLogs(jobId);

/* ── CHECKPOINT ── */
async function isDocAlreadyDone(jobId, docIndex) {
  try {
    const r = await getJsonFromS3(`jobs/${jobId}/processing/results/${docIndex}.json`);
    return r.status === 'completed' || r.status === 'failed';
  } catch { return false; }
}

/* ── SYSTEM PROMPT ── */
async function getSystemPrompt() {
  const base = `Analyze this legal document and extract structured information.
Return ONLY a valid JSON object with these exact fields:
{"appl_no":"string","borrower_name":"string","property_address":"string","property_type":"string","state":"string","tsr_date":"string","ownership_title_chain_status":"string","encumbrances_adverse_entries":"string","subsequent_charges":"string","prior_charge_subsisting":"string","roc_charge_flag":"string","litigation_lis_pendens":"string","mutation_status":"string","revenue_municipal_dues":"string","land_use_zoning_status":"string","stamping_registration_issues":"string","mortgage_perfection_issues":"string","advocate_adverse_remarks":"string","risk_rating":"High|Medium|Low","enforceability_decision":"string","enforceability_rationale":"string","recommended_actions":"string","confidence_score":0.0}
No markdown. No explanation. Just JSON.`;
  try {
    const buf     = await streamToBuffer(await getFromS3('masters/legal_audit_prompt.json'));
    const masters = JSON.parse(buf.toString());
    return `You are a legal document audit assistant for a bank/NBFC.\n${masters.systemRole || ''}\n${base}`;
  } catch {
    return `You are a legal document audit assistant for a bank/NBFC.\n${base}`;
  }
}

/* ── BEDROCK CALL ── */
function parseJsonResponse(text) {
  try { return JSON.parse(text); } catch {
    const m = text.match(/\{[\s\S]*\}/);
    if (m) { try { return JSON.parse(m[0]); } catch {} }
    return null;
  }
}

async function callBedrock(content, mediaType, systemPrompt, retries = 3) {
  let messageContent;
  if (mediaType === 'pdf') {
    messageContent = [
      { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: content } },
      { type: 'text', text: systemPrompt }
    ];
  } else if (mediaType.startsWith('image/')) {
    messageContent = [
      { type: 'image', source: { type: 'base64', media_type: mediaType, data: content } },
      { type: 'text', text: systemPrompt }
    ];
  } else {
    messageContent = [{ type: 'text', text: `${systemPrompt}\n\n${content}` }];
  }

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const cmd = new InvokeModelCommand({
        modelId: 'global.anthropic.claude-sonnet-4-6',
        contentType: 'application/json', accept: 'application/json',
        body: JSON.stringify({
          anthropic_version: 'bedrock-2023-05-31',
          max_tokens: 4096,
          messages: [{ role: 'user', content: messageContent }]
        })
      });
      const result   = await bedrockClient.send(cmd);
      const response = JSON.parse(new TextDecoder().decode(result.body));
      return {
        text:         response.content?.[0]?.text || '',
        inputTokens:  response.usage?.input_tokens  || 0,
        outputTokens: response.usage?.output_tokens || 0
      };
    } catch (err) {
      const throttle = err.message?.includes('ThrottlingException') ||
                       err.message?.includes('Rate exceeded') ||
                       err.message?.includes('Too Many Requests');
      if (attempt < retries) {
        const wait = throttle ? 12000 * attempt : 3000 * attempt;
        console.warn(`[Bedrock] retry ${attempt}/${retries} in ${wait}ms`);
        await new Promise(r => setTimeout(r, wait));
      } else throw err;
    }
  }
}

/* ── PROCESS ONE DOCUMENT (Buffer-based) ── */
async function processDocument(docBuffer, docType, systemPrompt) {
  let chunks = [], totalInput = 0, totalOutput = 0;

  if (docType === '.pdf') {
    if (docBuffer.length <= MAX_DIRECT_SIZE) {
      const { text, inputTokens, outputTokens } =
        await callBedrock(docBuffer.toString('base64'), 'pdf', systemPrompt);
      totalInput += inputTokens; totalOutput += outputTokens;
      const d = parseJsonResponse(text); if (d) chunks.push(d);
    } else {
      let pdfData; try { pdfData = await pdf(docBuffer); } catch {}
      const hasText = pdfData?.text?.trim().length > 500;

      if (hasText) {
        const paragraphs = pdfData.text.split(/\n\s*\n/);
        let cur = ''; const parts = [];
        for (const p of paragraphs) {
          if (cur.length + p.length > TEXT_CHUNK_SIZE && cur.length > 0) { parts.push(cur.trim()); cur = p; }
          else cur += (cur ? '\n\n' : '') + p;
        }
        if (cur.trim()) parts.push(cur.trim());
        for (const part of parts) {
          const { text, inputTokens, outputTokens } = await callBedrock(part, 'text', systemPrompt);
          totalInput += inputTokens; totalOutput += outputTokens;
          const d = parseJsonResponse(text); if (d) chunks.push(d);
          await new Promise(r => setTimeout(r, 500));
        }
      } else {
        const pdfDoc   = await PDFDocument.load(docBuffer, { ignoreEncryption: true });
        const maxPages = Math.min(pdfDoc.getPageCount(), 500);
        for (let i = 0; i < maxPages; i += MAX_PAGES_BATCH) {
          const idx = [];
          for (let j = i; j < Math.min(i + MAX_PAGES_BATCH, maxPages); j++) idx.push(j);
          const batch = await PDFDocument.create();
          (await batch.copyPages(pdfDoc, idx)).forEach(p => batch.addPage(p));
          const base64 = Buffer.from(await batch.save()).toString('base64');
          const { text, inputTokens, outputTokens } = await callBedrock(base64, 'pdf', systemPrompt);
          totalInput += inputTokens; totalOutput += outputTokens;
          const d = parseJsonResponse(text); if (d) chunks.push(d);
          await new Promise(r => setTimeout(r, 500));
        }
      }
    }
  } else if (['.png', '.jpg', '.jpeg'].includes(docType)) {
    const mt = docType === '.png' ? 'image/png' : 'image/jpeg';
    const { text, inputTokens, outputTokens } =
      await callBedrock(docBuffer.toString('base64'), mt, systemPrompt);
    totalInput += inputTokens; totalOutput += outputTokens;
    const d = parseJsonResponse(text); if (d) chunks.push(d);
  } else {
    const { text, inputTokens, outputTokens } =
      await callBedrock(docBuffer.toString('utf-8'), 'text', systemPrompt);
    totalInput += inputTokens; totalOutput += outputTokens;
    const d = parseJsonResponse(text); if (d) chunks.push(d);
  }

  if (!chunks.length) return { success: false, data: null, totalInput, totalOutput };
  if (chunks.length === 1) return { success: true, data: chunks[0], totalInput, totalOutput };

  const rp = ['High', 'Medium', 'Low', 'Manual Review Required', 'Unknown'];
  const merged = { ...chunks[0] };
  for (let i = 1; i < chunks.length; i++) {
    const r = chunks[i];
    const ci = rp.indexOf(merged.risk_rating), ni = rp.indexOf(r.risk_rating);
    if (ni !== -1 && (ci === -1 || ni < ci)) merged.risk_rating = r.risk_rating;
    ['property_address','enforceability_rationale','recommended_actions','advocate_adverse_remarks']
      .forEach(f => {
        if (r[f] && r[f] !== 'Unknown' && merged[f] && !merged[f].includes(r[f]))
          merged[f] = `${merged[f]}; ${r[f]}`;
      });
  }
  const scores = chunks.map(r => r.confidence_score).filter(s => typeof s === 'number');
  if (scores.length) merged.confidence_score = Math.round(scores.reduce((a,b)=>a+b,0)/scores.length);
  return { success: true, data: merged, totalInput, totalOutput };
}

/* ── PROCESS ONE DOC WRAPPER ── */
async function processOneDocument(jobId, doc, docIndex, totalDocs, systemPrompt) {
  // Skip already-done docs on resume
  if (await isDocAlreadyDone(jobId, docIndex)) {
    console.log(`[Worker] Skip (done) ${docIndex}: ${doc.name}`);
    try {
      const ex = await getJsonFromS3(`jobs/${jobId}/processing/results/${docIndex}.json`);
      return { success: ex.status === 'completed', docName: doc.name, input: ex.tokenDetails?.input || 0, output: ex.tokenDetails?.output || 0, skipped: true };
    } catch { return { success: true, docName: doc.name, input: 0, output: 0, skipped: true }; }
  }

  try {
    // CRITICAL FIX: convert S3 stream to Buffer before processing
    const docStream = await getFromS3(doc.key);
    const docBuffer = await streamToBuffer(docStream);

    const { success, data, totalInput, totalOutput } =
      await processDocument(docBuffer, doc.type, systemPrompt);

    const resultKey = `jobs/${jobId}/processing/results/${docIndex}.json`;

    if (success && data && data.confidence_score > 0) {
      data.document_name = doc.name;
      data.processed_at  = new Date().toISOString();
      await putJsonToS3(resultKey, {
        status: 'completed', documentName: doc.name, documentIndex: docIndex,
        data, tokenDetails: { input: totalInput, output: totalOutput }
      });
      console.log(`[Worker] ${doc.name} (${docIndex+1}/${totalDocs}) Risk: ${data.risk_rating}`);
      return { success: true, docName: doc.name, input: totalInput, output: totalOutput };
    } else {
      await putJsonToS3(resultKey, {
        status: 'failed', documentName: doc.name, documentIndex: docIndex,
        reason: 'Zero confidence or no data', data: data || null
      });
      return { success: false, docName: doc.name, input: totalInput, output: totalOutput };
    }
  } catch (err) {
    console.error(`[Worker] ${doc.name}:`, err.message);
    try {
      await putJsonToS3(`jobs/${jobId}/processing/results/${docIndex}.json`, {
        status: 'failed', documentName: doc.name, documentIndex: docIndex, reason: err.message
      });
    } catch {}
    return { success: false, docName: doc.name, input: 0, output: 0 };
  }
}

/* ── SLIDING CONCURRENCY POOL ── */
async function runWithConcurrency(tasks, concurrency, onComplete) {
  let idx = 0;
  const results = new Array(tasks.length);
  async function runNext() {
    if (idx >= tasks.length) return;
    const i    = idx++;
    results[i] = await tasks[i]();
    if (onComplete) onComplete(results.filter(Boolean).length, tasks.length, results[i]);
    await runNext();
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, tasks.length) }, runNext));
  return results;
}

/* ── JOB PROCESSOR ── */
async function processJob(job) {
  const { jobId, type } = job.data;
  console.log(`[Worker] Job ${job.id} | type: ${type} | jobId: ${jobId}`);
  try {
    switch (type) {
      case 'extract':         await processExtraction(jobId, job);       break;
      case 'analyze':         await processAnalysis(jobId, job);         break;
      case 'generate-report': await processReportGeneration(jobId, job); break;
      default: throw new Error(`Unknown job type: ${type}`);
    }
  } catch (err) {
    console.error(`[Worker] Job ${job.id} failed:`, err);
    broadcastToJob(jobId, 'error', { message: err.message });
    throw err;
  }
}

/* ── EXTRACTION ── */
async function processExtraction(jobId, job) {
  const ts = () => new Date().toISOString().split('T')[1].split('.')[0];
  await saveJobLog(jobId, ts(), 'Starting ZIP extraction...');
  broadcastToJob(jobId, 'log', { time: ts(), message: 'Starting ZIP extraction...' });

  const supported = ['.pdf', '.png', '.jpg', '.jpeg', '.docx'];
  const documents = [];
  let extracted = 0;

  try {
    const zipStream = await getFromS3(`jobs/${jobId}/uploads/raw/documents.zip`);
    broadcastToJob(jobId, 'log', { time: ts(), message: 'Connected to ZIP, streaming...' });
    if (job.updateProgress) await job.updateProgress(5);

    const zip = zipStream.pipe(unzipper.Parse({ forceStream: true }));
    for await (const entry of zip) {
      const name = entry.path.split('/').pop().split('\\').pop();
      if (entry.type === 'Directory' || !name ||
          name.startsWith('.') || name.startsWith('__') || name === 'Thumbs.db') {
        entry.autodrain(); continue;
      }
      const ext = name.toLowerCase().substring(name.lastIndexOf('.'));
      if (!supported.includes(ext)) { entry.autodrain(); continue; }
      try {
        const bufs = []; for await (const c of entry) bufs.push(c);
        const content = Buffer.concat(bufs);
        const key     = `jobs/${jobId}/uploads/extracted/${name}`;
        await uploadToS3(key, content);
        documents.push({ name, key, type: ext, size: content.length, status: 'pending' });
        extracted++;
        if (extracted % 50 === 0 || extracted <= 5) {
          const msg = `Extracted ${extracted} docs (${name}, ${(content.length/1024/1024).toFixed(2)}MB)...`;
          await saveJobLog(jobId, ts(), msg);
          broadcastToJob(jobId, 'log', { time: ts(), message: msg });
        }
        bufs.length = 0;
      } catch (e) { console.error(`[Extract] ${name}:`, e.message); }
    }

    const doneMsg = `Extraction complete. ${documents.length} documents found.`;
    await saveJobLog(jobId, ts(), doneMsg);
    broadcastToJob(jobId, 'log', { time: ts(), message: doneMsg });

    await putJsonToS3(`jobs/${jobId}/processing/queue.json`, {
      totalDocuments: documents.length, processedCount: 0,
      documents, results: [], failedDocuments: [], status: 'ready'
    });

    if (job.updateProgress) await job.updateProgress(60);
    updateJobStatus(jobId, { status: 'extracted', totalDocuments: documents.length, processedCount: 0 });
    broadcastToJob(jobId, 'extraction-complete', { totalDocuments: documents.length });
    broadcastToJob(jobId, 'log', { time: ts(), message: 'Auto-starting document analysis...' });
    await processAnalysis(jobId, job);
  } catch (err) {
    console.error('[Extract] FATAL:', err);
    broadcastToJob(jobId, 'log', { time: ts(), message: `Extraction failed: ${err.message}` });
    throw err;
  }
}

/* ── ANALYSIS ── */
async function processAnalysis(jobId, job) {
  const ts = () => new Date().toISOString().split('T')[1].split('.')[0];

  const queueData   = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
  const pendingDocs = queueData.documents.filter(d => d.status === 'pending');

  if (pendingDocs.length === 0) {
    broadcastToJob(jobId, 'log', { time: ts(), message: 'No pending documents — generating report.' });
    await processReportGeneration(jobId, job);
    return;
  }

  const systemPrompt = await getSystemPrompt();

  // Count already done via checkpoints
  let alreadyDone = 0;
  for (let i = 0; i < pendingDocs.length; i++) {
    if (await isDocAlreadyDone(jobId, i)) alreadyDone++;
  }

  const startMsg = alreadyDone > 0
    ? `Resuming — ${alreadyDone} already done, ${pendingDocs.length - alreadyDone} remaining, ${CONCURRENCY} concurrent`
    : `Direct Bedrock — ${pendingDocs.length} docs, ${CONCURRENCY} concurrent`;

  await saveJobLog(jobId, ts(), startMsg);
  broadcastToJob(jobId, 'log', { time: ts(), message: startMsg });
  updateJobStatus(jobId, { status: 'processing' });

  let successCount = 0, failCount = 0, totalInput = 0, totalOutput = 0;

  const tasks = pendingDocs.map((doc, idx) => async () =>
    processOneDocument(jobId, doc, idx, pendingDocs.length, systemPrompt)
  );

  await runWithConcurrency(tasks, CONCURRENCY, (done, total, result) => {
    if (result?.success) successCount++; else failCount++;
    totalInput  += result?.input  || 0;
    totalOutput += result?.output || 0;
    broadcastToJob(jobId, 'progress', { current: done, total, percentage: Math.round((done/total)*100) });
    if (done % 10 === 0 || done === total) {
      const msg = `[${done}/${total}] ${successCount} processed, ${failCount} flagged`;
      saveJobLog(jobId, ts(), msg).catch(() => {});
      broadcastToJob(jobId, 'log', { time: ts(), message: msg });
    }
  });

  // Collect results
  const results = [], failedDocuments = [];
  for (let i = 0; i < pendingDocs.length; i++) {
    try {
      const r = await getJsonFromS3(`jobs/${jobId}/processing/results/${i}.json`);
      if (r.status === 'completed' && r.data) results.push(r.data);
      else {
        failedDocuments.push({ name: r.documentName || pendingDocs[i].name, reason: r.reason || 'Failed' });
        if (r.data) results.push(r.data);
      }
    } catch { failedDocuments.push({ name: pendingDocs[i].name, reason: 'Result missing' }); }
  }

  queueData.results = results; queueData.failedDocuments = failedDocuments;
  queueData.processedCount = pendingDocs.length;
  queueData.totalTokensInput = totalInput; queueData.totalTokensOutput = totalOutput;
  queueData.status = 'analysis-complete';
  queueData.documents = queueData.documents.map(doc => ({
    ...doc, status: failedDocuments.find(f => f.name === doc.name) ? 'failed' : 'completed'
  }));
  await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);

  const totalTok = totalInput + totalOutput;
  const doneMsg  = `Analysis complete. ${results.length} processed, ${failedDocuments.length} flagged.`;
  const tokenMsg = `Tokens: ${totalTok.toLocaleString()} (${totalInput.toLocaleString()} in, ${totalOutput.toLocaleString()} out)`;

  await saveJobLog(jobId, ts(), doneMsg, 'success');
  await saveJobLog(jobId, ts(), tokenMsg, 'info');
  await finalizeJobLogs(jobId);

  updateJobStatus(jobId, {
    status: 'analysis-complete', processedCount: queueData.processedCount,
    failedCount: failedDocuments.length, totalTokensInput: totalInput, totalTokensOutput: totalOutput
  });
  broadcastToJob(jobId, 'log', { time: ts(), message: doneMsg });
  broadcastToJob(jobId, 'log', { time: ts(), message: tokenMsg });
  broadcastToJob(jobId, 'analysis-complete', {
    processed: results.length, failed: failedDocuments.length,
    totalTokensInput: totalInput, totalTokensOutput: totalOutput
  });
  broadcastToJob(jobId, 'log', { time: ts(), message: 'Auto-starting report generation...' });
  await processReportGeneration(jobId, job);
}

/* ── REPORT GENERATION ── */
async function processReportGeneration(jobId, job) {
  const ts = () => new Date().toISOString().split('T')[1].split('.')[0];
  broadcastToJob(jobId, 'log', { time: ts(), message: 'Generating Excel report...' });
  updateJobStatus(jobId, { status: 'generating-report' });

  const queueData   = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
  const reportKey   = await generateAuditReport(jobId, queueData.results, queueData.failedDocuments);
  const completedAt = new Date().toISOString();

  Object.assign(queueData, { status: 'completed', reportKey, completedAt });
  await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);

  try {
    const meta = await getJsonFromS3(`jobs/${jobId}/metadata.json`);
    Object.assign(meta, { status: 'completed', completedAt, reportKey,
      processedCount: queueData.processedCount,
      totalDocuments: queueData.totalDocuments,
      failedCount: queueData.failedDocuments?.length || 0
    });
    await putJsonToS3(`jobs/${jobId}/metadata.json`, meta);
  } catch (e) { console.error('[Report] metadata update failed:', e.message); }

  updateJobStatus(jobId, { status: 'completed', reportKey, completedAt,
    processedCount: queueData.processedCount,
    totalDocuments: queueData.totalDocuments
  });

  await saveJobLog(jobId, ts(), 'Report generated!', 'success');
  await saveJobLog(jobId, ts(), 'All processing complete!', 'success');
  await finalizeJobLogs(jobId);

  broadcastToJob(jobId, 'log',      { time: ts(), message: 'Report generated!' });
  broadcastToJob(jobId, 'log',      { time: ts(), message: 'All processing complete!' });
  broadcastToJob(jobId, 'complete', {
    reportKey, completedAt,
    stats: {
      total:        queueData.results.length,
      high:         queueData.results.filter(r => r.risk_rating === 'High').length,
      medium:       queueData.results.filter(r => r.risk_rating === 'Medium').length,
      low:          queueData.results.filter(r => r.risk_rating === 'Low').length,
      manualReview: queueData.failedDocuments.length
    }
  });
}

/* ── WORKER INIT ── */
export async function initializeWorker() {
  registerJobHandler('document-processing', processJob);
  if (isUsingInMemoryQueue()) { console.log('In-memory queue'); return null; }
  const conn = getWorkerConnection();
  if (!conn) { console.log('In-memory queue (no Redis)'); return null; }

  try {
    await new Promise((res, rej) => {
      if (conn.status === 'ready') { res(); return; }
      const t = setTimeout(() => rej(new Error('timeout')), 15000);
      conn.once('ready', () => { clearTimeout(t); res(); });
      conn.once('error', e  => { clearTimeout(t); rej(e); });
    });

    worker = new Worker('document-processing', processJob, {
      connection: conn, concurrency: 1,
      lockDuration: 1800000, stalledInterval: 600000,
      maxStalledCount: 5, lockRenewTime: 300000
    });

    worker.on('completed', j     => console.log(`[Worker] Job ${j.id} done`));
    worker.on('failed',    (j,e) => console.error(`[Worker] Job ${j?.id} failed:`, e.message));
    worker.on('stalled',   id    => console.warn(`[Worker] Job ${id} stalled`));
    worker.on('error',     e     => console.error(`[Worker] Error:`, e.message));

    console.log(`Worker ready — direct Bedrock, ${CONCURRENCY} concurrent, stream-to-buffer fix, checkpoint resume`);
    return worker;
  } catch (err) {
    console.warn('Worker init failed:', err.message);
    return null;
  }
}

export { worker };

console.log('[Worker] Starting...');
initializeWorker()
  .then(() => console.log('[Worker] Ready'))
  .catch(e  => { console.error('[Worker] Fatal:', e); process.exit(1); });

process.on('SIGTERM', () => { console.log('[Worker] SIGTERM'); process.exit(0); });
process.on('SIGINT',  () => { console.log('[Worker] SIGINT');  process.exit(0); });
setInterval(() => {}, 1000 * 60 * 60);