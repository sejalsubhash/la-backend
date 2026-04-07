import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import Busboy from 'busboy';
import { uploadStreamToS3, uploadToS3, getJsonFromS3, putJsonToS3, listS3Objects, getSignedDownloadUrl, getFromS3, streamToBuffer, getSignedUploadUrl } from '../services/s3Service.js';
import { queueManager, addSSEClient, removeSSEClient, getJobStatus, setJobStatus, updateJobStatus } from '../services/queueService.js';
import { getDocumentAnalysis } from '../services/claudeService.js';
import { analyzePdf } from '../services/pdfChunkService.js';

const router = express.Router();

// Create a new job
router.post('/create', async (req, res) => {
  try {
    const jobId = `${new Date().toISOString().split('T')[0].replace(/-/g, '')}_${Date.now()}_${uuidv4().slice(0, 8)}`;
    const jobData = {
      id: jobId, status: 'created',
      createdAt: new Date().toISOString(),
      createdBy: req.body.userEmail || 'unknown',
      totalDocuments: 0, processedCount: 0, failedCount: 0
    };
    await putJsonToS3(`jobs/${jobId}/metadata.json`, jobData);
    setJobStatus(jobId, jobData);
    res.json({ success: true, jobId, job: jobData });
  } catch (error) {
    console.error('Error creating job:', error);
    res.status(500).json({ error: 'Failed to create job' });
  }
});

// ─────────────────────────────────────────────────────────
// STEP 1 — Generate presigned URL for direct browser → S3
// Simple single PUT — fast, no multipart overhead
// ─────────────────────────────────────────────────────────
router.post('/:jobId/presign-upload', async (req, res) => {
  const { jobId } = req.params;
  const { fileName, contentType } = req.body;
  try {
    const s3Key     = `jobs/${jobId}/uploads/raw/documents.zip`;
    const uploadUrl = await getSignedUploadUrl(s3Key, contentType || 'application/zip', 3600);
    console.log(`[Presign] Generated upload URL for job: ${jobId}`);
    res.json({ success: true, uploadUrl, s3Key });
  } catch (error) {
    console.error('[Presign] Error:', error);
    res.status(500).json({ error: 'Failed to generate upload URL' });
  }
});

// ─────────────────────────────────────────────────────────
// STEP 2 — Confirm upload complete, update job + enqueue
// ─────────────────────────────────────────────────────────
router.post('/:jobId/confirm-upload', async (req, res) => {
  const { jobId } = req.params;
  const { s3Key, fileName, fileSize } = req.body;
  try {
    updateJobStatus(jobId, {
      status: 'uploaded', uploadedAt: new Date().toISOString(),
      fileName, fileSize
    });
    console.log(`[Confirm] Upload confirmed for job: ${jobId}, file: ${fileName}, size: ${(fileSize / 1024 / 1024).toFixed(2)}MB`);
    res.json({ success: true, message: 'File uploaded successfully', fileSize, fileName });
  } catch (error) {
    console.error('[Confirm] Error:', error);
    res.status(500).json({ error: 'Failed to confirm upload' });
  }
});

// Upload documents (ZIP) - stream to S3 (fallback)
router.post('/:jobId/upload', async (req, res) => {
  const { jobId } = req.params;
  console.log(`[Upload] Starting for job: ${jobId}`);
  try {
    const busboy = Busboy({ headers: req.headers, limits: { fileSize: 4 * 1024 * 1024 * 1024 } });
    let uploadPromise = null, fileName = '', fileSize = 0;

    busboy.on('file', (fieldname, fileStream, info) => {
      fileName = info.filename;
      const s3Key = `jobs/${jobId}/uploads/raw/documents.zip`;
      fileStream.on('data', (chunk) => { fileSize += chunk.length; });
      uploadPromise = uploadStreamToS3(s3Key, fileStream, 'application/zip');
    });

    busboy.on('finish', async () => {
      if (!uploadPromise) return res.status(400).json({ error: 'No file received' });
      try {
        await uploadPromise;
        updateJobStatus(jobId, { status: 'uploaded', uploadedAt: new Date().toISOString(), fileName, fileSize });
        res.json({ success: true, message: 'File uploaded successfully', fileSize, fileName });
      } catch (s3Error) {
        res.status(500).json({ error: 'S3 upload failed: ' + s3Error.message });
      }
    });

    busboy.on('error', (err) => res.status(500).json({ error: 'Upload parsing failed: ' + err.message }));
    req.pipe(busboy);
  } catch (error) {
    res.status(500).json({ error: 'Failed to upload file: ' + error.message });
  }
});

// Upload single PDF
router.post('/:jobId/upload-pdf', async (req, res) => {
  const { jobId } = req.params;
  try {
    const busboy = Busboy({ headers: req.headers, limits: { fileSize: 500 * 1024 * 1024 } });
    let fileName = '', fileSize = 0, fileBuffer = [];

    busboy.on('file', (fieldname, fileStream, info) => {
      fileName = info.filename;
      fileStream.on('data', (chunk) => { fileBuffer.push(chunk); fileSize += chunk.length; });
    });

    busboy.on('finish', async () => {
      if (!fileBuffer.length) return res.status(400).json({ error: 'No file received' });
      try {
        const buffer   = Buffer.concat(fileBuffer);
        const analysis = await analyzePdf(buffer);
        const s3Key    = `jobs/${jobId}/uploads/extracted/${fileName}`;
        await uploadToS3(s3Key, buffer, 'application/pdf');

        const queueData = {
          totalDocuments: 1, processedCount: 0,
          documents: [{ name: fileName, key: s3Key, type: '.pdf', size: fileSize, status: 'pending', analysis }],
          results: [], failedDocuments: [], status: 'ready'
        };
        await putJsonToS3(`jobs/${jobId}/processing/queue.json`, queueData);
        updateJobStatus(jobId, { status: 'extracted', uploadedAt: new Date().toISOString(), fileName, fileSize, totalDocuments: 1, processedCount: 0, pdfAnalysis: analysis });
        res.json({ success: true, message: 'PDF uploaded', fileSize, fileSizeMB: (fileSize/1024/1024).toFixed(2), fileName, analysis });
      } catch (error) {
        res.status(500).json({ error: 'PDF processing failed: ' + error.message });
      }
    });

    busboy.on('error', (err) => res.status(500).json({ error: 'Upload parsing failed: ' + err.message }));
    req.pipe(busboy);
  } catch (error) {
    res.status(500).json({ error: 'Failed to upload PDF: ' + error.message });
  }
});

// Analyze PDF
router.get('/:jobId/analyze-pdf', async (req, res) => {
  const { jobId } = req.params;
  try {
    const queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
    if (!queueData.documents?.length) return res.status(404).json({ error: 'No documents found' });
    const pdfDoc = queueData.documents.find(d => d.type === '.pdf');
    if (!pdfDoc) return res.status(404).json({ error: 'No PDF found' });
    if (pdfDoc.analysis) return res.json({ success: true, documentName: pdfDoc.name, ...pdfDoc.analysis });
    const pdfStream = await getFromS3(pdfDoc.key);
    const pdfBuffer = await streamToBuffer(pdfStream);
    const analysis  = await analyzePdf(pdfBuffer);
    res.json({ success: true, documentName: pdfDoc.name, ...analysis });
  } catch (error) {
    res.status(500).json({ error: 'Failed to analyze PDF: ' + error.message });
  }
});

// Start extraction
router.post('/:jobId/extract', async (req, res) => {
  try {
    const { jobId } = req.params;
    await queueManager.add('extract', { jobId, type: 'extract' }, { jobId: `${jobId}-extract` });
    updateJobStatus(jobId, { status: 'extracting' });
    res.json({ success: true, message: 'Extraction started' });
  } catch (error) {
    res.status(500).json({ error: 'Failed to start extraction' });
  }
});

// Start analysis
router.post('/:jobId/analyze', async (req, res) => {
  try {
    const { jobId } = req.params;
    await queueManager.add('analyze', { jobId, type: 'analyze' }, { jobId: `${jobId}-analyze` });
    updateJobStatus(jobId, { status: 'analyzing' });
    res.json({ success: true, message: 'Analysis started' });
  } catch (error) {
    res.status(500).json({ error: 'Failed to start analysis' });
  }
});

// Resume interrupted job
router.post('/:jobId/resume', async (req, res) => {
  try {
    const { jobId } = req.params;
    console.log(`[Resume] Attempting to resume job: ${jobId}`);

    let queueData;
    try {
      queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
    } catch {
      return res.status(404).json({ error: 'Job queue data not found. Cannot resume.' });
    }

    const pendingDocs   = queueData.documents.filter(d => d.status === 'pending');
    const completedDocs = queueData.documents.filter(d => d.status === 'completed');
    const failedDocs    = queueData.documents.filter(d => d.status === 'failed');

    console.log(`[Resume] ${completedDocs.length} completed, ${failedDocs.length} failed, ${pendingDocs.length} pending`);

    if (pendingDocs.length === 0) {
      if (queueData.status === 'completed') {
        return res.json({ success: true, message: 'Job already completed', status: 'completed',
          stats: { total: queueData.totalDocuments, completed: completedDocs.length, failed: failedDocs.length, pending: 0 } });
      }
      await queueManager.add('generate-report', { jobId, type: 'generate-report' }, { jobId: `${jobId}-report-resume` });
      updateJobStatus(jobId, { status: 'generating-report' });
      return res.json({ success: true, message: 'All docs processed. Generating report...', status: 'generating-report',
        stats: { total: queueData.totalDocuments, completed: completedDocs.length, failed: failedDocs.length, pending: 0 } });
    }

    try {
      const metadata = await getJsonFromS3(`jobs/${jobId}/metadata.json`);
      metadata.status    = 'processing';
      metadata.resumedAt = new Date().toISOString();
      metadata.resumeCount = (metadata.resumeCount || 0) + 1;
      await putJsonToS3(`jobs/${jobId}/metadata.json`, metadata);
    } catch {}

    await queueManager.add('analyze', { jobId, type: 'analyze', isResume: true }, { jobId: `${jobId}-analyze-resume-${Date.now()}` });
    updateJobStatus(jobId, { status: 'processing' });

    res.json({
      success: true,
      message: `Resuming. ${pendingDocs.length} documents remaining — will skip already-completed docs.`,
      status: 'processing',
      stats: { total: queueData.totalDocuments, completed: completedDocs.length, failed: failedDocs.length, pending: pendingDocs.length }
    });
  } catch (error) {
    res.status(500).json({ error: 'Failed to resume job', details: error.message });
  }
});

// Generate report
router.post('/:jobId/generate-report', async (req, res) => {
  try {
    const { jobId } = req.params;
    await queueManager.add('generate-report', { jobId, type: 'generate-report' }, { jobId: `${jobId}-report` });
    res.json({ success: true, message: 'Report generation started' });
  } catch (error) {
    res.status(500).json({ error: 'Failed to start report generation' });
  }
});

// Get job status
router.get('/:jobId/status', async (req, res) => {
  try {
    const { jobId } = req.params;
    let status = getJobStatus(jobId);
    if (!status) {
      try { status = await getJsonFromS3(`jobs/${jobId}/metadata.json`); setJobStatus(jobId, status); }
      catch { return res.status(404).json({ error: 'Job not found' }); }
    }
    try {
      const queueData = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
      status.queueStatus    = queueData.status;
      status.totalDocuments = queueData.totalDocuments;
      status.processedCount = queueData.processedCount;
      status.failedCount    = queueData.failedDocuments?.length || 0;
    } catch {}
    res.json(status);
  } catch (error) {
    res.status(500).json({ error: 'Failed to get job status' });
  }
});

// Get job logs
router.get('/:jobId/logs', async (req, res) => {
  try {
    const { jobId } = req.params;
    try {
      const logsData = await getJsonFromS3(`jobs/${jobId}/processing/logs.json`);
      res.json({ success: true, logs: logsData.logs || [], lastUpdated: logsData.lastUpdated });
    } catch {
      res.json({ success: true, logs: [], lastUpdated: null });
    }
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch logs' });
  }
});

// SSE — live updates
router.get('/:jobId/events', (req, res) => {
  const { jobId } = req.params;
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();
  res.write(`event: connected\ndata: ${JSON.stringify({ jobId })}\n\n`);
  addSSEClient(jobId, res);
  const heartbeat = setInterval(() => {
    res.write(`event: heartbeat\ndata: ${JSON.stringify({ time: Date.now() })}\n\n`);
  }, 30000);
  req.on('close', () => { clearInterval(heartbeat); removeSSEClient(jobId, res); });
});

// Download report
router.get('/:jobId/download', async (req, res) => {
  try {
    const { jobId }     = req.params;
    const reportKey     = `jobs/${jobId}/output/Legal_Audit_Report.xlsx`;
    const downloadUrl   = await getSignedDownloadUrl(reportKey, 3600);
    res.json({ success: true, downloadUrl, expiresIn: 3600 });
  } catch (error) {
    res.status(500).json({ error: 'Failed to generate download link' });
  }
});

// List all jobs
router.get('/', async (req, res) => {
  try {
    // List only top-level job folders (not all objects inside them)
    const { S3Client, ListObjectsV2Command } = await import('@aws-sdk/client-s3');
    const s3 = new S3Client({ region: process.env.AWS_REGION });
    const BUCKET = process.env.S3_BUCKET_NAME;

    const allJobIds = new Set();
    let continuationToken = null;

    do {
      const response = await s3.send(new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: 'jobs/',
        Delimiter: '/',          // ← KEY: only get top-level folders, not all files
        MaxKeys: 1000,
        ...(continuationToken && { ContinuationToken: continuationToken })
      }));

      // CommonPrefixes gives us folder names like "jobs/20260330_.../
      response.CommonPrefixes?.forEach(p => {
        const match = p.Prefix.match(/^jobs\/([^\/]+)\/$/);
        if (match) allJobIds.add(match[1]);
      });

      continuationToken = response.IsTruncated ? response.NextContinuationToken : null;
    } while (continuationToken);

    console.log(`[Jobs] Found ${allJobIds.size} job folders`);

    const jobs = [];
    for (const jobId of allJobIds) {
      try {
        const metadata = await getJsonFromS3(`jobs/${jobId}/metadata.json`);

        // Enrich with queue.json for accurate doc counts
        try {
          const queue = await getJsonFromS3(`jobs/${jobId}/processing/queue.json`);
          if (queue.totalDocuments > 0) {
            metadata.totalDocuments = queue.totalDocuments;
            metadata.processedCount = queue.processedCount || 0;
            metadata.failedCount    = queue.failedDocuments?.length || 0;
          }
          if (queue.status === 'completed' && metadata.status !== 'completed') {
            metadata.status      = 'completed';
            metadata.completedAt = queue.completedAt || metadata.completedAt;
            metadata.reportKey   = queue.reportKey   || metadata.reportKey;
          }
        } catch {}

        jobs.push(metadata);
      } catch {}
    }

    jobs.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json({ jobs });
  } catch (error) {
    console.error('List jobs error:', error);
    res.status(500).json({ error: 'Failed to list jobs' });
  }
});

export default router;