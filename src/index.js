import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { loadSecrets } from './config/secrets.js';
import authRoutes from './routes/auth.js';
import mastersRoutes from './routes/masters.js';
import jobsRoutes from './routes/jobs.js';
import { initializeS3Bucket, getFromS3, streamToBuffer } from './services/s3Service.js';
import { initializeQueue } from './services/queueService.js';
import { initializeWorker } from './workers/documentWorker.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3001;

// ─────────────────────────────────────────────────────────────
// CORS — only needed for local dev.
// In production, browser and API are on the same ALB origin
// so CORS is not triggered. Keep it here for local convenience.
// ─────────────────────────────────────────────────────────────
app.use(cors({
  origin: process.env.FRONTEND_URL || 'http://localhost:3000',
  credentials: true
}));

// Body parser limits for large JSON payloads
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// ─────────────────────────────────────────────────────────────
// Health check
// ─────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ─────────────────────────────────────────────────────────────
// API Routes — registered first so /api/* never hits the proxy
// ─────────────────────────────────────────────────────────────
app.use('/api/auth', authRoutes);
app.use('/api/masters', mastersRoutes);
app.use('/api/jobs', jobsRoutes);

// ─────────────────────────────────────────────────────────────
// S3 Frontend Proxy
//
// How it works:
//   1. Browser requests GET /  or GET /dashboard  or GET /assets/index-abc.js
//   2. Express maps the path → S3 key under the FRONTEND_S3_PREFIX folder
//   3. Fetches the object from the private S3 bucket
//   4. Streams it back to the browser with the correct Content-Type
//
// S3 bucket stays 100% private — no public access, no bucket policy,
// no CloudFront. Only the ECS task role can read these objects.
//
// React Router support:
//   - Requests for known asset extensions → served as-is from S3
//   - All other paths → serve index.html so React Router handles routing
// ─────────────────────────────────────────────────────────────

const FRONTEND_S3_PREFIX = process.env.FRONTEND_S3_PREFIX || 'frontend';

// File extensions that are real static assets (not React routes)
const ASSET_EXTENSIONS = /\.(js|css|png|jpg|jpeg|svg|ico|woff|woff2|ttf|eot|map|json|txt|webmanifest)$/i;

// MIME type map for correct Content-Type headers
const MIME_TYPES = {
  '.js':          'application/javascript',
  '.css':         'text/css',
  '.html':        'text/html; charset=utf-8',
  '.json':        'application/json',
  '.png':         'image/png',
  '.jpg':         'image/jpeg',
  '.jpeg':        'image/jpeg',
  '.svg':         'image/svg+xml',
  '.ico':         'image/x-icon',
  '.woff':        'font/woff',
  '.woff2':       'font/woff2',
  '.ttf':         'font/ttf',
  '.eot':         'application/vnd.ms-fontobject',
  '.map':         'application/json',
  '.webmanifest': 'application/manifest+json',
  '.txt':         'text/plain',
};

function getMimeType(filePath) {
  const ext = filePath.match(/\.[^.]+$/)?.[0]?.toLowerCase() || '';
  return MIME_TYPES[ext] || 'application/octet-stream';
}

async function proxyFromS3(s3Key, res) {
  const stream = await getFromS3(s3Key);
  const buffer = await streamToBuffer(stream);
  const mimeType = getMimeType(s3Key);
  res.setHeader('Content-Type', mimeType);
  // Cache assets aggressively, never cache HTML
  if (mimeType === 'text/html; charset=utf-8') {
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  } else {
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
  }
  res.send(buffer);
}

// Frontend proxy handler — catches everything that isn't /api/* or /health
app.get('*', async (req, res) => {
  const reqPath = req.path;

  // Determine the S3 key to fetch
  let s3Key;
  if (reqPath === '/' || !ASSET_EXTENSIONS.test(reqPath)) {
    // React route — always serve index.html
    s3Key = `${FRONTEND_S3_PREFIX}/index.html`;
  } else {
    // Real static asset — serve the exact file
    s3Key = `${FRONTEND_S3_PREFIX}${reqPath}`;
  }

  try {
    await proxyFromS3(s3Key, res);
  } catch (err) {
    // If asset not found, fall back to index.html (handles deep links)
    if (err?.$metadata?.httpStatusCode === 404 || err?.name === 'NoSuchKey') {
      try {
        await proxyFromS3(`${FRONTEND_S3_PREFIX}/index.html`, res);
      } catch (fallbackErr) {
        console.error('[Frontend Proxy] index.html not found in S3:', fallbackErr.message);
        res.status(404).send('Frontend not deployed. Upload your React build to S3.');
      }
    } else {
      console.error('[Frontend Proxy] S3 fetch error:', err.message);
      res.status(502).send('Error fetching frontend from S3.');
    }
  }
});

// ─────────────────────────────────────────────────────────────
// Global error handler
// ─────────────────────────────────────────────────────────────
app.use((err, req, res, next) => {
  console.error('Error:', err);
  if (err.code === 'LIMIT_FILE_SIZE') {
    return res.status(413).json({ error: 'File too large. Maximum size is 4GB.' });
  }
  res.status(err.status || 500).json({
    error: err.message || 'Internal server error',
    details: process.env.NODE_ENV === 'development' ? err.stack : undefined
  });
});

// ─────────────────────────────────────────────────────────────
// Boot sequence
// ─────────────────────────────────────────────────────────────
async function start() {
  try {
    await loadSecrets();

    console.log('Initializing queue...');
    const queueResult = await initializeQueue();
    if (queueResult.useInMemory) {
      console.log('✓ Using in-memory queue');
    } else {
      console.log('✓ Redis queue initialized');
    }

    await initializeS3Bucket();
    console.log('✓ S3 bucket initialized');

    await initializeWorker();
    console.log('✓ Document processing worker initialized');

    app.listen(PORT, () => {
      console.log(`✓ Server running on port ${PORT}`);
      console.log(`  Environment: ${process.env.NODE_ENV || 'development'}`);
      console.log(`  Frontend S3 prefix: ${FRONTEND_S3_PREFIX}`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

start();
