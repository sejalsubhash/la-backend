/**
 * PDF Chunk Service
 * Handles large PDFs by splitting them into manageable chunks/pages
 * and aggregating results from multiple Claude API calls
 * 
 * Uses only pdf-parse and pdf-lib (no system dependencies like ImageMagick)
 */

import pdf from 'pdf-parse';
import { PDFDocument } from 'pdf-lib';

// Configuration for large file handling
export const PDF_CONFIG = {
  // Maximum PDF size to send directly to Claude (in bytes)
  // Claude's limit is ~32MB before base64, but we use 10MB for safety with base64 expansion
  MAX_DIRECT_PDF_SIZE: 10 * 1024 * 1024, // 10MB (reduced from 15MB for reliability)
  
  // Maximum size per batch after splitting (in bytes)
  // Each batch should be under 10MB to avoid API issues
  MAX_BATCH_SIZE: 8 * 1024 * 1024, // 8MB per batch
  
  // Maximum pages to process in a single batch (as PDF documents)
  MAX_PAGES_PER_BATCH: 5, // Increased to 5 pages per batch for efficiency
  
  // Minimum text length to consider a PDF text-extractable
  MIN_TEXT_LENGTH: 500,
  
  // Maximum total pages to process (to prevent runaway costs)
  MAX_TOTAL_PAGES: 500, // Increased from 150 to handle large documents
  
  // Text chunk size for text-based PDFs
  TEXT_CHUNK_SIZE: 40000, // ~40K characters per chunk
  
  // Delay between API calls (in ms) to avoid rate limits
  INTER_BATCH_DELAY: 1000 // 1 second between batches
};

/**
 * Analyze PDF and determine processing strategy
 * @param {Buffer} pdfBuffer - The PDF file buffer
 * @returns {Object} Analysis result with strategy recommendation
 */
export async function analyzePdf(pdfBuffer) {
  const fileSize = pdfBuffer.length;
  const fileSizeMB = (fileSize / 1024 / 1024).toFixed(2);
  
  console.log(`[PdfChunk] Analyzing PDF: ${fileSizeMB}MB`);
  
  try {
    // Get basic PDF info
    const pdfData = await pdf(pdfBuffer);
    const pageCount = pdfData.numpages;
    const textLength = pdfData.text ? pdfData.text.trim().length : 0;
    const hasText = textLength > PDF_CONFIG.MIN_TEXT_LENGTH;
    
    // Calculate average text per page
    const avgTextPerPage = pageCount > 0 ? textLength / pageCount : 0;
    
    // Determine if it's a scanned PDF (mostly images, little text)
    const isScannedPdf = !hasText || avgTextPerPage < 100;
    
    console.log(`[PdfChunk] Pages: ${pageCount}, Text length: ${textLength}, Scanned: ${isScannedPdf}`);
    
    // Determine processing strategy
    let strategy;
    let reason;
    
    if (fileSize <= PDF_CONFIG.MAX_DIRECT_PDF_SIZE && !isScannedPdf) {
      strategy = 'direct-text';
      reason = 'Small text-based PDF, extracting text directly';
    } else if (fileSize <= PDF_CONFIG.MAX_DIRECT_PDF_SIZE) {
      strategy = 'direct-pdf';
      reason = 'Small PDF within size limits, sending as PDF document';
    } else if (!isScannedPdf && textLength > PDF_CONFIG.MIN_TEXT_LENGTH) {
      strategy = 'text-chunk';
      reason = `Large text PDF (${fileSizeMB}MB), will extract and chunk text`;
    } else {
      strategy = 'page-split';
      reason = `Large scanned PDF (${fileSizeMB}MB, ${pageCount} pages), will split into page batches`;
    }
    
    const estimatedChunks = strategy === 'page-split' 
      ? Math.ceil(Math.min(pageCount, PDF_CONFIG.MAX_TOTAL_PAGES) / PDF_CONFIG.MAX_PAGES_PER_BATCH)
      : strategy === 'text-chunk'
        ? Math.ceil(textLength / PDF_CONFIG.TEXT_CHUNK_SIZE)
        : 1;
    
    console.log(`[PdfChunk] Strategy: ${strategy}, Estimated chunks: ${estimatedChunks}`);
    
    return {
      fileSize,
      fileSizeMB,
      pageCount,
      hasText,
      textLength,
      avgTextPerPage: Math.round(avgTextPerPage),
      isScannedPdf,
      strategy,
      reason,
      estimatedChunks
    };
  } catch (error) {
    console.error('[PdfChunk] Error analyzing PDF:', error.message);
    
    // Fallback - try to use pdf-lib which is more tolerant
    try {
      const pdfDoc = await PDFDocument.load(pdfBuffer, { ignoreEncryption: true });
      const pageCount = pdfDoc.getPageCount();
      
      return {
        fileSize,
        fileSizeMB,
        pageCount,
        hasText: false,
        textLength: 0,
        avgTextPerPage: 0,
        isScannedPdf: true,
        strategy: 'page-split',
        reason: `PDF analysis partial (${error.message}), will split ${pageCount} pages`,
        estimatedChunks: Math.ceil(Math.min(pageCount, PDF_CONFIG.MAX_TOTAL_PAGES) / PDF_CONFIG.MAX_PAGES_PER_BATCH),
        warning: error.message
      };
    } catch (pdfLibError) {
      // Complete fallback
      return {
        fileSize,
        fileSizeMB,
        pageCount: 0,
        hasText: false,
        textLength: 0,
        avgTextPerPage: 0,
        isScannedPdf: true,
        strategy: 'direct-pdf',
        reason: `Could not analyze PDF, will attempt direct processing`,
        estimatedChunks: 1,
        error: error.message
      };
    }
  }
}

/**
 * Extract text from PDF and split into chunks
 * @param {Buffer} pdfBuffer - The PDF file buffer
 * @param {number} chunkSize - Target characters per chunk
 * @returns {Array} Array of text chunks with metadata
 */
export async function extractTextChunks(pdfBuffer, chunkSize = PDF_CONFIG.TEXT_CHUNK_SIZE) {
  console.log(`[PdfChunk] Extracting text chunks (max ${chunkSize} chars each)...`);
  
  const pdfData = await pdf(pdfBuffer);
  const fullText = pdfData.text;
  const pageCount = pdfData.numpages;
  
  if (!fullText || fullText.trim().length === 0) {
    console.log('[PdfChunk] No text found in PDF');
    return [];
  }
  
  const chunks = [];
  let currentChunk = '';
  let chunkIndex = 0;
  
  // Split by paragraphs to maintain context
  const paragraphs = fullText.split(/\n\s*\n/);
  
  for (const paragraph of paragraphs) {
    if (currentChunk.length + paragraph.length > chunkSize && currentChunk.length > 0) {
      chunks.push({
        index: chunkIndex,
        type: 'text',
        content: currentChunk.trim(),
        charCount: currentChunk.length
      });
      currentChunk = paragraph;
      chunkIndex++;
    } else {
      currentChunk += (currentChunk ? '\n\n' : '') + paragraph;
    }
  }
  
  // Don't forget the last chunk
  if (currentChunk.trim().length > 0) {
    chunks.push({
      index: chunkIndex,
      type: 'text',
      content: currentChunk.trim(),
      charCount: currentChunk.length
    });
  }
  
  console.log(`[PdfChunk] Created ${chunks.length} text chunks`);
  return chunks;
}

/**
 * Split PDF into page batches with size control
 * @param {Buffer} pdfBuffer - The PDF file buffer
 * @param {number} pagesPerBatch - Target pages per batch (may be reduced for large pages)
 * @returns {Array} Array of batch objects with PDF buffers
 */
export async function splitPdfIntoBatches(pdfBuffer, pagesPerBatch = PDF_CONFIG.MAX_PAGES_PER_BATCH) {
  console.log(`[PdfChunk] Splitting PDF into batches (target: ${pagesPerBatch} pages per batch)...`);
  
  const pdfDoc = await PDFDocument.load(pdfBuffer, { ignoreEncryption: true });
  const totalPages = pdfDoc.getPageCount();
  const maxPages = Math.min(totalPages, PDF_CONFIG.MAX_TOTAL_PAGES);
  
  if (totalPages > PDF_CONFIG.MAX_TOTAL_PAGES) {
    console.warn(`[PdfChunk] PDF has ${totalPages} pages, limiting to ${PDF_CONFIG.MAX_TOTAL_PAGES}`);
  }
  
  const batches = [];
  let currentBatchPages = [];
  let currentBatchStartPage = 0;
  
  for (let pageIndex = 0; pageIndex < maxPages; pageIndex++) {
    currentBatchPages.push(pageIndex);
    
    // Check if we should finalize this batch
    const shouldFinalize = 
      currentBatchPages.length >= pagesPerBatch || // Hit page limit
      pageIndex === maxPages - 1; // Last page
    
    if (shouldFinalize && currentBatchPages.length > 0) {
      try {
        // Create a new PDF with these pages
        const batchDoc = await PDFDocument.create();
        const copiedPages = await batchDoc.copyPages(pdfDoc, currentBatchPages);
        copiedPages.forEach(page => batchDoc.addPage(page));
        
        const batchBuffer = Buffer.from(await batchDoc.save());
        const batchSizeMB = batchBuffer.length / 1024 / 1024;
        
        // Check if batch is too large
        if (batchBuffer.length > PDF_CONFIG.MAX_BATCH_SIZE && currentBatchPages.length > 1) {
          // Batch too large, split into individual pages
          console.log(`[PdfChunk] Batch too large (${batchSizeMB.toFixed(2)}MB), splitting into individual pages...`);
          
          for (const pi of currentBatchPages) {
            try {
              const singleDoc = await PDFDocument.create();
              const [copiedPage] = await singleDoc.copyPages(pdfDoc, [pi]);
              singleDoc.addPage(copiedPage);
              
              const singleBuffer = Buffer.from(await singleDoc.save());
              
              batches.push({
                batchIndex: batches.length,
                startPage: pi + 1,
                endPage: pi + 1,
                totalPages: totalPages,
                pageCount: 1,
                buffer: singleBuffer,
                size: singleBuffer.length,
                base64: singleBuffer.toString('base64')
              });
              
              console.log(`[PdfChunk] Created single-page batch: page ${pi + 1} (${(singleBuffer.length / 1024 / 1024).toFixed(2)}MB)`);
            } catch (pageError) {
              console.error(`[PdfChunk] Failed to extract page ${pi + 1}:`, pageError.message);
            }
          }
        } else {
          // Batch size is OK
          batches.push({
            batchIndex: batches.length,
            startPage: currentBatchStartPage + 1,
            endPage: pageIndex + 1,
            totalPages: totalPages,
            pageCount: currentBatchPages.length,
            buffer: batchBuffer,
            size: batchBuffer.length,
            base64: batchBuffer.toString('base64')
          });
          
          console.log(`[PdfChunk] Created batch ${batches.length}: pages ${currentBatchStartPage + 1}-${pageIndex + 1} (${batchSizeMB.toFixed(2)}MB, ${currentBatchPages.length} pages)`);
        }
        
        // Reset for next batch
        currentBatchPages = [];
        currentBatchStartPage = pageIndex + 1;
        
      } catch (error) {
        console.error(`[PdfChunk] Error creating batch for pages ${currentBatchStartPage + 1}-${pageIndex + 1}:`, error.message);
        
        // Try individual pages if batch fails
        for (const pi of currentBatchPages) {
          try {
            const singleDoc = await PDFDocument.create();
            const [copiedPage] = await singleDoc.copyPages(pdfDoc, [pi]);
            singleDoc.addPage(copiedPage);
            
            const singleBuffer = Buffer.from(await singleDoc.save());
            
            batches.push({
              batchIndex: batches.length,
              startPage: pi + 1,
              endPage: pi + 1,
              totalPages: totalPages,
              pageCount: 1,
              buffer: singleBuffer,
              size: singleBuffer.length,
              base64: singleBuffer.toString('base64')
            });
            
            console.log(`[PdfChunk] Created single-page fallback batch: page ${pi + 1}`);
          } catch (pageError) {
            console.error(`[PdfChunk] Failed to extract page ${pi + 1}:`, pageError.message);
          }
        }
        
        // Reset for next batch
        currentBatchPages = [];
        currentBatchStartPage = pageIndex + 1;
      }
    }
  }
  
  console.log(`[PdfChunk] Created ${batches.length} batches total for ${maxPages} pages`);
  return batches;
}

/**
 * Merge extraction results from multiple chunks/pages
 * @param {Array} results - Array of extraction results from Claude
 * @param {Object} documentInfo - Original document info
 * @returns {Object} Merged and consolidated result
 */
export function mergeExtractionResults(results, documentInfo = {}) {
  if (!results || results.length === 0) {
    return null;
  }
  
  // Filter out null/undefined results
  const validResults = results.filter(r => r && typeof r === 'object');
  
  if (validResults.length === 0) {
    return null;
  }
  
  // If only one result, return it directly
  if (validResults.length === 1) {
    return validResults[0];
  }
  
  console.log(`[PdfChunk] Merging ${validResults.length} extraction results...`);
  
  // Start with first result as base
  const merged = { ...validResults[0] };
  
  // Fields that should be concatenated (with deduplication)
  const concatenateFields = [
    'property_address',
    'enforceability_rationale',
    'recommended_actions',
    'advocate_adverse_remarks'
  ];
  
  // Risk priority (lower index = higher priority/worse)
  const riskPriority = ['High', 'Medium', 'Low', 'Manual Review Required', 'Unknown'];
  const enforceabilityPriority = ['Not Enforceable', 'Enforceable with Conditions', 'Enforceable', 'Manual Review Required', 'Unknown'];
  
  // Fields where YES overrides NO
  const yesOverridesFields = [
    'encumbrances_adverse_entries',
    'subsequent_charges',
    'prior_charge_subsisting',
    'litigation_lis_pendens',
    'stamping_registration_issues',
    'mortgage_perfection_issues'
  ];
  
  // Fields where we take first non-Unknown value
  const firstValidFields = [
    'appl_no',
    'borrower_name',
    'property_type',
    'state',
    'tsr_date',
    'roc_charge_flag',
    'mutation_status',
    'revenue_municipal_dues',
    'land_use_zoning_status',
    'ownership_title_chain_status'
  ];
  
  // Process remaining results
  for (let i = 1; i < validResults.length; i++) {
    const result = validResults[i];
    
    // Concatenate fields
    for (const field of concatenateFields) {
      if (result[field] && result[field] !== 'Unknown' && result[field] !== 'Not in this section') {
        if (merged[field] && merged[field] !== 'Unknown' && merged[field] !== 'Not in this section') {
          // Avoid duplicates
          if (!merged[field].includes(result[field])) {
            merged[field] = `${merged[field]}; ${result[field]}`;
          }
        } else {
          merged[field] = result[field];
        }
      }
    }
    
    // Take worst risk rating
    const currentRiskIndex = riskPriority.indexOf(merged.risk_rating);
    const newRiskIndex = riskPriority.indexOf(result.risk_rating);
    if (newRiskIndex !== -1 && (currentRiskIndex === -1 || newRiskIndex < currentRiskIndex)) {
      merged.risk_rating = result.risk_rating;
    }
    
    // Take worst enforceability
    const currentEnfIndex = enforceabilityPriority.indexOf(merged.enforceability_decision);
    const newEnfIndex = enforceabilityPriority.indexOf(result.enforceability_decision);
    if (newEnfIndex !== -1 && (currentEnfIndex === -1 || newEnfIndex < currentEnfIndex)) {
      merged.enforceability_decision = result.enforceability_decision;
    }
    
    // Yes overrides No
    for (const field of yesOverridesFields) {
      if (result[field] && result[field].toLowerCase().startsWith('yes')) {
        merged[field] = result[field];
      }
    }
    
    // First valid value
    for (const field of firstValidFields) {
      if ((!merged[field] || merged[field] === 'Unknown' || merged[field] === 'Not in this section') && 
          result[field] && result[field] !== 'Unknown' && result[field] !== 'Not in this section') {
        merged[field] = result[field];
      }
    }
  }
  
  // Calculate average confidence
  const confidenceScores = validResults
    .map(r => r.confidence_score)
    .filter(c => typeof c === 'number' && !isNaN(c));
  
  if (confidenceScores.length > 0) {
    merged.confidence_score = Math.round(
      confidenceScores.reduce((sum, c) => sum + c, 0) / confidenceScores.length
    );
  }
  
  // Add metadata
  merged._chunked = true;
  merged._chunks_processed = validResults.length;
  
  console.log(`[PdfChunk] Merged result: Risk=${merged.risk_rating}, Confidence=${merged.confidence_score}%`);
  
  return merged;
}

/**
 * Get processing estimates for a large PDF
 * @param {Object} analysis - Result from analyzePdf
 * @returns {Object} Estimated processing stats
 */
export function getProcessingEstimates(analysis) {
  const { strategy, pageCount, estimatedChunks, fileSize } = analysis;
  
  let estimatedTimeMinutes;
  let estimatedApiCalls;
  
  switch (strategy) {
    case 'direct-text':
    case 'direct-pdf':
      estimatedApiCalls = 1;
      estimatedTimeMinutes = 0.5;
      break;
      
    case 'text-chunk':
      estimatedApiCalls = estimatedChunks;
      estimatedTimeMinutes = estimatedChunks * 0.5;
      break;
      
    case 'page-split':
      estimatedApiCalls = estimatedChunks;
      estimatedTimeMinutes = estimatedChunks * 1; // Pages take longer
      break;
      
    default:
      estimatedApiCalls = estimatedChunks || 1;
      estimatedTimeMinutes = estimatedApiCalls * 0.5;
  }
  
  return {
    strategy,
    estimatedApiCalls,
    estimatedTimeMinutes: Math.round(estimatedTimeMinutes * 10) / 10,
    warnings: pageCount > 50 ? [`Large document with ${pageCount} pages may take several minutes`] : []
  };
}
