import {
  BedrockRuntimeClient,
  InvokeModelCommand
} from "@aws-sdk/client-bedrock-runtime";

import { getJsonFromS3 } from "./s3Service.js";
import {
  analyzePdf,
  extractTextChunks,
  splitPdfIntoBatches,
  mergeExtractionResults,
  PDF_CONFIG
} from "./pdfChunkService.js";

const bedrockClient = new BedrockRuntimeClient({
  region: process.env.AWS_REGION
});

/* ------------------------------------------------ */
/* RETRY LOGIC                                       */
/* ------------------------------------------------ */

const executeWithRetry = async (fn, retries = 3, delay = 2000) => {
  let attempt = 0;
  while (attempt < retries) {
    try {
      return await fn();
    } catch (error) {
      attempt++;
      if (attempt >= retries) throw error;
      console.warn(`Retrying... Attempt ${attempt}`);
      await new Promise((res) => setTimeout(res, delay * attempt));
    }
  }
};

/* ------------------------------------------------ */
/* CLAUDE CHUNK ANALYSIS                             */
/* ------------------------------------------------ */

async function analyzeChunk(chunkContent, systemPrompt, mediaType = 'text') {
  const response = await executeWithRetry(async () => {

    let messageContent;

    if (mediaType === 'pdf') {
      messageContent = [
        {
          type: "document",
          source: {
            type: "base64",
            media_type: "application/pdf",
            data: chunkContent
          }
        },
        {
          type: "text",
          text: systemPrompt
        }
      ];
    } else if (mediaType === 'image/png' || mediaType === 'image/jpeg' || mediaType === 'image/gif' || mediaType === 'image/webp') {
      messageContent = [
        {
          type: "image",
          source: {
            type: "base64",
            media_type: mediaType,
            data: chunkContent
          }
        },
        {
          type: "text",
          text: systemPrompt
        }
      ];
    } else {
      messageContent = [
        {
          type: "text",
          text: `${systemPrompt}\n\n${chunkContent}`
        }
      ];
    }

    const command = new InvokeModelCommand({
      modelId: "global.anthropic.claude-sonnet-4-6",
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify({
        anthropic_version: "bedrock-2023-05-31",
        max_tokens: 4096,
        messages: [{ role: "user", content: messageContent }]
      })
    });

    const result = await bedrockClient.send(command);
    const decoded = new TextDecoder().decode(result.body);
    return JSON.parse(decoded);
  });

  const responseText = response.content?.[0]?.text || "";
  const inputTokens = response.usage?.input_tokens || 0;
  const outputTokens = response.usage?.output_tokens || 0;

  return { responseText, inputTokens, outputTokens };
}

/* ------------------------------------------------ */
/* HELPER: PARSE JSON FROM RESPONSE                  */
/* ------------------------------------------------ */

function parseJsonResponse(responseText) {
  try {
    return JSON.parse(responseText);
  } catch {
    const match = responseText.match(/\{[\s\S]*\}/);
    if (match) {
      try {
        return JSON.parse(match[0]);
      } catch {
        return null;
      }
    }
    return null;
  }
}

/* ------------------------------------------------ */
/* SYSTEM PROMPT                                     */
/* ------------------------------------------------ */

const SYSTEM_PROMPT = `You are a legal document audit assistant for a bank/NBFC.
Analyze this legal document and extract structured information.
Return ONLY a valid JSON object with these exact fields:
{
  "appl_no": "string",
  "borrower_name": "string",
  "property_address": "string",
  "property_type": "string",
  "state": "string",
  "tsr_date": "string",
  "ownership_title_chain_status": "string",
  "encumbrances_adverse_entries": "string",
  "subsequent_charges": "string",
  "prior_charge_subsisting": "string",
  "roc_charge_flag": "string",
  "litigation_lis_pendens": "string",
  "mutation_status": "string",
  "revenue_municipal_dues": "string",
  "land_use_zoning_status": "string",
  "stamping_registration_issues": "string",
  "mortgage_perfection_issues": "string",
  "advocate_adverse_remarks": "string",
  "risk_rating": "High|Medium|Low",
  "enforceability_decision": "string",
  "enforceability_rationale": "string",
  "recommended_actions": "string",
  "confidence_score": 0.0
}
No markdown. No explanation. Just JSON.`;

/* ------------------------------------------------ */
/* MAIN PDF ANALYSIS ENTRY                           */
/* ------------------------------------------------ */

export async function analyzeDocument(documentContent, documentName, mediaType = 'text', options = {}) {
  try {
    const pdfBuffer = options.pdfBuffer;
    const onProgress = options.onProgress;

    // If we have a PDF buffer, use chunking strategy
    if (pdfBuffer) {
      const analysis = await analyzePdf(pdfBuffer);
      let chunkResults = [];
      let totalInput = 0;
      let totalOutput = 0;

      if (analysis.strategy === 'text-chunk') {
        const chunks = await extractTextChunks(pdfBuffer);

        for (let i = 0; i < chunks.length; i++) {
          const chunk = chunks[i];

          if (onProgress) {
            onProgress({
              phase: 'processing',
              current: i + 1,
              total: chunks.length,
              message: `Processing text chunk ${i + 1}/${chunks.length}...`
            });
          }

          const { responseText, inputTokens, outputTokens } = await analyzeChunk(chunk.content, SYSTEM_PROMPT, 'text');
          totalInput += inputTokens;
          totalOutput += outputTokens;

          const data = parseJsonResponse(responseText);
          if (data) chunkResults.push(data);

          await new Promise(r => setTimeout(r, PDF_CONFIG.INTER_BATCH_DELAY));
        }

      } else if (analysis.strategy === 'page-split') {
        const batches = await splitPdfIntoBatches(pdfBuffer);

        for (let i = 0; i < batches.length; i++) {
          const batch = batches[i];

          if (onProgress) {
            onProgress({
              phase: 'processing',
              current: i + 1,
              total: batches.length,
              message: `Processing pages ${batch.startPage}-${batch.endPage} (${i + 1}/${batches.length})...`
            });
          }

          const { responseText, inputTokens, outputTokens } = await analyzeChunk(batch.base64, SYSTEM_PROMPT, 'pdf');
          totalInput += inputTokens;
          totalOutput += outputTokens;

          const data = parseJsonResponse(responseText);
          if (data) chunkResults.push(data);

          await new Promise(r => setTimeout(r, PDF_CONFIG.INTER_BATCH_DELAY));
        }

      } else {
        // direct-pdf or direct-text strategy
        const { responseText, inputTokens, outputTokens } = await analyzeChunk(documentContent, SYSTEM_PROMPT, 'pdf');
        totalInput += inputTokens;
        totalOutput += outputTokens;

        const data = parseJsonResponse(responseText);
        if (data) chunkResults.push(data);
      }

      const mergedData = mergeExtractionResults(chunkResults, { documentName });

      return {
        success: !!mergedData,
        data: mergedData,
        strategy: analysis.strategy,
        chunksProcessed: chunkResults.length,
        tokenDetails: { input: totalInput, output: totalOutput }
      };
    }

    // No pdfBuffer — send content directly (small files, images, text)
    const { responseText, inputTokens, outputTokens } = await analyzeChunk(documentContent, SYSTEM_PROMPT, mediaType);
    const data = parseJsonResponse(responseText);

    return {
      success: !!data,
      data,
      tokenDetails: { input: inputTokens, output: outputTokens }
    };

  } catch (error) {
    console.error('[Bedrock] analyzeDocument error:', error.message);
    return {
      success: false,
      error: error.message,
      data: null,
      tokenDetails: { input: 0, output: 0 }
    };
  }
}

export async function getDocumentAnalysis(pdfBuffer, documentName) {
  return { success: true, documentName };
}