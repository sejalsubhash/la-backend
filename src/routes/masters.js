import express from 'express';
import { getJsonFromS3, putJsonToS3 } from '../services/s3Service.js';

const router = express.Router();

// Get the current audit prompt
router.get('/prompt', async (req, res) => {
  try {
    const prompt = await getJsonFromS3('masters/legal_audit_prompt.json');
    res.json(prompt);
  } catch (error) {
    console.error('Error fetching prompt:', error);
    res.status(500).json({ error: 'Failed to fetch prompt' });
  }
});

// Update the audit prompt
router.put('/prompt', async (req, res) => {
  try {
    const { systemRole, scope, riskClassification, outputSchema } = req.body;
    
    // Get current prompt
    let currentPrompt;
    try {
      currentPrompt = await getJsonFromS3('masters/legal_audit_prompt.json');
    } catch {
      currentPrompt = {};
    }

    // Update with new values
    const updatedPrompt = {
      ...currentPrompt,
      version: incrementVersion(currentPrompt.version || '1.0.0'),
      updatedAt: new Date().toISOString(),
      updatedBy: req.body.updatedBy || 'unknown',
      systemRole: systemRole || currentPrompt.systemRole,
      scope: scope || currentPrompt.scope,
      riskClassification: riskClassification || currentPrompt.riskClassification,
      outputSchema: outputSchema || currentPrompt.outputSchema
    };

    await putJsonToS3('masters/legal_audit_prompt.json', updatedPrompt);
    
    res.json({
      success: true,
      prompt: updatedPrompt
    });
  } catch (error) {
    console.error('Error updating prompt:', error);
    res.status(500).json({ error: 'Failed to update prompt' });
  }
});

// Get prompt history (versions)
router.get('/prompt/history', async (req, res) => {
  try {
    let history;
    try {
      history = await getJsonFromS3('masters/prompt_history.json');
    } catch {
      history = { versions: [] };
    }
    res.json(history);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch history' });
  }
});

function incrementVersion(version) {
  const parts = version.split('.').map(Number);
  parts[2]++;
  if (parts[2] >= 100) {
    parts[2] = 0;
    parts[1]++;
  }
  if (parts[1] >= 100) {
    parts[1] = 0;
    parts[0]++;
  }
  return parts.join('.');
}

export default router;
