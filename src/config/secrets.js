import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';

const client = new SecretsManagerClient({ 
  region: process.env.AWS_REGION || 'ap-south-1' 
});

export async function loadSecrets() {
  if (process.env.NODE_ENV !== 'production') {
    console.log('✓ Local environment - using .env file');
    return;
  }

  try {
    console.log('Loading secrets from AWS Secrets Manager...');
    
    const command = new GetSecretValueCommand({
      SecretId: 'legal-audit/backend/prod'
    });

    const response = await client.send(command);
    const secrets = JSON.parse(response.SecretString);

    // This injects into process.env - no other files need changing
    Object.assign(process.env, secrets);
    
    console.log('✓ Secrets loaded successfully');
  } catch (error) {
    console.error('❌ Failed to load secrets:', error.message);
    throw error;
  }
}