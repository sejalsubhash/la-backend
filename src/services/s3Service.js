import { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command, HeadObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'ap-south-1',
});

const BUCKET = process.env.S3_BUCKET_NAME || 'legal-audit-platform';

// Default legal audit prompt
const DEFAULT_PROMPT = {
  version: "1.0.0",
  updatedAt: new Date().toISOString(),
  updatedBy: "system",
  systemRole: `You are a senior legal advisor for a regulated NBFC/bank, performing a preventive and detective legal risk assessment of Title Search Reports (TSRs) for existing borrower properties. Your goal is to identify case-level risks that may impair security enforceability in case of default, and to classify each case into High / Medium / Low risk with a clear Enforceability Decision ("Enforceable", "Enforceable with Conditions", "Not Enforceable") supported by rationale and required actions.`,
  scope: [
    "A. Ownership & Title Chain Defects",
    "B. Encumbrances / Charges / Liens",
    "C. Property Use & Land Classification Risks",
    "D. Revenue & Municipal Records",
    "E. Litigation / Adverse Proceedings",
    "F. Document & Stamp/Registration Deficiencies",
    "G. Mortgage Creation & Perfection",
    "H. Advocate's TSR Remarks"
  ],
  riskClassification: {
    high: [
      "Subsequent charge created after our mortgage without consent/NOC",
      "Prior charge still subsisting or attachment order affecting property",
      "Adverse court orders/stay/lis pendens impacting transfer or enforcement",
      "Title break/unregistered crucial link deed; forged/fraudulent indicators",
      "Mortgage not perfected/registered where legally required",
      "IBC/insolvency underway or SARFAESI proceedings by third party",
      "Agricultural land enforceability restrictions with no conversion"
    ],
    medium: [
      "Mutation pending; revenue records not updated but conveyance otherwise valid",
      "ROC charge exists but property not clearly included",
      "Property tax arrears; zoning/NA conversion pending but in process",
      "Advocate TSR contains adverse remarks requiring compliance",
      "Minor stamp/description discrepancies that are curable"
    ],
    low: [
      "Clean title chain; no adverse encumbrances; records updated",
      "Minor administrative gaps with documented fixes"
    ]
  },
  outputSchema: [
    "Appl_No / Loan_No",
    "Borrower_Name",
    "Property_Address",
    "Property_Type (Res/Comm/Agr)",
    "State",
    "TSR_Date",
    "Ownership_Title_Chain_Status",
    "Encumbrances_Adverse_Entries",
    "Subsequent_Charges_After_Our_Mortgage",
    "Prior_Charge_Subsisting",
    "ROC_Charge_Flag (If Company/LLP)",
    "Litigation_LisPendens_Attachments",
    "Mutation_Status",
    "Revenue_Municipal_Dues",
    "Land_Use_Zoning_NA_Conversion_Status",
    "Stamping_Registration_Issues",
    "Mortgage_Perfection_Issues",
    "Advocate_TSR_Adverse_Remarks",
    "Risk_Rating",
    "Enforceability_Decision",
    "Enforceability_Rationale",
    "Recommended_Actions",
    "Next_Review_Due_Date",
    "Prepared_By",
    "Reviewed_By (Legal/RCU)"
  ]
};

export async function initializeS3Bucket() {
  try {
    try {
      await s3Client.send(new HeadObjectCommand({
        Bucket: BUCKET,
        Key: 'masters/legal_audit_prompt.json'
      }));
      console.log('  Masters prompt already exists');
    } catch (err) {
      if (err.name === 'NotFound' || err.$metadata?.httpStatusCode === 404) {
        await s3Client.send(new PutObjectCommand({
          Bucket: BUCKET,
          Key: 'masters/legal_audit_prompt.json',
          Body: JSON.stringify(DEFAULT_PROMPT, null, 2),
          ContentType: 'application/json'
        }));
        console.log('  Created default masters prompt');
      } else {
        throw err;
      }
    }

    try {
      await s3Client.send(new HeadObjectCommand({
        Bucket: BUCKET,
        Key: 'users/users.json'
      }));
    } catch (err) {
      if (err.name === 'NotFound' || err.$metadata?.httpStatusCode === 404) {
        const defaultUsers = {
          users: [
            {
              email: process.env.SUPER_ADMIN_EMAIL || 'admin@acc.com',
              name: 'Super Admin',
              role: 'admin',
              createdAt: new Date().toISOString()
            }
          ]
        };
        await s3Client.send(new PutObjectCommand({
          Bucket: BUCKET,
          Key: 'users/users.json',
          Body: JSON.stringify(defaultUsers, null, 2),
          ContentType: 'application/json'
        }));
        console.log('  Created default users');
      }
    }
  } catch (error) {
    console.error('S3 initialization error:', error);
    throw error;
  }
}

export async function uploadToS3(key, body, contentType = 'application/octet-stream') {
  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: BUCKET,
      Key: key,
      Body: body,
      ContentType: contentType
    }
  });
  return upload.done();
}

export async function uploadStreamToS3(key, stream, contentType = 'application/octet-stream') {
  console.log(`[S3] Starting multipart upload: ${key}`);
  
  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: BUCKET,
      Key: key,
      Body: stream,
      ContentType: contentType
    },
    queueSize: 4,
    partSize: 10 * 1024 * 1024,
    leavePartsOnError: false
  });

  let partCount = 0;
  let lastLoggedMB = 0;

  upload.on('httpUploadProgress', (progress) => {
    const loadedMB = Math.floor(progress.loaded / 1024 / 1024);
    const currentPart = Math.ceil(progress.loaded / (10 * 1024 * 1024));
    if (currentPart > partCount) {
      partCount = currentPart;
      console.log(`[S3] Part ${partCount} uploaded (${loadedMB} MB total)`);
    }
    if (loadedMB >= lastLoggedMB + 50) {
      console.log(`[S3] Progress: ${loadedMB} MB uploaded`);
      lastLoggedMB = loadedMB;
    }
  });

  const result = await upload.done();
  console.log(`[S3] Upload complete: ${key}`);
  return result;
}

export async function getFromS3(key) {
  const response = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: key
  }));
  return response.Body;
}

export async function getJsonFromS3(key) {
  const response = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: key
  }));
  const str = await response.Body.transformToString();
  return JSON.parse(str);
}

export async function putJsonToS3(key, data) {
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    Body: JSON.stringify(data, null, 2),
    ContentType: 'application/json'
  }));
}

export async function listS3Objects(prefix, maxKeys = 10000) {
  const allObjects = [];
  let continuationToken = null;
  
  do {
    const params = {
      Bucket: BUCKET,
      Prefix: prefix,
      MaxKeys: 1000
    };
    if (continuationToken) {
      params.ContinuationToken = continuationToken;
    }
    const response = await s3Client.send(new ListObjectsV2Command(params));
    if (response.Contents) {
      allObjects.push(...response.Contents);
    }
    continuationToken = response.IsTruncated ? response.NextContinuationToken : null;
    if (allObjects.length >= maxKeys) break;
  } while (continuationToken);
  
  return allObjects;
}

export async function getSignedDownloadUrl(key, expiresIn = 3600) {
  const command = new GetObjectCommand({ Bucket: BUCKET, Key: key });
  return getSignedUrl(s3Client, command, { expiresIn });
}

// NEW: Generate presigned URL for direct browser-to-S3 upload (bypasses CloudFront/ALB)
export async function getSignedUploadUrl(key, contentType = 'application/zip', expiresIn = 3600) {
  const command = new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    ContentType: contentType
  });
  return getSignedUrl(s3Client, command, { expiresIn });
}

export async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

export { s3Client, BUCKET };