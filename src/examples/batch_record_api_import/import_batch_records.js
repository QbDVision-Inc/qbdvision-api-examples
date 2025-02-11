"use strict"

const fs = require("fs");
const {OpenAPIProxy} = require("./open_api_proxy");
const batchRecordDocsToProcessIds = require("./batchRecordDocsToProcessIds.json");
const {S3Facade} = require("./facades/s3_facade");

const BATCH_RECORD_DOCUMENTS_PATH = './batchRecordDocs/';
const API_KEY = "YOUR_API_KEY";
const BASE_URL = "https://api.sandbox.qbdvision.com/sandbox/";
const openAPIProxy = new OpenAPIProxy(API_KEY, BASE_URL);
const s3Facade = new S3Facade(API_KEY, BASE_URL);

(async () => {
  await main();
})();

async function main() {
  const batchRecordDocs = fs.readdirSync(BATCH_RECORD_DOCUMENTS_PATH);

  for(let batchRecordDoc of batchRecordDocs) {
    try {
      const processId = batchRecordDocsToProcessIds[batchRecordDoc];

      if(processId) {
        console.log(`Processing batch record ${batchRecordDoc}`);

        const uploadUrl = await getUploadUrl(batchRecordDoc);
        const batchRecordInfo = await uploadToS3(uploadUrl, batchRecordDoc);
        const jobId = await startBatchRecordJob(batchRecordInfo, processId);
        const batchResults = await waitForJobToFinish(jobId);
        // Verify CoA mapped tests to results
        await importBatchRecord(batchRecordInfo, batchResults, processId);

        console.log("Batch Record imported successfully");
      }
    } catch(error) {
      console.log(error);
    }
  }
}

async function getUploadUrl(fileName) {
let response;
  try {
    response = await openAPIProxy.get(encodeURI(`import/getSmartImportUploadUrl?fileName=${fileName}`));
  } catch(error) {
    console.log(error);
  }

  return response.data.url;
}

async function uploadToS3(uploadUrl, coaDoc) {
  const batchRecordFileInfo = {};
  let response;

  try {
    response = await s3Facade.uploadToS3(uploadUrl, `${BATCH_RECORD_DOCUMENTS_PATH}${coaDoc}`);
  } catch(error) {
    console.log(error);
  }

  batchRecordFileInfo.fileName = coaDoc;
  batchRecordFileInfo.fileVersion = response.headers["x-amz-version-id"];
  const regex = /(https:\/\/[-\w]*\.s3.amazonaws.com\/[-_\w]*\/([-\w]*)\/)/;
  const match = regex.exec(uploadUrl);
  if (match) {
    batchRecordFileInfo.fileKey = match[2];
  }

  return batchRecordFileInfo;
}

async function startBatchRecordJob(coaAPIInfo, processId) {
  let result;
  let data = {
    fileData: coaAPIInfo,
    processId: processId
  };

  try {
    result = await openAPIProxy.post(`import/processBatchRecord`, data);
  } catch(error) {
    console.log(error);
  }

  return result.data.jobId;
}

async function waitForJobToFinish(jobId) {
  const apiPath = `import/processBatchRecord?jobId=${jobId}`;
  let result = "";
  while (!(result?.status === 200 && result?.data?.status === "success")) {
    console.log("Waiting for the job to finish...");

    try {
      result = await openAPIProxy.post(apiPath, null, {
        params: {
          jobId: jobId,
        }
      });
    } catch(ignore) {}

    await sleep(2000);
  }

  return result.data;
}

async function importBatchRecord(coaAPIInfo, batchResults, processId) {
  let payload = {
    fileData: coaAPIInfo,
    etlData: JSON.stringify(batchResults),
    processId: processId
  };

  return await openAPIProxy.put("import/importBatchRecord", payload);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
