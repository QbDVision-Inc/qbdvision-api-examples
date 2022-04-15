"use strict"

const {CoAOpenAPIProxy} = require("./coa_open_api_proxy");
const coaDocsToMaterialIds = require("./coaDocsToMaterialIds.json");
const fs = require("fs");

const COA_DOCUMENTS_PATH = './coaDocs/';
const API_KEY = "95b9db61cf654c20aa3d97b14f780171";
const BASE_URL = "https://api.cicdkons.dev.qbdvision.com/cicdkons/"
const openAPIProxy = new CoAOpenAPIProxy(API_KEY, BASE_URL);

(async () => {
  await main();
})();

async function main() {
  const coaDocs = fs.readdirSync(COA_DOCUMENTS_PATH);

  for(let coaDoc of coaDocs) {
    try {
      const materialId = coaDocsToMaterialIds[coaDoc];

      if(materialId) {
        console.log(`Processing CoA ${coaDoc}`);

        const uploadUrl = await getUploadUrl(coaDoc);
        const coaAPIInfo = await uploadToS3(uploadUrl, coaDoc);
        const jobId = await startCoAJob(coaAPIInfo, materialId);
        const batchResults = await waitForJobToFinish(jobId);
        // Verify CoA mapped tests to results
        await importCoA(coaAPIInfo, batchResults, materialId);

        console.log("CoA imported successfully");
      }
    } catch(error) {
      console.log(error);
    }
  }
}

async function getUploadUrl(fileName) {
let response;
  try {
    response = await openAPIProxy.get(encodeURI(`import/getCoAUploadUrl?fileName=${fileName}`));
  } catch(error) {
    console.log(error);
  }

  return response.data.url;
}

async function uploadToS3(uploadUrl, coaDoc) {
  const coaAPIInfo = {};
  let response;

  try {
    response = await openAPIProxy.uploadToS3(uploadUrl, `${COA_DOCUMENTS_PATH}${coaDoc}`);
  } catch(error) {
    console.log(error);
  }

  coaAPIInfo.fileName = coaDoc;
  coaAPIInfo.fileVersion = response.headers["x-amz-version-id"];
  const regex = /(https:\/\/[-\w]*\.s3.amazonaws.com\/[-_\w]*\/([-\w]*)\/)/;
  const match = regex.exec(uploadUrl);
  if (match) {
    coaAPIInfo.fileKey = match[2];
  }

  return coaAPIInfo;
}

async function startCoAJob(coaAPIInfo, materialId) {
  let result;
  let data = {
    fileData: coaAPIInfo,
    materialId: materialId
  };

  try {
    result = await openAPIProxy.post(`import/processCoA`, data);
  } catch(error) {
    console.log(error);
  }

  return result.data.jobId;
}

async function waitForJobToFinish(jobId) {
  const apiPath = `import/processCoA?jobId=${jobId}`;
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

async function importCoA(coaAPIInfo, batchResults, materialId) {
  let payload = {
    fileData: coaAPIInfo,
    etlData: JSON.stringify(batchResults),
    materialId: materialId
  };

  return await openAPIProxy.put("import/importCoA", payload);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
