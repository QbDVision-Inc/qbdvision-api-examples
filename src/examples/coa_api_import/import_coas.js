"use strict"

require('dotenv').config()
const {CoAOpenAPIProxy} = require("./coa_open_api_proxy");
const coaDocsToMaterialIds = require("./coaDocsToMaterialIds.json");
const fs = require("fs");

const COA_DOCUMENTS_PATH = './coaDocs/';
const API_KEY = process.env.API_KEY;
const BASE_URL = process.env.BASE_URL;
const openAPIProxy = new CoAOpenAPIProxy(API_KEY, BASE_URL);

(async () => {
    await main();
})();

async function main() {
    const coaDocs = fs.readdirSync(COA_DOCUMENTS_PATH);

    for (let coaDoc of coaDocs) {
        try {
            const materialId = coaDocsToMaterialIds[coaDoc];

            if (materialId && materialId !== -1) {
                console.log(`Processing CoA ${coaDoc}`);

                console.log("Getting upload URL...");
                const uploadUrl = await getUploadUrl(coaDoc);
                console.log("Uploading file to ", uploadUrl, "...");
                const coaAPIInfo = await uploadToS3(uploadUrl, coaDoc);
                console.log("Starting CoA job...");
                const jobId = await startCoAJob(coaAPIInfo, materialId);
                console.log(`Waiting for job with ID ${jobId} to complete...`);
                const batchResults = await waitForJobToFinish(jobId);
                console.log("Uploading CoA mapped tests to results...");
                await importCoA(coaAPIInfo, batchResults, materialId);

                console.log("CoA imported successfully");
            }
        } catch (error) {
            console.error("Error caught:", error);
            break;
        }
    }
}

async function getUploadUrl(fileName) {
    const response = await openAPIProxy.get(encodeURI(`import/getCoAUploadUrl?fileName=${fileName}`));

    return response.data.url;
}

async function uploadToS3(uploadUrl, coaDoc) {
    const coaAPIInfo = {};
    let response;

    response = await openAPIProxy.uploadToS3(uploadUrl, `${COA_DOCUMENTS_PATH}${coaDoc}`);

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

    result = await openAPIProxy.post(`import/processCoA`, data);
    console.log("Job started:", result.data);

    const jobId = result.data.jobId;
    if (!jobId) {
        throw new Error("Job ID not found in the response");
    }
    return jobId;
}

async function waitForJobToFinish(jobId) {
    const apiPath = `import/processCoA?jobId=${jobId}`;
    let result = "";
    while (!(result?.status === 200 && result?.data?.status === "success")) {
        console.log("Waiting for the job to finish...");

        result = await openAPIProxy.post(apiPath, null);
        console.log("Job status response:", result?.data);

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
