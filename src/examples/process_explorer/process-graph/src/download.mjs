// The functions in this file are responsible for downloading the process graph from the QbDVision server.

import {OpenAPIProxy} from "./utils/open_api_proxy.mjs";
import fs from "fs";

const API_KEY = "YOUR_API_KEY";
const BASE_URL = "http://api.sandbox.qbdvision.com/sandbox"
const PROJECT_ID = 1;
const PROCESS_ID = 258;

async function main() {
    let data = await getProcessExplorerData(PROJECT_ID, PROCESS_ID);

    console.log("Writing the data to a file...");
    fs.writeFileSync("./src/apiLib/process-explorer-data.json", JSON.stringify(data, null, 2));
}

async function getProcessExplorerData(projectId, processId) {
    const openAPIProxy = new OpenAPIProxy(API_KEY, BASE_URL);
    const url = `/processExplorer/${projectId}?processId=${processId}`;
    let result;

    try {
        result = await openAPIProxy.get(url);
    } catch (e) {
        console.log(`Failed to get the process explorer data: ${e.message}`);
    }
    return result?.data;
}

// noinspection JSIgnoredPromiseFromCall
main();
