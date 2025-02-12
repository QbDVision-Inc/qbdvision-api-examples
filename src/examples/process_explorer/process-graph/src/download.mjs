// The functions in this file are responsible for downloading the process graph from the QbDVision server.

import {OpenAPIProxy} from "./utils/open_api_proxy.mjs";
import fs from "fs";
import 'dotenv/config';

const API_KEY = process.env.API_KEY;
const BASE_URL = process.env.BASE_URL;
const PROJECT_ID = process.env.PROJECT_ID;
const PROCESS_ID = process.env.PROCESS_ID;

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
        throw e;
    }
    return result?.data;
}

// noinspection JSIgnoredPromiseFromCall
main();
