import { OpenAPIProxy } from "./utils/open_api_proxy.js";
import fs from "fs";
import 'dotenv/config';

const BASE_URL = process.env.BASE_URL;
const PROJECT_ID = process.env.PROJECT_ID;
const PROCESS_ID = process.env.PROCESS_ID;

main()

async function main() {
  const openAPIProxy = new OpenAPIProxy(BASE_URL);

  console.log("Logging in...")
  await openAPIProxy.login();

  console.log("Sending request to get process explorer data...");
  let result = await openAPIProxy.get(`/processExplorer/${PROJECT_ID}?processId=${PROCESS_ID}`);

  let allRecordKeys = [];
  allRecordKeys = allRecordKeys.concat(convertMapToArrayOfKeys(result.data?.uoMap));
  allRecordKeys = allRecordKeys.concat(convertMapToArrayOfKeys(result.data?.stpMap));
  allRecordKeys = allRecordKeys.concat(convertMapToArrayOfKeys(result.data?.mtMap));
  allRecordKeys = allRecordKeys.concat(convertMapToArrayOfKeys(result.data?.prcMap));
  allRecordKeys = allRecordKeys.concat(convertMapToArrayOfKeys(result.data?.ppMap));
  allRecordKeys = allRecordKeys.concat(convertMapToArrayOfKeys(result.data?.maMap));
  allRecordKeys = allRecordKeys.concat(convertMapToArrayOfKeys(result.data?.iqaMap));
  allRecordKeys = allRecordKeys.concat(convertMapToArrayOfKeys(result.data?.ipaMap));
  console.log(`Found the following keys: `, allRecordKeys);

  console.log("Sending request to get record data...");
  const dataResult = await openAPIProxy.put(`/editables/multiple/list/${PROJECT_ID}?onlyLoadDetails=true&includeHistory=true&includeClonedFrom=true&shouldCompress=true&includeFromLibrary=true&includeRecordOrder=true&returnLatestApprovedOrDraftVersionsOnly=true`, {requirementIds: allRecordKeys});

  const instances = dataResult.data.instances;
  console.log(`Received ${instances.length} records:`);
  for (const instance of instances) {
    console.log(`  ${instance.typeCode}-${instance.id}: ${instance.name}`);
  }

  console.log("Writing the data to all-records.json...");
  fs.writeFileSync("all-records.json", JSON.stringify(instances, null, 2));

  console.log("Done!");
}

function convertMapToArrayOfKeys(map) {
  const objects = Object.values(map).filter(obj => !obj.deletedAt);
  return objects.map(obj => obj.typeCode + "-" + obj.id);
}
