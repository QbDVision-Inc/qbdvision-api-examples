"use strict"

require('dotenv').config()
const {CoAOpenAPIProxy} = require("./coa_open_api_proxy");
const fs = require("fs");
const {program} = require("commander");

const API_KEY = process.env.API_KEY;
const BASE_URL = process.env.BASE_URL;
const openAPIProxy = new CoAOpenAPIProxy(API_KEY, BASE_URL);

(async () => {
    await main();
})();

/**
 * Ryan's best guess at what's going on in the JSON file.
 * @type {{}}
 */
const JSON_ARRAY_STRUCTURE = {
    UUID: 2,
    CATEGORY: 3,
    NAME: 4,
    SPECIFICATION_TXT: 5,
    VALUE: 6,
    BATCH_ID: 16,
    RETEST_DATE: 17,
    MANUFACTURE_DATE: 24,
    AMOUNT: 25,
    SIGN_OFF_DATE: 32,
};

async function main() {
    program
        .name("CoA API Import From JSON")
        .description("CLI to import a CoA from a JSON using QbDVision's REST API")
        .version("1.0.0")
        .requiredOption("-m, --materialId <value>", `The ID of the library material to import the CoA into.`)
        .requiredOption("-j, --jsonFile <value>", `The JSON file containing the CoA data.`)
        .requiredOption("-p, --pdfFile <value>", "The PDF file to upload as a reference for the CoA data.")
        .option("-l, --lotId <value>", "Override the Lot ID in the JSON file with this value.");

    program.parse();

    try {
        const options = program.opts();
        let {materialId, jsonFile, pdfFile, lotId} = options;

        // Sanitize the inputs
        materialId = parseInt(materialId, 10);
        const pdfFilename = pdfFile.split("/").pop();
        const jsonFilename = jsonFile.split("/").pop();

        console.log(`Loading CoA from ${jsonFile}`);
        const jsonContents = fs.readFileSync(jsonFile, "utf8");
        const jsonFromFile = JSON.parse(jsonContents);
        const keys = Object.keys(jsonFromFile);
        const coaArray = [];
        for (const key of keys) {
            coaArray.push(jsonFromFile[key].data);
        }

        console.log("Loading the Library Material...");
        const libraryMaterial = await loadLibraryMaterial(materialId);
        //console.log("Found Library Material:", libraryMaterial);
        console.log("Found Library Material with name:", libraryMaterial.name);

        console.log("Getting upload URL...");
        const uploadUrl = await getUploadUrl(pdfFilename);

        console.log(`Uploading file to "${uploadUrl.substring(0, 40)}...${uploadUrl.substring(uploadUrl.length - 20)}"...`);
        const coaAPIInfo = await uploadToS3(uploadUrl, pdfFile);

        console.log("Create the CoA object with the appropriate data...");
        const coaResults = createCoAResults(libraryMaterial, coaAPIInfo, coaArray, pdfFilename, jsonFilename, lotId);

        console.log(`Uploading CoA mapped tests to results${lotId ? " with Lot ID " + lotId : ""}...`);
        await importCoA(coaResults);

        console.log("CoA imported successfully!");
    } catch (error) {
        console.error("Error caught:", error);
    }
}

async function loadLibraryMaterial(materialId) {
    const response = await openAPIProxy.get(encodeURI(`editables/LibraryMaterial/${materialId}`));

    return response.data;
}

/**
 * In order to have access to S3 to upload the PDF file, we need to call an
 * API which will return a special URL that (for a short period) will give us
 * access to upload the file.
 * @param fileName
 * @return {Promise<*>}
 */
async function getUploadUrl(fileName) {
    const response = await openAPIProxy.get(encodeURI(`import/getCoAUploadUrl?fileName=${fileName}`));

    return response.data.url;
}

async function uploadToS3(uploadUrl, pdfFile) {
    const coaAPIInfo = {};
    let response;

    response = await openAPIProxy.uploadToS3(uploadUrl, pdfFile);

    coaAPIInfo.fileName = pdfFile;
    coaAPIInfo.fileVersion = response.headers["x-amz-version-id"];
    const regex = /(https:\/\/[-\w]*\.s3.amazonaws.com\/[-_\w]*\/([-\w]*)\/)/;
    const match = regex.exec(uploadUrl);
    if (match) {
        coaAPIInfo.fileKey = match[2];
    }

    return coaAPIInfo;
}

/**
 * This does the heavy lifting of creating the object that will be sent to
 * QbDVision's REST API to store the CoA results.
 * @param libraryMaterial The Library Material object that contains the specifications.
 * @param coaAPIInfo The information about the uploaded PDF file.
 * @param coaArray The json information read from disk.
 * @param pdfFilename The name of the PDF file.
 * @param jsonFilename The name of the JSON file.
 * @param lotId An optional lot ID to override the one in the JSON file.
 * @return {{modelName: string, supportFileData: string, smartImportUserStatistics: {importedFromJSON: boolean}, projectId: null, processId: null, importType: string, dependency: string, dataSectionName: string, isPaperImport: boolean, selectedDependencyRecord: string}}
 */
function createCoAResults(libraryMaterial, coaAPIInfo, coaArray, pdfFilename, jsonFilename, lotId) {
    const coaResults = {
        modelName: "Specification",
        supportFileData: JSON.stringify({
            name: pdfFilename,
            description: `Uploaded CoA along with results from ${jsonFilename}`,
            ...coaAPIInfo
        }),
        smartImportUserStatistics: {
            importedFromJSON: true,
        },
        projectId: null,
        processId: null,
        importType: "Append to batch data",
        dependency: "LibraryMaterial",
        dataSectionName: "Process Capability",
        isPaperImport: true,
        selectedDependencyRecord: `MTL-${libraryMaterial.id} - ${libraryMaterial.name}`,
    };

    // Create a map of Specification names to objects
    const specNameToObjectMap = new Map();
    libraryMaterial.Specifications.forEach(spec => {
        specNameToObjectMap.set(spec.name, spec);
    });

    // Iterate over the coaArray and create the objects to be imported
    const objectsToImport = [];
    for (const coaArrayElement of coaArray) {
        let qbdSpecName = getQbDVisionSpecName(coaArrayElement);
        if (!specNameToObjectMap.has(qbdSpecName)) {
            console.warn(`Specification not found for ${qbdSpecName}`);
            continue;
        }
        const specification = specNameToObjectMap.get(qbdSpecName);
        const objToImport = {
            attributeID: `MTLS-${specification.id}`,
            attributeName: "Infrared absorption spectrum / Id A",
            batchId: lotId || coaArrayElement[JSON_ARRAY_STRUCTURE.BATCH_ID],
            scale: coaArrayElement[JSON_ARRAY_STRUCTURE.AMOUNT],
            startDate: "",
            manufactureDate: coaArrayElement[JSON_ARRAY_STRUCTURE.MANUFACTURE_DATE],
            releaseDate: coaArrayElement[JSON_ARRAY_STRUCTURE.SIGN_OFF_DATE],
            expirationDate: coaArrayElement[JSON_ARRAY_STRUCTURE.RETEST_DATE],
        };
        if (specification.measure === "Conforms (Pass/Fail)") {
            objToImport.defectivePercentage = "0%";
            objToImport.measurements = [
                {
                    label: "Measurement 1",
                    value: coaArrayElement[JSON_ARRAY_STRUCTURE.VALUE],
                }
            ];
            objToImport["Measurement 1"] = coaArrayElement[JSON_ARRAY_STRUCTURE.VALUE];
        } else {
            /**
             * If you have multiple values/samples for the same product, you'd
             * create multiple measurements[] and then compute & set the min,
             * max, average and standard deviation (sd) values.
             */
            const value = convertToNumber(coaArrayElement[JSON_ARRAY_STRUCTURE.VALUE]);
            if (!value) {
                console.warn(`Value not found for ${qbdSpecName}. Value: ${coaArrayElement[JSON_ARRAY_STRUCTURE.VALUE]}`);
                continue;
            }
            objToImport.measurements = [
                {
                    label: "Measurement 1",
                    value: value,
                }
            ];
            objToImport["Measurement 1"] = value;
            objToImport.min = value;
            objToImport.max = value;
            objToImport.average = value;
            objToImport.sd = 0;
        }
        objectsToImport.push(objToImport);
    }
    coaResults.objectsToImport = JSON.stringify(objectsToImport);
    coaResults.allRecords = JSON.stringify(objectsToImport);

    return coaResults;
}

/**
 * The names including the categories were very long, so I shortened them using
 * this little algorithm.
 * @param coaArrayElement The row of the JSON file that contains the data.
 * @return {string} The name of the specification.
 */
function getQbDVisionSpecName(coaArrayElement) {
    let categoryName = "";
    const category = coaArrayElement[JSON_ARRAY_STRUCTURE.CATEGORY];
    if (category.startsWith("Impurity") || category === "ID, Purity, Assay and Impurity") {
        categoryName = "Impurity, ";
    } else if (category.startsWith("Residual solvents")) {
        categoryName = "Residual solvents, ";
    } else if (category.startsWith("Isomeric Purity")) {
        categoryName = "Isomeric Purity, ";
    }

    return categoryName + coaArrayElement[JSON_ARRAY_STRUCTURE.NAME];
}

/**
 * Converts a string to a number. It'll take something like "< approximately 1.0"
 * or "RR 2.0 1.0" and (in both cases) convert it to "1.0". It always takes the
 * last number in the string.
 *
 * @param someString The string that (hopefully) contains a number.
 * @return {string} A string that is just the number.
 */
function convertToNumber(someString) {
    // This regex looks for numbers with optional decimal points
    // It handles numbers preceded by characters like "<" or other text
    const regex = /[-+]?\d*\.?\d+/g;
    const matches = someString.match(regex);

    return matches && matches.length > 0 ? matches[matches.length - 1] : null;
}

/**
 * Send the data to QbDVision.
 * @param coaResults The CoA results to be imported.
 * @return {Promise<*>}
 */
async function importCoA(coaResults) {
    return await openAPIProxy.put("import/CoA/import", coaResults);
}
