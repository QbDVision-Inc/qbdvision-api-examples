import OpenAPIProxy from "./utils/OpenAPIProxy.js";
import ExcelReader from "./utils/ExcelReader.js";
import "dotenv/config";

const API_KEY = process.env.API_KEY;
const BASE_URL = process.env.BASE_URL;
const PROJECT_ID = process.env.PROJECT_ID;
const NEW_PROCESS_NAME = process.env.NEW_PROCESS_NAME;
const EXCEL_FILE_PATH = process.env.EXCEL_FILE_PATH;

const TIME_TOKEN = "Time to load data from Excel";
console.time(TIME_TOKEN);
const openAPIProxy = new OpenAPIProxy(API_KEY, BASE_URL);

main();

async function main() {
    console.log(`Reading Excel File ${EXCEL_FILE_PATH}...`);
    const excelData = new ExcelReader(EXCEL_FILE_PATH).read();

    const process = await saveRecord("Process", {
        name: NEW_PROCESS_NAME,
        description: "This is a new process created by the Import Records from XLSX example script."
    });

    // Go through the excel rows and create records
    const uoNameToUOMap = new Map();
    const equipmentNameToPRCMap = new Map();
    let previousUOId = null;
    let lastEquipment = null;
    for (const row of excelData) {
        if (!row.description) {
            // Assume it's a blank line.
            continue;
        }

        // Add the UO if needed.
        let unitOperation;
        if (uoNameToUOMap.has(row.description)) {
            unitOperation = uoNameToUOMap.get(row.description);
        } else {
            unitOperation = await saveRecord("UnitOperation", {
                name: row.description,
                PreviousUnitId: previousUOId,
                ProcessId: process.id,
            });
            previousUOId = unitOperation.id;
            uoNameToUOMap.set(unitOperation.name, unitOperation);
        }

        // Add the proper record based on the Attribute column
        switch (row.Attribute) {
            case "operation":
                // Ignore
                break;
            case "material":
                let material = {
                    name: row.Value,
                    description: `This is a material for the ${unitOperation.name} unit operation.`,
                    category: "Drug Substance (API)",
                    use: "DS/DP",
                    UnitOperations: [unitOperation],
                    ProcessId: process.id,
                    MaterialFlows: [{
                        flow: "Input",
                        UnitOperationId: unitOperation.id,
                        ProcessId: process.id,
                        function: null,
                    }],
                };
                if (row.Attribute2 === "quantity") {
                    console.log(`Adding quantity to material (${row.Value2} ${row.Unit})...`);
                    material.quantityAbsolute = row.Value2;
                    material.descriptiveUnitAbsolute = row.Unit;
                }
                await saveRecord("Material", material);
                break;
            case "equipment":
            case "from_equipment":
            case "filter_equipment":
            case "ML_equipment":
            case "inline_filter":
                /* Re-use the equipment if it already exists (except for
                   inline_filter, which sounds like it's something you don't
                   re-use?)
                 */
                if (equipmentNameToPRCMap.has(row.Value) && row.Attribute !== "inline_filter") {
                    // Add this UO to the equipment
                    let equipment = equipmentNameToPRCMap.get(row.Value);
                    equipment.UnitOperations.push(unitOperation);
                    equipment = await saveRecord("ProcessComponent", equipment);
                    equipmentNameToPRCMap.set(equipment.name, equipment);
                    lastEquipment = equipment;
                } else {
                    lastEquipment = await saveRecord("ProcessComponent", {
                        name: row.Value,
                        description: `This is equipment for the ${unitOperation.name} unit operation.`,
                        UnitOperations: [unitOperation],
                        ProcessId: process.id,
                        type: "Equipment",
                    });
                    equipmentNameToPRCMap.set(lastEquipment.name, lastEquipment);
                }
                break;
            default:
                // Must be a process parameter
                const attachToLastEquipment = !!lastEquipment?.UnitOperations?.find(uo => uo.id === unitOperation.id);
                await saveRecord("ProcessParameter", {
                    name: row.Attribute,
                    description: `${row.Value} ${row.Unit}`,
                    measure: "Range",
                    UnitOperationId: unitOperation.id,
                    target: row.Value,
                    measurementUnits: row.Unit,
                    ProcessId: process.id,
                    ProcessComponentId: attachToLastEquipment && lastEquipment ? lastEquipment.id : null,
                });
                break;
        }
    }

    console.timeEnd(TIME_TOKEN);
    console.log("Done!");
}

/**
 * Saves (either create or update) a new record to the database. QbDVision knows
 * to create a new record if the ID is not provided.
 *
 * @param objectType The type of record, such as "Process", "UnitOperation", etc.
 * @param objectToSave The object to save.
 * @param objectToSave.name The name attribute of the record.
 * @param objectToSave.description The description attribute of the record.
 * @return {Promise<{id: number, name: string, description: string}>} The saved record that includes the ID.
 */
async function saveRecord(objectType, objectToSave) {
    console.log(`Creating a new ${objectType} named ${objectToSave.name}...`);
    objectToSave.ProjectId = PROJECT_ID;
    const response = await openAPIProxy.put(`/editables/${objectType}/addOrEdit`, objectToSave);
    const savedObject = response.data;
    console.log(`${objectType} ${savedObject.name} (id: ${savedObject.id}) saved successfully.`);
    return savedObject;
}

