"use strict";

import * as XLSX from "xlsx";
import fs from "fs";

XLSX.set_fs(fs);

/**
 * This class is responsible for reading data from an Excel file.
 */
export default class ExcelReader {
    constructor(filePath) {
        this.filePath = filePath;
    }

    read() {
        const workbook = XLSX.readFile(this.filePath);
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];
        const rowArray = XLSX.utils.sheet_to_json(sheet, {header: 1});

        const header = rowArray[0];
        return this.convertRowDataToObjects(rowArray, header);
    }

    convertRowDataToObjects(rowArray, header) {
        const dataAsObjects = [];
        console.log(`Found ${rowArray.length - 1} rows in ${this.filePath}`);
        for (let i = 1; i < rowArray.length; i++) {
            let row = rowArray[i];

            const objectForRow = {};
            for (let j = 0; j < header.length; j++) {
                const headerCell = header[j];

                // Find duplicate columns
                let postFix = "";
                while (objectForRow[headerCell + postFix]) {
                    if (postFix === "") {
                        postFix = 2;
                    } else {
                        postFix++;
                    }
                }
                objectForRow[headerCell + postFix] = row[j];
            }
            dataAsObjects.push(objectForRow);
        }
        return dataAsObjects;
    }
}
