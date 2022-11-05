import * as fflate from 'fflate';
import {DataSet} from "vis-data/peer";

const {OpenAPIProxy} = require("../utils/open_api_proxy");

const TYPE_CODE = {
  PROJECT: "PRJ",
  PROCESS: "PR",
  UNIT_OPERATION: "UO",
  STEP: "STP",
  MATERIAL: "MT",
  PROCESS_COMPONENT: "PRC",
  MATERIAL_ATTRIBUTE: "MA",
  PROCESS_PARAMETER: "PP",
  IA: "IA",
  IQA: "IQA",
  IPA: "IPA",
  FA: "FA",
};

const TYPE_CODE_TO_BACKGROUND_COLOR = {
  [TYPE_CODE.PROCESS]: "#f5f5f5",
  [TYPE_CODE.MATERIAL]: "#F0FBFC",
  [TYPE_CODE.PROCESS_COMPONENT]: "#E3F6FF",
  [TYPE_CODE.MATERIAL_ATTRIBUTE]: "#D1EAFF",
  [TYPE_CODE.PROCESS_PARAMETER]: "#C4E4F5",
  [TYPE_CODE.UNIT_OPERATION]: "#FEFBE5",
  [TYPE_CODE.STEP]: "#FEFBE5",
  [TYPE_CODE.IA]: "#EEF0F1",
  [TYPE_CODE.IQA]: "#EEF0F1",
  [TYPE_CODE.IPA]: "#EEF0F1",
  [TYPE_CODE.FA]: "#D5D7D8",
};

const TYPE_CODE_TO_SHAPE = {
  [TYPE_CODE.PROCESS]: "dot",
  [TYPE_CODE.MATERIAL]: "diamond",
  [TYPE_CODE.PROCESS_COMPONENT]: "box",
  [TYPE_CODE.MATERIAL_ATTRIBUTE]: "star",
  [TYPE_CODE.PROCESS_PARAMETER]: "triangle",
  [TYPE_CODE.UNIT_OPERATION]: "ellipse",
  [TYPE_CODE.STEP]: "ellipse",
  [TYPE_CODE.IQA]: "triangleDown",
  [TYPE_CODE.IPA]: "triangleDown",
};

/**
 *
 * @type {exports.ProcessExplorerAPIProxy}
 */
export default class ProcessExplorerAPIProxy extends OpenAPIProxy {
  constructor(API_KEY, BASE_URL) {
    super(API_KEY, BASE_URL);

    this.nodesDataSet = new DataSet([]);
    this.edgesDataSet = new DataSet([]);
  }

  async getProcessExplorerData(projectId, processId) {
    const url = `/processExplorer/${projectId}?processId=${processId}`;
    let result;

    try {
      result = await this.get(url);
    } catch (e) {
      console.log(`Failed to get the process explorer data: ${e.message}`);
    }
    const treeData = this.decompress(result?.data?.data);
    return this.getMapDataset(JSON.parse(treeData))
  }

  decompress(input) {
    // This is required in order to make fflate decompress.
    let fflateCompressedLatinEncodedStr = fflate.strToU8(input, true);

    // Decompress the compressed data
    let decompressedDataStream = fflate.decompressSync(fflateCompressedLatinEncodedStr);

    // Convert the decompressed data stream to string
    return fflate.strFromU8(decompressedDataStream);
  };

  getMapDataset(treeData) {
    // add the parsed data to the DataSets.
    this.nodesDataSet.clear();
    this.edgesDataSet.clear();

    const recordIdToNodeId = {};
    let counter = 1;

    // Add the nodes first
    for (let map in treeData) {
      if (map === "processes") {
        for (let process of treeData[map]) {
          this.nodesDataSet.add({
            id: counter,
            label: process.name,
            color: TYPE_CODE_TO_BACKGROUND_COLOR[process.typeCode],
            shape: TYPE_CODE_TO_SHAPE[process.typeCode]
          });
          recordIdToNodeId[`${process.typeCode}-${process.id}`] = counter;
          counter++;
        }
      } else {
        for (let recordKey in treeData[map]) {
          const record = treeData[map][recordKey];
          if(record.deletedAt) {
            continue;
          }
          this.nodesDataSet.add({
            id: counter,
            label: record.name,
            color: TYPE_CODE_TO_BACKGROUND_COLOR[record.typeCode],
            shape: TYPE_CODE_TO_SHAPE[record.typeCode]
          });
          recordIdToNodeId[`${record.typeCode}-${record.id}`] = counter;
          counter++;
        }
      }
    }

    // Then the edges
    for (let map in treeData) {
      for (let recordKey in treeData[map]) {
        const record = treeData[map][recordKey];

        switch (map) {
          case "uoMap":
            this.edgesDataSet.add({
              id: counter,
              from: recordIdToNodeId[record.PreviousUnitId ? `UO-${record.PreviousUnitId}` : `PR-${record.processId}`],
              to: recordIdToNodeId[`${record.typeCode}-${record.id}`],
            });
            break;
          case "stpMap":
            this.edgesDataSet.add({
              id: counter,
              from: recordIdToNodeId[`UO-${record.UnitOperation.id}`],
              to: recordIdToNodeId[`${record.typeCode}-${record.id}`],
            });
            break;
          case "iqaMap":
          case "ipaMap":
            this.edgesDataSet.add({
              id: counter,
              from: recordIdToNodeId[record.Step ? `STP-${record.Step.id}` : `UO-${record.UnitOperation.id}`],
              to: recordIdToNodeId[`${record.typeCode}-${record.id}`],
            });
            break;
          case "prcMap":
          case "mtMap":
            for (let uo of record.UnitOperations) {
              this.edgesDataSet.add({
                id: counter,
                from: recordIdToNodeId[`UO-${uo.id}`],
                to: recordIdToNodeId[`${record.typeCode}-${record.id}`],
              });
            }
            for (let step of record.Steps) {
              this.edgesDataSet.add({
                id: counter,
                from: recordIdToNodeId[`STP-${step.id}`],
                to: recordIdToNodeId[`${record.typeCode}-${record.id}`],
              });
            }
            break;
          case "maMap":
          case "ppMap":
            let from = recordIdToNodeId[`UO-${record.UnitOperation.id}`];
            if (record.MaterialId) {
              from = recordIdToNodeId[`MT-${record.MaterialId}`];
            } else if (record.ProcessComponentId) {
              from = recordIdToNodeId[`PRC-${record.ProcessComponentId}`];
            } else if (record.Step) {
              from = recordIdToNodeId[`PRC-${record.Step.id}`];
            }
            this.edgesDataSet.add({
              id: counter,
              from,
              to: recordIdToNodeId[`${record.typeCode}-${record.id}`],
            });
            break;
          default:
            break;
        }

        counter++;
      }
    }

    return {
      nodes: this.nodesDataSet,
      edges: this.edgesDataSet
    };
  }
};
