import * as fflate from 'fflate';
const {OpenAPIProxy} = require("../utils/open_api_proxy");

/**
 *
 * @type {exports.ProcessExplorerAPIProxy}
 */
export default class ProcessExplorerAPIProxy extends OpenAPIProxy {
  async getProcessExplorerData(projectId, processId) {
    const url = `/processExplorer/${projectId}?processId=${processId}`;
    let result;

    try {
      result = await this.get(url);
    } catch (e) {
      console.log(`Failed to get the process explorer data: ${e.message}`);
    }
    const treeData = this.decompress(result?.data?.data);
    console.log(treeData);
    return this.convertToGephi(JSON.parse(treeData))
  }

  decompress(input) {
    // This is required in order to make fflate decompress.
    let fflateCompressedLatinEncodedStr = fflate.strToU8(input, true);

    // Decompress the compressed data
    let decompressedDataStream = fflate.decompressSync(fflateCompressedLatinEncodedStr);

    // Convert the decompressed data stream to string
    return fflate.strFromU8(decompressedDataStream);
  };

  convertToGephi(treeData) {
    const gephiData = {
      nodes: [],
      edges: []
    }

    for(let map in treeData) {

      if(map === "processes") {
        for(let process of treeData[map]) {
          gephiData.nodes.push({
            label: process.name,
            id: `${process.typeCode}-${process.id}`
          })
        }
      } else {
        for(let recordKey in treeData[map]) {
          const record = treeData[map][recordKey]
          gephiData.nodes.push({
            label: record.name,
            id: `${record.typeCode}-${record.id}`
          });

          switch(map) {
            case "uoMap":
              gephiData.edges.push({
                source: `PR-${record.processId}`,
                target: `${record.typeCode}-${record.id}`,
              });
              break;
            case "stpMap":
              gephiData.edges.push({
                source: `UO-${record.UnitOperation.id}`,
                target: `${record.typeCode}-${record.id}`,
              });
              break;
            case "iqaMap":
            case "ipaMap":
              gephiData.edges.push({
                source: record.Step ? `STP-${record.Step.id}` : `UO-${record.UnitOperation.id}`,
                target: `${record.typeCode}-${record.id}`,
              });
              break;
            case "prcMap":
            case "maMap":
              for(let uo of record.UnitOperations) {
                gephiData.edges.push({
                  source: `UO-${uo.id}`,
                  target: `${record.typeCode}-${record.id}`,
                });
              }
              for(let step of record.Steps) {
                gephiData.edges.push({
                  source: `STP-${step.id}`,
                  target: `${record.typeCode}-${record.id}`,
                });
              }

              break;
          }
        }
      }
    }

    return gephiData;
  }
};
