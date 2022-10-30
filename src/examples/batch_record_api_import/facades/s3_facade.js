"use strict";

const {OpenAPIProxy} = require("../open_api_proxy");
const fs = require("fs");

/**
 * This takes care of making requests to QbDVision's Open API (REST API) using an API key. This tests how clients
 * integrate with QbDVision.
 *
 * @type {exports.OpenAPIProxy}
 */
module.exports.S3Facade = class S3Facade extends OpenAPIProxy{
  async uploadToS3(url, filePath) {
    const fileData = fs.readFileSync(filePath);

    let result;
    try {
      result = await this.axios.put(url, fileData, {
        headers: {
          "x-amz-server-side-encryption": "AES256"
        }
      });
    } catch (e) {
      console.log(`S3 file upload operation failed: ${e.message}`);
    }
    return result;
  }
};
