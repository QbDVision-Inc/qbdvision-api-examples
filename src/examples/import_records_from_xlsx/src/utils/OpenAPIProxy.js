"use strict";

import axios from "axios";
import * as fflate from "fflate";

/**
 * This takes care of making requests to QbDVision's Open API (REST API) using an API key. This tests how clients
 * integrate with QbDVision.
 *
 * @type {exports.OpenAPIProxy}
 */
export default class OpenAPIProxy {
  /**
   * @param apiKey {string} The string of digits that QbDVision provides as an API Key.
   * @param baseURL The base URL of the environment the API targets.
   */
  constructor(apiKey, baseURL) {
    this.apiKey = apiKey;
    // Set config defaults for future API Calls

    this.axios = axios.create({
      baseURL,
      headers: {
        "qbdvision-api-key": this.apiKey,
      }
    });
  }

  // Delegate all the API calls to Axios. Read more here: https://axios-http.com/docs/api_intro
  async get(url, config) {
    return await this.decompressIfNeeded(await this.axios.get(url, config));
  }

  async put(url, data, config) {
    return await this.decompressIfNeeded(await this.axios.put(url, data, config));
  }

  async post(url, data, config) {
    return await this.decompressIfNeeded(await this.axios.post(url, data, config));
  }

  async delete(url, config) {
    return await this.decompressIfNeeded(await this.axios.delete(url, config));
  }

  decompressIfNeeded(result) {
    if (result.data?.data) {
      const input = result.data.data;
      // This is required in order to make fflate decompress.
      const fflateCompressedLatinEncodedStr = fflate.strToU8(input, true);

      // Decompress the compressed data
      const decompressedDataStream = fflate.decompressSync(fflateCompressedLatinEncodedStr);

      // Convert the decompressed data stream to string
      const decompressedString = fflate.strFromU8(decompressedDataStream);

      // Parse the string to JSON
      result.data = JSON.parse(decompressedString);
    }

    return result;
  }
}
