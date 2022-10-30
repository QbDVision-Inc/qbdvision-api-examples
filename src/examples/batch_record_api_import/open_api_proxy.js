"use strict";

const axios = require("axios");

/**
 * This takes care of making requests to QbDVision's Open API (REST API) using an API key. This tests how clients
 * integrate with QbDVision.
 *
 * @type {exports.OpenAPIProxy}
 */
module.exports.OpenAPIProxy = class OpenAPIProxy {
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

  // Delegate all of the API calls to Axios. Read more here: https://axios-http.com/docs/api_intro
  get(url, config) {
    return this.axios.get(url, config);
  }

  put(url, data, config) {
    return this.axios.put(url, data, config);
  }

  post(url, data, config) {
    return this.axios.post(url, data, config);
  }

  delete(url, config) {
    return this.axios.delete(url, config);
  }
};
