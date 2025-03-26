"use strict";

import axios from "axios";
import * as fflate from "fflate";
import 'dotenv/config';

const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const COGNITO_AUTHORIZATION_URL = process.env.COGNITO_AUTHORIZATION_URL;

/**
 * This takes care of making requests to QbDVision's Open API (REST API) using an API key. This tests how clients
 * integrate with QbDVision.
 *
 * @type {exports.OpenAPIProxy}
 */
export class OpenAPIProxy {
    /**
     * @param apiKey {string} The string of digits that QbDVision provides as an API Key.
     * @param baseURL The base URL of the environment the API targets.
     */
    constructor(baseURL) {
        this.baseURL = baseURL;
    }

    async login() {
        try {
            const params = new URLSearchParams();
            params.append('grant_type', 'client_credentials');
            params.append('client_id', CLIENT_ID);
            params.append('client_secret', CLIENT_SECRET);

            const response = await axios.post(
                COGNITO_AUTHORIZATION_URL,
                params,
                {
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded'
                    }
                }
            );

            // The access_token in the response is already a JWT
            this.accessToken = response.data.access_token;
            console.log("Received response data: ", response.data);
        } catch (error) {
            console.error('Error:', error);
            throw error;
        }

        // Set up future API calls
        const baseURL = this.baseURL;
        this.axios = axios.create({
            baseURL,
            headers: {
                "Authorization": this.accessToken,
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
