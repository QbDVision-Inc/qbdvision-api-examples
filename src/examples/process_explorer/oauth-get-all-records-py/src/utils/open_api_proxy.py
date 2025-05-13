import json
import os
import requests
from dotenv import load_dotenv
import gzip
import io

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
COGNITO_AUTHORIZATION_URL = os.getenv('COGNITO_AUTHORIZATION_URL')


class OpenAPIProxy:
    """
    This takes care of making requests to QbDVision's Open API (REST API) using OAuth.
    """

    def __init__(self, base_url):
        """
        Initialize the OpenAPIProxy with a base URL.
        
        Args:
            base_url (str): The base URL of the environment the API targets.
        """
        self.base_url = base_url
        self.access_token = None

    def login(self):
        """Authenticate using OAuth 2.0 client credentials flow."""
        try:
            payload = {
                'grant_type': 'client_credentials',
                'client_id': CLIENT_ID,
                'client_secret': CLIENT_SECRET
            }

            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            response = requests.post(
                COGNITO_AUTHORIZATION_URL,
                data=payload,
                headers=headers
            )
            response.raise_for_status()

            # The access_token in the response is already a JWT
            data = response.json()
            self.access_token = data['access_token']

        except Exception as error:
            print(f'Error: {error}')
            raise error

    def get_headers(self):
        """Get the headers for API requests with authorization token."""
        return {
            "Authorization": self.access_token
        }

    def get(self, url, params=None):
        """Make a GET request to the API."""
        full_url = f"{self.base_url}{url}"
        response = requests.get(full_url, params=params,
                                headers=self.get_headers())
        response.raise_for_status()
        return self.decompress_if_needed(response)

    def put(self, url, data=None, params=None):
        """Make a PUT request to the API."""
        full_url = f"{self.base_url}{url}"
        response = requests.put(full_url, json=data, params=params,
                                headers=self.get_headers())
        response.raise_for_status()
        return self.decompress_if_needed(response)

    def post(self, url, data=None, params=None):
        """Make a POST request to the API."""
        full_url = f"{self.base_url}{url}"
        response = requests.post(full_url, json=data, params=params,
                                 headers=self.get_headers())
        response.raise_for_status()
        return self.decompress_if_needed(response)

    def delete(self, url, params=None):
        """Make a DELETE request to the API."""
        full_url = f"{self.base_url}{url}"
        response = requests.delete(full_url, params=params,
                                   headers=self.get_headers())
        response.raise_for_status()
        return self.decompress_if_needed(response)

    def decompress_if_needed(self, response):
        """Decompress the response data if it's compressed."""
        result = {"data": response.json()}

        if result["data"] and "data" in result["data"]:
            input_data = result["data"]["data"]

            compressed_data = input_data.encode('latin-1')
            with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as f:
                decompressed_data = f.read()

            # Convert to string and parse JSON
            result["data"] = json.loads(decompressed_data.decode('utf-8'))

        return result
