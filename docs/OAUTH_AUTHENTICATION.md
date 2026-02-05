# OAuth 2.0 Authentication Guide

This guide explains how to authenticate with the QbDVision REST API using OAuth 2.0 credentials. OAuth 2.0 is the **recommended authentication method** as it provides better security and control compared to static API keys.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Getting Your OAuth Credentials](#getting-your-oauth-credentials)
- [The Authentication Flow](#the-authentication-flow)
- [Configuration Requirements](#configuration-requirements)
- [Making Your First Authentication Request](#making-your-first-authentication-request)
- [Using the Access Token](#using-the-access-token)
- [Troubleshooting](#troubleshooting)

## Overview

QbDVision uses the **OAuth 2.0 Client Credentials Grant** flow for API authentication. This flow is designed for server-to-server communication where your application needs to authenticate itself (not a specific user).

## Prerequisites

- A QbDVision account with your organization or an account on [Sandbox environment](https://sandbox.qbdvision.com/)
- Administrator or appropriate permissions to create API keys in your organization
- Access to your environment's Cognito authorization URL (see [Configuration Requirements](#configuration-requirements))

## Getting Your OAuth Credentials

1. **Navigate to the API Keys page**:
   - We base this documentation based on our [Sandbox environment](https://sandbox.qbdvision.com/users/list.html?showAPIKeys=true)

2. **Create a new API Key**:
   - Select Add > API Key
   - Select **"OAuth2"** as the API Key Type
   - Give your key a descriptive name for its use
   - Click the Generate OAuth 2.0 Credentials Button
   - Write down your credentials and save the key

3. **Copy your credentials**:
   - **Client ID**: A unique identifier given for your Key
   - **Client Secret**: Treat this like a password
   
   Store these credentials securely. Deleting the API Key entry will invalidate these credentials as well.

## The Authentication Flow

The OAuth 2.0 Client Credentials flow works in two steps:

```
┌─────────────┐                                    ┌──────────────────┐
│             │  1. Request Access Token           │                  │
│  Your App   │─────────────────────────────────>  │  Cognito (AWS)   │
│             │     (Client ID + Client Secret)    │                  │
│             │                                    │                  │
│             │  2. Receive Access Token (JWT)     │                  │
│             │ <───────────────────────────────── │                  │
└─────────────┘                                    └──────────────────┘
       │
       │ 3. Make API calls with token
       ▼
┌─────────────────────────────────────────────────┐
│          QbDVision REST API                     │
│         (Authorization: <access_token>)         │
└─────────────────────────────────────────────────┘
```

### Step-by-Step:

1. **Request a token**: Send your Client ID and Client Secret to the Cognito authorization endpoint
2. **Receive a JWT token**: The server responds with an access token (JSON Web Token)
3. **Use the token**: Include the token in the `Authorization` header of all API requests
4. **Token expires**: After expiration (typically 1 hour), request a new token

## Configuration Requirements

To authenticate, you need three pieces of information:

### 1. Client ID and Client Secret
Generated from the QbDVision UI (see [Getting Your OAuth Credentials](#getting-your-oauth-credentials))

### 2. Cognito Authorization URL
The authorization endpoint varies by environment:
Contact support@qbdvision.com if you need help with this entry.

**Note on Scope**: QbDVision's OAuth 2.0 Keys (App Clients) are preconfigured with `api/full` scope by default, which grants access to all APIs.

## Making Your First Authentication Request

Here's how to obtain an access token using different methods:

### Using curl

```bash
curl -X POST "https://your-cognito-domain.auth.region.amazoncognito.com/oauth2/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "client_secret=YOUR_CLIENT_SECRET"
```

### Using JavaScript (Node.js)

```javascript
const axios = require('axios');

const CLIENT_ID = 'YOUR_CLIENT_ID';
const CLIENT_SECRET = 'YOUR_CLIENT_SECRET';
const COGNITO_AUTHORIZATION_URL = 'https://your-cognito-domain.auth.region.amazoncognito.com/oauth2/token';

async function getAccessToken() {
  const params = new URLSearchParams();
  params.append('grant_type', 'client_credentials');
  params.append('client_id', CLIENT_ID);
  params.append('client_secret', CLIENT_SECRET);

  const response = await axios.post(COGNITO_AUTHORIZATION_URL, params, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  });

  return response.data.access_token;
}
```

### Using Python

```python
import requests

CLIENT_ID = 'YOUR_CLIENT_ID'
CLIENT_SECRET = 'YOUR_CLIENT_SECRET'
COGNITO_AUTHORIZATION_URL = 'https://your-cognito-domain.auth.region.amazoncognito.com/oauth2/token'

def get_access_token():
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    response = requests.post(COGNITO_AUTHORIZATION_URL, data=payload, headers=headers)
    response.raise_for_status()
    
    return response.json()['access_token']
```

### Expected Response

A successful authentication returns a JSON response:

```json
{
  "access_token": "eyJraWQiOiJ...[JWT token]...xyz",
  "expires_in": 3600,
  "token_type": "Bearer"
}
```

- **access_token**: The JWT token to use in API requests
- **expires_in**: Token lifetime in seconds (typically 3600 = 1 hour)
- **token_type**: Always "Bearer" for this flow

## Using the Access Token

Once you have an access token, include it in the `Authorization` header of every API request:

### HTTP Header Format

```
Authorization: <access_token>
```

### Example API Request

```bash
curl -X GET "https://sandbox.qbdvision.com/restAPI/records/UO-123" \
  -H "Authorization: eyJraWQiOiJ...[your_access_token]...xyz"
```

### JavaScript Example

```javascript
const axios = require('axios');

const accessToken = 'eyJraWQiOiJ...[your_access_token]...xyz';
const baseURL = 'https://sandbox.qbdvision.com/restAPI';

const api = axios.create({
  baseURL,
  headers: {
    'Authorization': accessToken
  }
});

// Make API calls
const record = await api.get('/records/UO-123');
```

### Python Example

```python
import requests

access_token = 'eyJraWQiOiJ...[your_access_token]...xyz'
base_url = 'https://sandbox.qbdvision.com/restAPI'

headers = {
    'Authorization': access_token
}

# Make API calls
response = requests.get(f'{base_url}/records/UO-123', headers=headers)
record = response.json()
```

## Troubleshooting

If you continue to experience issues please contact support.

Email support@qbdvision.com with:
 - Your environment
 - The error message or response you're receiving
 - Your Client ID
