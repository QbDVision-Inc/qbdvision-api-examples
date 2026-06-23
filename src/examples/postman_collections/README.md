# QbDVision Postman Collections

This folder contains Postman collections with example QbDVision REST API requests:

- `Add Or Edit Record.postman_collection.json`
- `Get Single Record.postman_collection.json`
- `Lists.postman_collection.json`
- `Miscellaneous.postman_collection.json`

## Import Into Postman

1. Open Postman.
2. Select **Import**.
3. Choose one or more of the `.postman_collection.json` files from this folder.
4. After import, open each collection's authorization settings and set the static API key value.

These collections are configured for static API key authentication. The API key is sent using the `qbdvision-api-key` header.

## Before Sending Requests

The included requests are examples and are configured for the QbDVision sandbox by default. Update the request URLs and base paths so they point to your own QbDVision environment before using them.

For example, replace sandbox URLs like:

```text
https://api.sandbox.qbdvision.com/sandbox/...
```

with the host and path for your target environment.

The request bodies are examples only. They are meant to show the expected JSON shape for each request. Some field values, such as project IDs, record IDs, process IDs, user IDs, and other environment-specific references, must be changed to values that exist in your environment before the requests will work correctly.
