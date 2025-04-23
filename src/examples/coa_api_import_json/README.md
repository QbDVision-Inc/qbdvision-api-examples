# Importing Certificate of Analysis (CoA) Record (from JSON) Example (Node.js)

This example demonstrates how to import a CoA (from JSON) using the QbDVision REST API with Node.js.

## Prerequisites

- Node.js 20.x or higher
- A QbDVision API key

## Getting Started

1. Install the dependencies
```bash
npm install
```
2. Copy `.env.example` to `.env` and edit it:
   1. Replace `API_KEY` with your QbDVision API key.
   2. Modify `BASE_URL` to point to your QbDVision environment if you're not using Sandbox.
4. (Optional) Place the documents you want to import in the `./coaDocs` directory.
```bash
cp /path/to/your/coa/files/* ./coaDocs
```
6. Run the script, passing in the material ID and the path to the JSON and PDF files:
```bash
node import_coas.js --materialId <MATERIAL_ID> --jsonFile <PATH_TO_JSON_FILE> --pdfFile <PATH_TO_PDF_FILE>
```

# Best Practices for Production
 - Use environment variables for API keys
 - Implement proper error handling and retries
 - Add logging/reporting according to your company's standards
 - Use the latest versions of all libraries
