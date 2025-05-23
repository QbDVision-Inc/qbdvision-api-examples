# Importing Certificate of Analysis (CoA) Records Example (Node.js)

This example demonstrates how to import a CoA using the QbDVision REST API with Node.js.

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
5. Update coaDocsToMaterialIds.json with the mapping of CoA documents to Library 
 Material IDs. The JSON file should look like this:
```json
{
  "CoADocument1.pdf": 1,
  "CoADocument2.pdf": 14
}
```
Where 1 and 14 are the ids of the Materials in QbDVision (ex. MT-1 and 
MT-14). Make the Material ID -1 if you want that file to be ignored.

6. Run the script
```bash
node import_coas.js
```

# Best Practices for Production
 - Use environment variables for API keys
 - Implement proper error handling and retries
 - Add logging/reporting according to your company's standards
 - Use the latest versions of all libraries
