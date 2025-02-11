# Importing Batch Records Example (Node.js)

This example demonstrates how to import batch records using the QbDVision REST API with Node.js.

## Prerequisites

- Node.js 20.x or higher
- A QbDVision API key

## Getting Started

1. Install the dependencies
```bash
npm install
```
2. Edit `import_batch_records.js` and replace `YOUR_API_KEY` with your QbDVision API key.
3. Modify `BASE_URL` to point to your QbDVision environment if you're not using Sandbox.
4. Create a directory called `./batchRecordDocs` and place the batch record documents you want to import in this directory.
```bash
mkdir batchRecordDocs
cp /path/to/your/batch/record/files/* ./batchRecordDocs
```
5. Edit `batchRecordDocsToProcessIds.json` to map the batch record documents to the (existing) process IDs in QbDVision.
```json
{
  "batch_record_1.pdf": 1,
  "batch_record_2.pdf": 42
}
```
6. Run the script
```bash
node import_batch_records.js
```
# Best Practices for Production
 - Use environment variables for API keys
 - Implement proper error handling and retries
 - Add logging/reporting according to your company's standards
 - Use the latest versions of all libraries
