# Importing Certificate of Analysis (CoA) Records Example (Node.js)

This example demonstrates how to import batch records using the QbDVision REST API with Node.js.

## Prerequisites

- Node.js 20.x or higher
- A QbDVision API key

## Getting Started

1. Install the dependencies
```bash
npm install
```
2. Edit `import_coas.js` and replace `YOUR_API_KEY` with your QbDVision API key.
3. Modify `BASE_URL` to point to your QbDVision environment if you're not using Sandbox.
4. Create a directory called `./coaDocs` and place the documents you want to import in this directory.
```bash
cp /path/to/your/coa/files/* ./coaDocs
```
5. Run the script
```bash
node import_coas.js
```

# Best Practices for Production
 - Use environment variables for API keys
 - Implement proper error handling and retries
 - Add logging/reporting according to your company's standards
 - Use the latest versions of all libraries
