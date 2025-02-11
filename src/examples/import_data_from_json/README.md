# Import/Update Unit Operations from JSON Example (Python)

This example demonstrates how to import and update Unit Operations (UOs) using the QbDVision REST API with Python.

## Prerequisites

- Python 3.13 or higher
- A QbDVision API key

## Getting Started (Import)

1. Create a project in QbDVision and note the project and process ID that you want to import data into.
2. Edit `ImportCookies.py` and replace `YOUR_API_KEY` with your QbDVision API key.
3. Modify `BASE_URL` to point to your QbDVision environment if you're not using Sandbox.
4. Modify `PROJECT_ID` and `PROCESS_ID` to be the project and process ID you identified in step 1.
5. Run the script
```bash
python3 ImportCookies.py
```

## Getting Started (Update)

1. Run the import script to create the UOs in QbDVision.
2. Edit `UpdateCookies.py` and replace `YOUR_API_KEY` with your QbDVision API key.
3. Modify `BASE_URL` to point to your QbDVision environment if you're not using Sandbox.
4. Modify `PROJECT_ID` and `UNIT_OPERATION_ID` to be the project and unit operation ID you want to update (likely created during import).
5. Run the script
```bash
python3 UpdateCookies.py
```

# Best Practices for Production
 - Use environment variables for API keys
 - Implement proper error handling and retries
 - Add logging/reporting according to your company's standards




