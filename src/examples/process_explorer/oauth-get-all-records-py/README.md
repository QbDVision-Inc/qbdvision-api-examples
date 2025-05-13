# Use OAuth 2.0 Keys to get all Process Explorer records (Python)

This example demonstrates how to use OAuth 2.0 keys to load all of the records in a 
process using the QbDVision REST API with Python.

## Prerequisites

- Python 3.9 or higher
- A QbDVision OAuth 2.0 API key
  - Currently you need to contact support@qbdvision.com to get an OAuth 2.0 key created.

## Getting Started

Here are some instructions to get you started with this sample:
1. Create a local virtual environment:
   ```bash
      python -m venv venv
   ```
   1. Activate the virtual environment. 
       1. On Windows:
       ```bash
      venv\Scripts\activate
       ```
       2. On macOS and Linux:
       ```bash
      source venv/bin/activate
       ```
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in:
    1. Modify `CLIENT_ID` to be your QbDVision OAuth 2.0 Client ID.
    1. Modify `CLIENT_SECRET` to be your QbDVision OAuth 2.0 Client Secret.
    3. Modify `COGNITO_AUTHORIZATION_URL` to be the Authorization URL provided by QbDVision.
    2. Modify `BASE_URL` to point to your QbDVision environment if you're not using Sandbox.
    3. Modify `PROJECT_ID` and `PROCESS_ID` to be the project and process ID that you want to see.
4. Run the example:
```bash
python src/index.py
````

## Output
The output will look something like this, with the full results written to a file named `all-records.json`:

```
(venv) ryanshillington:~/projects/qbdvision-api-examples/src/examples/process_explorer/oauth-get-all-records-py$ python src/index.py 
Logging in...
Sending request to get process explorer data...
Found the following keys: ['UO-370', 'UO-1437', 'UO-1458', 'MT-349', 'PRC-369', 'PRC-1864', 'MA-4071', 'MA-4072', 'MA-4074', 'IQA-2937']
Sending request to get record data...
Received 10 records:
  UO-1458: Bake the cake
  UO-1437: Package the cake
  UO-370: Mixing Ingredients
  MT-349: Batter
  PRC-369: Oven
  PRC-1864: Mixer
  MA-4074: Impurities - Baking
  MA-4072: Color
  MA-4071: pH
  IQA-2937: iqa1
Writing the data to all-records.json...
Done!
```

# Best Practices for Production
- Use a key store for API keys
- Implement proper error handling and retries
- Add logging/reporting according to your company's standards
- Use the latest versions of all libraries
