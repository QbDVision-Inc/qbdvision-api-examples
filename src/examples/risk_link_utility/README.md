# QbDVision Risk Linking Utility

`CreateRiskLinksGeneric.py` creates QbDVision risk links from a CSV file using one consistent format for supported record types.

The script reads configuration from `.env`, reads link rows from a CSV file, fetches each target record, skips links that already exist, appends new links, and sends the updated record back to QbDVision.


## Setup

Prerequisite: Python 3.11+
              A QbDVision static API key

Create a local virtual environment:

```powershell
python -m venv venv
```

Activate the virtual environment.

On Windows:

```powershell
venv\Scripts\activate
```

On macOS and Linux:

```bash
source venv/bin/activate
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

Create your local `.env` file.

On Windows:

```powershell
Copy-Item .env.example .env
```

On macOS and Linux:

```bash
cp .env.example .env
```

Open `.env` and replace the example values with your real QbDVision values.

## Configuration

All required values must be present in `.env`; the script does not prompt for missing values at runtime.

| Key | Description |
|---|---|
| `API_KEY` | QbDVision API key for the target environment |
| `API_HOST` | Target host, for example `api.sandbox.qbdvision.com` |
| `API_BASE_PATH` | Target base path, for example `sandbox` |
| `CSV_FILE` | Path to the CSV file containing risk links to create |

If a required value is missing, or `CSV_FILE` does not exist, the script exits with an error.

## CSV Format

Required columns:

- `target_record`
- `linked_record`

Optional columns (these are optional because they may or may not be available depending on the RMP being used):

- `impact`
- `justification`
- `effect`
- `uncertainty`

Record values must use `TYPE-ID` format. Examples:

- `IQA-18`
- `IPA-91`
- `MA-8337`
- `PP-204`
- `FPA-152`
- `FQA-22`
- `GA-45`

Each CSV row creates one link. Invalid rows are skipped with a message showing the row number and row contents.

## Sample CSV

```csv
target_record,linked_record,impact,justification,effect,uncertainty
MA-8337,FPA-152,,Material attribute contributes to final performance,Removes,
PP-204,IQA-18,3,Process parameter affects intermediate quality,Adds,1
IPA-91,FQA-22,2,Intermediate performance attribute impacts final quality,Degrades,
IQA-18,IPA-91,1,IQA is influenced by IPA,,2
FPA-152,GA-45,3,Final performance is linked to this general attribute,,
FQA-22,GA-46,2,Final quality is linked to this general attribute,,1
```

## Supported Links

Supported target record types:

- `IQA`
- `IPA`
- `MA`
- `PP`
- `FPA`
- `FQA`

Supported link combinations:

- `IQA` -> `FPA`, `FQA`, `IPA`, `IQA`
- `IPA` -> `FPA`, `FQA`, `IQA`, `IPA`
- `MA` -> `FPA`, `FQA`, `IPA`, `IQA`
- `PP` -> `FPA`, `FQA`, `IPA`, `IQA`
- `FPA` -> `GA`
- `FQA` -> `GA`

## Run

From this folder:

```powershell
python CreateRiskLinksGeneric.py
```

## Behavior Notes

- Existing links are checked before insert; duplicates are skipped.
- Blank optional fields are omitted from the payload.

## Troubleshooting

- `CSV_FILE not found`: verify the path in `.env`.
- `Unsupported record type`: confirm the CSV uses exact prefixes like `IQA`, `IPA`, `MA`, `PP`, `FPA`, `FQA`, and `GA`.
- `Invalid record reference`: confirm values use `TYPE-ID` format such as `MA-8337`.
- `Not updated ...`: verify the API host, base path, API key, and record IDs.
- Duplicate-skip messages mean the target record already has that link.
