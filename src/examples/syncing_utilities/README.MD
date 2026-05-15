# QbDVision Copy/Sync Utilities

Utilities for copying or syncing QbDVision project-level data and process-level data from a source environment into a target environment.

These utilities are provided "as is" - and no support from QbDVision will be provided.

Run the project sync first. A process belongs to a project, so the target project and project-level records must exist before syncing a process.

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

Open `.env` and replace the example values with your real source and target environment values. Do not commit `.env`; it contains API keys and is ignored by git.

## Configuration

All required values must be present in `.env`; the scripts do not prompt for missing values at runtime.

| Key | Used by | Description |
|---|---|---|
| `SOURCE_PROJECT_ID` | Both | Source project ID |
| `SOURCE_PROCESS_ID` | Process only | Source process ID |
| `TARGET_PROJECT_ID` | Process only | Target project ID that receives the process |
| `SOURCE_KEY` | Both | Source environment API key |
| `TARGET_KEY` | Both | Target environment API key |
| `SOURCE_HOST` | Both | Source host, for example `api.demo.qbdvision.com` |
| `SOURCE_BASE_PATH` | Both | Source base path, for example `demo` |
| `TARGET_HOST` | Both | Target host, for example `api.sandbox.qbdvision.com` |
| `TARGET_BASE_PATH` | Both | Target base path, for example `sandbox` |

If a required value is missing, or an ID value is not a whole number, the script reports the configuration error and stops.

## Run

Run the project sync first:

```powershell
python Copy_Sync_Project.py
```

If the project sync creates a new RMP, approve the RMP in QbDVision and rerun `Copy_Sync_Project.py`.

Then run the process sync:

```powershell
python Copy_Sync_Process.py
```

Run `Copy_Sync_Process.py` once for each source process you want to copy or sync. Update `SOURCE_PROCESS_ID` and `TARGET_PROJECT_ID` in `.env` before each run as needed.

## Preserve Mapping Files

Keep `project_id_map.json` and `process_id_map.json` after each successful run. These files are how the scripts remember which source records map to which existing target records.

If you delete or lose a mapping file, later runs will create new copies in the target instead of syncing updates into the records that were already created. This can result in duplicate projects, processes, and related records.

Recommended handling:

- Back up the mapping files
- Do not manually edit the mapping files

## Script Behavior

### `sync_common.py`

Shared helper module used by both sync scripts. Keep this file in the same folder as `Copy_Sync_Project.py` and `Copy_Sync_Process.py`.

Common responsibilities:

- Validates required `.env` values and integer IDs.
- Builds QbDVision API clients from host, base path, and API key values.
- Wraps API `GET` and `PUT` calls used by the sync scripts.
- Configures timestamped file logging.
- Loads and saves ID mapping files.
- Removes attachment links from `*links` fields before payloads are saved.
- Builds sanitized payloads from allowed field lists.
- Provides shared comparison, acceptance-criteria, ID mapping, and supplier-remapping helpers.

The project and process scripts still own the business rules for which records to copy, which fields are allowed, and how each record type is remapped.

### `Copy_Sync_Project.py`

Copies or syncs a source project into the target environment and maintains source-to-target ID mappings.

Synced entities:

- Project
- TPP Sections
- General Attributes
- Control Methods
- FPA
- FQA
- Drug Substance
- Drug Product

Key behaviors:

- Uses `project_id_map.json` to persist source-to-target IDs.
- Writes run logs to `logs/`.
- Remaps `RMPId` by RMP name and creates a missing RMP in the target.
- Remaps supplier references by supplier name and creates missing suppliers.
- Skips archived records.
- Removes attachment links from `*links` fields before sync; attachments must be copied manually.

### `Copy_Sync_Process.py`

Copies or syncs one source process into an existing target project.

Synced entities:

- Process
- Unit Operations
- Steps
- Process Components
- Materials
- Material Attributes
- Batch Record (Excludes Batch Attributes)
- Process Parameters
- IQAs
- IPAs
- Samples
- Drug Substance / Drug Product flows

Key behaviors:

- Uses `process_id_map.json` to persist source-to-target IDs.
- Writes run logs to `logs/`.
- Requires the source process to belong to `SOURCE_PROJECT_ID`.
- Relies on project-level entities already existing in the target.
- Syncs risk links after records are created.
- Syncs supplier IDs for Materials and Process Components at the end.

## Generated Files

- `project_id_map.json`: project/entity source-to-target ID mappings.
- `process_id_map.json`: process/entity source-to-target ID mappings.
- `logs/`: timestamped run logs for troubleshooting and audit.
