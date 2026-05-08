"""
Process copy tool.
Copies a single Process and its Unit Operations, Steps, Process Params, Materials, Material Attributes, Samples, IPAs, IQAs, and Process Components
Also handles DS / DP flows
from a source project into an existing target project.

Does NOT sync the supplier list between environments. SupplierId for Materials and Process Components is remapped
by supplier name (create if missing). These are synced at the very end, after records are created - to preserve
unit ops, steps, and flows.

Smart Content fields are not synced
"""

import requests
import json
import logging
import os
import sys
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, List
from collections import Counter
from dotenv import load_dotenv

# --------------------- CONFIG ---------------------

load_dotenv()

def required_int(name: str, value: str | None) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        print(f"Error: {name} must be a whole number.")
        sys.exit(1)

def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Error: Missing required environment value: {name}")
        print("Copy .env.example to .env and fill in all required values.")
        sys.exit(1)
    return value

# --------------------- LOGGING ---------------------

LOG_DIR = "logs"

def setup_logging(src_process_id: int, tgt_process_id: int | None = None) -> str:
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if tgt_process_id is None:
        tgt_process_id = "unknown"
    log_path = os.path.join(
        LOG_DIR,
        f"copy_process_src{src_process_id}_tgt{tgt_process_id}_{timestamp}.log",
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return log_path

logger = logging.getLogger("qbd_copy_process")

# --------------------- ALLOWED FIELDS ---------------------

ALLOWED_MATERIAL_FIELDS = [
    "name", "chemicalNameCAS", "category", "gmp", "use", "links", "description",
    "descriptiveUnitAbsolute", "quantityAbsolute", "quantityRelative",
    "quantityPerDose", "formulationQuantityLinks", "materialQualified", "partNumber", "internalPartNumber", "effectiveDate", "expirationDate",
    "qualificationLinks", "regulatoryFiling", "referenceNumber",
    "authorizationLetter", "regulatoryLinks", "drugSubstanceType",
    "form", "empiricalFormula", "density", "densityConditions",
    "chemicalStructure", "molecularWeight",
    "chemicalNameIUPAC", "otherNames", "innUsan", "casRegistryNumber",
    "compendialStandard", "certificateOfAnalysis",
    "propertiesLinks", "referencesLinks"
]
ALLOWED_MATERIAL_ATTRIBUTE_FIELDS = [
    "name", "ProcessId", "UnitOperationId", "ProcessComponentId", "StepId", "MaterialId",
    "type", "description", "potentialFailureModes", "recommendedActions", "riskLinks",
    "dataSpace", "measure", "group", "label", "lowerLimit", "target", "upperLimit",
    "measurementUnits", "targetJustification", "ControlMethods", "samplingPlan", "acceptanceCriteriaLinks", "AcceptanceCriteriaRanges",
    "capabilityRisk", "estimatedSampleSize", "capabilityJustification", "detectabilityRisk",
    "detectabilityJustification", "controlStrategy", "ccp", "controlStrategyJustification",
    "riskControlLinks", "referencesLinks", "impact", "riskAssessmentMethod"
]
ALLOWED_PROCESS_FIELDS = [
    "name", "description", "site", "gmp",
    "scale", "integrations", "referencesLinks",
]
ALLOWED_UNIT_OPERATION_FIELDS = [
    "name", "description", "risk", "input",
    "output", "links", "order"
]
ALLOWED_TIMEPOINT_FIELDS = [
    "name", "recordOrder"
]
ALLOWED_STEP_FIELDS = [
    "name", "description", "links"
]
ALLOWED_PROCESS_PARAMETER_FIELDS = [
    "name", "type", "description", "potentialFailureModes", "scaleDependent",
    "scaleJustification", "recommendedActions", "riskLinks", "dataSpace",
    "measure", "group", "label", "lowerLimit", "target", "upperLimit",
    "measurementUnits", "targetJustification", "samplingPlan", "lowerOperatingLimit",
    "upperOperatingLimit", "acceptanceCriteriaLinks", "AcceptanceCriteriaRanges",
    "capabilityRisk", "capabilityJustification", "estimatedSampleSize", "detectabilityRisk", "detectabilityJustification",
    "ccp", "controlStrategy", "controlStrategyJustification", "riskControlLinks", "referencesLinks", "impact", "riskAssessmentMethod"
]
ALLOWED_PROCESS_COMPONENT_FIELDS = [
    "name", "type", "function", "description", "certificateOfAnalysis", "links",
    "componentQualified", "partNumber", "internalPartNumber", "effectiveDate",
    "expirationDate", "componentQualificationLinks", "acceptanceTesting",
    "qualificationStatus", "calibration", "unitId", "unitQualificationLinks",
    "drugProductContact", "contactRisk", "contactRiskJustification",
    "cleaningValidation", "sterilizationValidation", "componentRiskLinks",
    "referencesLinks"
]
ALLOWED_IQA_FIELDS = [
    "name", "type", "description", "recommendedActions", "riskLinks", "dataSpace", "measure",
    "group", "label", "lowerLimit", "target", "upperLimit", "measurementUnits", "targetJustification", "samplingPlan", "acceptanceCriteriaLinks",
    "AcceptanceCriteriaRanges", "estimatedSampleSize", "capabilityRisk", "capabilityJustification",
    "detectabilityJustification", "detectabilityRisk", "controlStrategy", "controlStrategyJustification", "ccp",
    "riskControlLinks", "referencesLinks", "ControlMethods", "impact", "riskAssessmentMethod"
]
ALLOWED_IPA_FIELDS = [
    "name", "type", "description", "recommendedActions", "dataSpace", "measure",
    "group", "label", "lowerLimit", "target", "upperLimit",
    "measurementUnits", "targetJustification", "samplingPlan", "acceptanceCriteriaLinks", "AcceptanceCriteriaRanges",
    "estimatedSampleSize", "capabilityRisk", "capabilityJustification", "detectabilityJustification",
    "detectabilityRisk", "controlStrategy", "controlStrategyJustification", "ccp",
    "riskControlLinks", "referencesLinks", "ControlMethods", "impact", "riskAssessmentMethod"
]
ALLOWED_SAMPLE_FIELDS = [
    "name", "type", "description", "ProcessId", "StepId", "MaterialId", "UnitOperationId", "MatrixMaterialId",
    "numberOfAliquots", "amount", "amountUnit", "container", "contactMaterial",
    "storageCondition", "conditionUnit", "storageDuration", "durationUnit",
    "destination", "testingSite", "sampleLabel", "sampleCode", "documentCode",
    "internalName", "externalName", "sampleSpecificationsLinks",
    "sampleLogisticsLinks", "sampleNamesLinks", "referencesLinks"
]
ALLOWED_SUPPLIER_FIELDS = [
    "name", "address", "phone", "website", "servicesOrProducts",
    "supplierRank", "classification", "auditMethod", "dateCompleted",
    "nextAudit", "additionalAuditComments", "auditLinks",
    "qualificationStatus", "effectiveDate", "expirationDate",
    "qualityAgreement", "supplyAgreement",
    "additionalQualificationComments", "qualificationLinks",
    "riskRating", "riskJustification", "riskMitigation", "riskControl",
    "riskLinks", "primaryContactName", "primaryContactPhone",
    "primaryContactEmail", "primaryContactTitle",
    "regulatoryContactName", "regulatoryContactPhone",
    "regulatoryContactEmail", "regulatoryContactTitle",
    "qualityContactName", "qualityContactPhone", "qualityContactEmail",
    "qualityContactTitle", "otherContactName", "otherContactPhone",
    "otherContactEmail", "otherContactTitle",
]
ACR_FIELDS = [
    "group", "label", "isDefault", "lowerLimit", "target",
    "upperLimit", "measurementUnits", "targetJustification",
]
REQUIRED_FIELDS_BY_TYPE = {
    "Process": ALLOWED_PROCESS_FIELDS,
    "UnitOperation": ALLOWED_UNIT_OPERATION_FIELDS,
    "Step": ALLOWED_STEP_FIELDS,
    "Material": ALLOWED_MATERIAL_FIELDS,
    "MaterialAttribute": ALLOWED_MATERIAL_ATTRIBUTE_FIELDS,
    "ProcessComponent": ALLOWED_PROCESS_COMPONENT_FIELDS,
    "ProcessParameter": ALLOWED_PROCESS_PARAMETER_FIELDS,
    "IPA": ALLOWED_IPA_FIELDS,
    "IQA": ALLOWED_IQA_FIELDS,
    "Sample": ALLOWED_SAMPLE_FIELDS,
}

# --------------------- API CLIENTS ---------------------

class QbdApiClient:
    def __init__(self, base_url: str, api_key: str):
        if not base_url:
            raise ValueError("base_url is required")
        if not api_key:
            raise ValueError("api_key is required")
        self.base_url = base_url.rstrip("/") + "/"
        self.headers = {
            "Content-Type": "application/json",
            "qbdvision-api-key": api_key,
        }

    @classmethod
    def from_host(cls, host: str, base_path: str, api_key: str) -> "QbdApiClient":
        host = (host or "").strip().rstrip("/")
        base_path = (base_path or "").strip().strip("/")
        if not host or not base_path:
            raise ValueError("host and base_path are required")
        return cls(f"https://{host}/{base_path}/", api_key)

    def get(self, path: str, params: dict | None = None) -> Any:
        r = requests.get(self.url(path), headers=self.headers, params=self.clean_params(params))
        r.raise_for_status()
        return r.json() if r.text else {}

    def put(self, path: str, payload: Dict[str, Any]) -> Any:
        r = requests.put(self.url(path), headers=self.headers, json=payload)
        r.raise_for_status()
        return r.json() if r.text else {}

    def get_record(self, record_type: str, record_id: int, *, approved: bool = False) -> dict:
        return self.get(
            f"editables/{record_type}/{record_id}",
            params={"approved": str(approved).lower()},
        )

    def list_records(self, record_type: str, project_id: int | None = None, **params) -> dict:
        path = f"editables/{record_type}/list"
        if project_id is not None:
            path = f"{path}/{project_id}"
        return self.get(path, params=params)

    def save_record(self, record_type: str, payload: Dict[str, Any]) -> Any:
        return self.put(f"editables/{record_type}/addOrEdit", payload)

    def process_explorer(self, project_id: int, process_id: int | None = None) -> dict:
        return self.get("processExplorer/" + str(project_id), params={"processId": process_id})

    def url(self, path: str) -> str:
        return self.base_url + path.lstrip("/")

    @staticmethod
    def clean_params(params: dict | None) -> dict | None:
        if not params:
            return None
        return {k: v for k, v in params.items() if v is not None}

class SyncWriter:
    def __init__(self, client: QbdApiClient):
        self.client = client

    def save_record(self, record_type: str, payload: Dict[str, Any], *, reason: str | None = None) -> Any:
        if reason:
            logger.debug("Saving %s: %s", record_type, reason)
        return self.client.save_record(record_type, payload)

    def save_fn(self, record_type: str, *, reason: str | None = None):
        return lambda payload: self.save_record(record_type, payload, reason=reason)

@dataclass(frozen=True)
class SyncConfig:
    src_project_id: int
    src_process_id: int
    tgt_project_id: int
    src_client: QbdApiClient
    tgt_client: QbdApiClient

def load_config() -> SyncConfig:
    src_project_id = required_int("SOURCE_PROJECT_ID", required_env("SOURCE_PROJECT_ID"))
    src_process_id = required_int("SOURCE_PROCESS_ID", required_env("SOURCE_PROCESS_ID"))
    tgt_project_id = required_int("TARGET_PROJECT_ID", required_env("TARGET_PROJECT_ID"))

    return SyncConfig(
        src_project_id=src_project_id,
        src_process_id=src_process_id,
        tgt_project_id=tgt_project_id,
        src_client=QbdApiClient.from_host(
            required_env("SOURCE_HOST"),
            required_env("SOURCE_BASE_PATH"),
            required_env("SOURCE_KEY"),
        ),
        tgt_client=QbdApiClient.from_host(
            required_env("TARGET_HOST"),
            required_env("TARGET_BASE_PATH"),
            required_env("TARGET_KEY"),
        ),
    )

# --------------------- RECORD HELPERS ---------------------

def strip_attachment_links(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove Attachment links from any *links field (e.g., links, riskLinks, referencesLinks).
    Applies recursively to nested dicts/lists. Preserves original type (JSON string or list).
    """
    stack = [payload]

    while stack:
        obj = stack.pop()
        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                if isinstance(k, str) and k.lower().endswith("links"):
                    if isinstance(v, str):
                        try:
                            data = json.loads(v)
                        except Exception:
                            continue
                        if isinstance(data, list):
                            filtered = [
                                item for item in data
                                if not (isinstance(item, dict) and item.get("linkType") == "Attachment")
                            ]
                            obj[k] = json.dumps(filtered)
                        continue

                    if isinstance(v, list):
                        filtered = [
                            item for item in v
                            if not (isinstance(item, dict) and item.get("linkType") == "Attachment")
                        ]
                        obj[k] = filtered
                    continue

                if isinstance(v, (dict, list)):
                    stack.append(v)
            continue

        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return payload

def validate_target_scope(record: dict | None, project_id: int, process_id: int, label: str) -> dict | None:
    if not record or not isinstance(record, dict):
        return None

    rec_project = record.get("ProjectId") or record.get("projectId")
    rec_process = record.get("ProcessId") or record.get("processId")

    if rec_project is not None and rec_project != project_id:
        logger.warning("Ignoring %s id %s from project %s (expected %s)", label, record.get("id"), rec_project, project_id)
        return None

    if rec_process is not None and rec_process != process_id:
        logger.warning("Ignoring %s id %s from process %s (expected %s)", label, record.get("id"), rec_process, process_id)
        return None

    return record

def ensure_full_record(record_type: str, record: dict | None, client: QbdApiClient) -> dict | None:
    if not record or not isinstance(record, dict):
        return record

    required = REQUIRED_FIELDS_BY_TYPE.get(record_type)
    if required:
        for f in required:
            if f not in record or record.get(f) is None:
                rec_id = record.get("id")
                if not rec_id:
                    return record
                return client.get_record(record_type, rec_id)

    if "LastVersionId" in record:
        return record

    rec_id = record.get("id")
    if not rec_id:
        return record

    return client.get_record(record_type, rec_id)

def get_process_explorer(
    client: QbdApiClient,
    project_id: int,
    process_id: int,
) -> dict:
    return client.process_explorer(project_id, process_id)

# --------------------- LOOKUP & MAPPING HELPERS ---------------------

def normalize_id(val):
    try:
        return int(val)
    except Exception:
        return val

def map_lookup(mapping: dict, key):
    if key is None:
        return None
    if key in mapping:
        return mapping[key]
    k = str(key)
    if k in mapping:
        return mapping[k]
    try:
        ik = int(key)
        if ik in mapping:
            return mapping[ik]
    except Exception:
        pass
    return None

def resolve_target_by_name(
    *,
    src_id,
    src_name,
    prev_mapping: dict,
    tgt_by_name: dict,
    duplicate_names: set,
    record_type: str,
    record_label: str,
    tgt_client: QbdApiClient,
    tgt_project_id: int,
    tgt_process_id: int,
    fallback_when_mapped_invalid: bool = False,
    fetch_fallback_full: bool = True,
) -> tuple[int | None, dict | None]:
    tgt_id = map_lookup(prev_mapping, src_id)
    tgt_record = tgt_client.get_record(record_type, tgt_id) if tgt_id else None
    tgt_record = validate_target_scope(tgt_record, tgt_project_id, tgt_process_id, record_label)

    if tgt_id and not tgt_record and fallback_when_mapped_invalid:
        tgt_id = None

    if not tgt_id and src_name in tgt_by_name and src_name not in duplicate_names:
        tgt_record = tgt_by_name[src_name]
        tgt_id = tgt_record["id"]
        if fetch_fallback_full:
            tgt_record = tgt_client.get_record(record_type, tgt_id)

    if not tgt_id and src_name in duplicate_names:
        logger.info("%s '%s' has duplicate name in source; skipping name-based fallback", record_label, src_name)

    return tgt_id, tgt_record

def resolve_target_by_lookup(
    *,
    src_id,
    prev_mapping: dict,
    fallback_key,
    tgt_lookup: dict,
    record_type: str,
    record_label: str,
    tgt_client: QbdApiClient,
    tgt_project_id: int,
    tgt_process_id: int,
) -> tuple[int | None, dict | None]:
    tgt_id = map_lookup(prev_mapping, src_id)
    if tgt_id:
        tgt_record = tgt_client.get_record(record_type, tgt_id)
        tgt_record = validate_target_scope(tgt_record, tgt_project_id, tgt_process_id, record_label)
        return tgt_id, tgt_record

    tgt_record = tgt_lookup.get(fallback_key)
    return (tgt_record.get("id"), tgt_record) if tgt_record else (None, None)

def find_duplicate_keys(records, key_fn):
    counts = Counter()
    for r in records or []:
        key = key_fn(r)
        if key is None:
            continue
        counts[key] += 1
    return {k for k, c in counts.items() if c > 1}

def list_process_records(
    client: QbdApiClient,
    record_type: str,
    project_id: int,
    process_id: int,
) -> list:
    return client.list_records(record_type, project_id, processId=process_id).get("instances", [])

def list_project_records(client: QbdApiClient, record_type: str, project_id: int) -> list:
    return client.list_records(record_type, project_id).get("instances", [])

def get_scoped_record(
    client: QbdApiClient,
    record_type: str,
    record_id: int,
    project_id: int,
    process_id: int,
    label: str,
) -> dict | None:
    record = client.get_record(record_type, record_id)
    return validate_target_scope(record, project_id, process_id, label)

def active_source_full_record(
    client: QbdApiClient,
    record_type: str,
    source_stub: dict,
    project_id: int,
    process_id: int,
    label: str,
) -> dict | None:
    if is_archived(source_stub):
        return None

    if not validate_target_scope(source_stub, project_id, process_id, label):
        return None

    full_source = get_scoped_record(
        client,
        record_type,
        source_stub["id"],
        project_id,
        process_id,
        label,
    )
    if not full_source or is_archived(full_source):
        return None
    return full_source

def log_duplicate_name_fallback(record_label: str, duplicate_names: set) -> None:
    if duplicate_names:
        logger.info(
            "Duplicate %s names in source; disabling name-based fallback for: %s",
            record_label,
            ", ".join(sorted(duplicate_names)),
        )

def name_record_lookup(records: list) -> dict:
    return {record["name"]: record for record in records if record.get("name")}

def name_id_lookup(records: list) -> dict:
    return {record["name"]: record["id"] for record in records if record.get("name") and record.get("id")}

def build_name_based_id_mapping(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    record_type: str,
    src_project_id: int,
    tgt_project_id: int,
) -> dict:
    src_records = list_project_records(src_client, record_type, src_project_id)
    tgt_by_name = name_id_lookup(list_project_records(tgt_client, record_type, tgt_project_id))
    return {
        src_record["id"]: tgt_by_name.get(src_record["name"])
        for src_record in src_records
        if src_record.get("name") in tgt_by_name
    }

def convert_map_to_record_keys(map_data: dict) -> List[str]:
    if not map_data:
        return []
    return [
        f"{obj['typeCode']}-{obj['id']}"
        for obj in map_data.values()
        if not obj.get("deletedAt") and obj.get("typeCode") and obj.get("id")
    ]

def build_target_lookup(
    client: QbdApiClient,
    project_id,
    record_type,
    *,
    return_full=False,
):
    instances = client.list_records(record_type, project_id).get("instances", [])

    if return_full:
        return {r["name"]: r for r in instances if r.get("name")}
    else:
        return {r["name"]: r["id"] for r in instances if r.get("name")}

# --------------------- ACCEPTANCE CRITERIA ---------------------

def parse_json_container(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value

def parsed_list_or_none(value) -> list | None:
    value = parse_json_container(value)
    return value if isinstance(value, list) else None

def parsed_dict_or_none(value) -> dict | None:
    value = parse_json_container(value)
    return value if isinstance(value, dict) else None

def acceptance_criteria_ranges_from_container(container) -> list | None:
    container = parsed_dict_or_none(container)
    if not container:
        return None

    for field in ("AcceptanceCriteriaRanges", "AcceptanceCriteriaRangeLinkedVersions"):
        ranges = parsed_list_or_none(container.get(field))
        if ranges is not None:
            return ranges

    return None

def acceptance_criteria_ranges_from_record(record: dict) -> list | None:
    for source in (
        record.get("Requirement"),
        record,
        record.get("RequirementVersion"),
    ):
        ranges = acceptance_criteria_ranges_from_container(source)
        if ranges is not None:
            return ranges

    return None

def normalize_acr_value(val):
    if val in ("", None):
        return None
    if isinstance(val, str):
        obj = parse_json_container(val)
        if obj in ({}, []):
            return None
        if isinstance(obj, (list, dict)):
            return obj
    if val in ({}, []):
        return None
    return val

def normalize_acceptance_criteria_ranges_list(ranges: list) -> list:
    cleaned = []
    for r in ranges:
        if not isinstance(r, dict):
            continue
        cleaned.append({k: normalize_acr_value(r.get(k)) for k in ACR_FIELDS})

    def _sortable(v):
        if v is None:
            return (0, "")
        return (1, str(v))

    def _key(item):
        return tuple(_sortable(item.get(f)) for f in ACR_FIELDS)

    return sorted(cleaned, key=_key)

def add_acr_to_payload(full_src: dict, payload: dict) -> dict | None:
    if not isinstance(full_src, dict):
        return None

    src_ranges = acceptance_criteria_ranges_from_record(full_src)

    if not src_ranges:
        return None

    src_ranges = normalize_acceptance_criteria_ranges_list(src_ranges)
    payload["AcceptanceCriteriaRanges"] = src_ranges
    return {"AcceptanceCriteriaRanges": src_ranges}

def add_tgt_acr_for_diff(tgt_full: dict) -> dict:
    if not isinstance(tgt_full, dict):
        return tgt_full

    tgt_ranges = acceptance_criteria_ranges_from_container(tgt_full.get("Requirement"))
    if tgt_ranges is None:
        tgt_ranges = parsed_list_or_none(tgt_full.get("AcceptanceCriteriaRanges"))
    if isinstance(tgt_ranges, list):
        copy = dict(tgt_full)
        copy["AcceptanceCriteriaRanges"] = normalize_acceptance_criteria_ranges_list(tgt_ranges)
        return copy

    return tgt_full

# --------------------- NORMALIZATION & DIFF ---------------------

def normalize_whitespace(text: str):
    collapsed = " ".join(text.split())
    return collapsed if collapsed else None

def normalize(val):
    """Normalize values for comparison and ignore whitespace-only string diffs."""
    if val is None:
        return None

    if isinstance(val, str):
        try:
            obj = json.loads(val)
            if isinstance(obj, (list, dict)):
                return normalize(obj)
            if isinstance(obj, str):
                return normalize_whitespace(obj)
        except Exception:
            pass
        return normalize_whitespace(val)

    if isinstance(val, list):
        return [normalize(v) for v in val]

    if isinstance(val, dict):
        return {k: normalize(v) for k, v in val.items()}

    return val

def param_changed(src, tgt):
    """
    Compare src and tgt after normalization.
    Handles scalars (including numeric strings), lists, and dicts.
    """
    src_n = normalize(src)
    tgt_n = normalize(tgt)

    if isinstance(src_n, (list, dict)) or isinstance(tgt_n, (list, dict)):
        return src_n != tgt_n

    try:
        if src_n is not None and tgt_n is not None:
            return float(src_n) != float(tgt_n)
    except (ValueError, TypeError):
        pass

    return src_n != tgt_n

def freeze_for_compare(val):
    val = normalize(val)

    if isinstance(val, dict):
        return {k: freeze_for_compare(v) for k, v in sorted(val.items())}

    if isinstance(val, list):
        frozen = [freeze_for_compare(v) for v in val]
        return sorted(frozen, key=lambda v: json.dumps(v, sort_keys=True, default=str))

    return val

def changed_fields_for(payload: dict, target: dict, fields: list, *, skip: set | None = None) -> list:
    skip = skip or set()
    return [
        field
        for field in fields
        if field not in skip and param_changed(payload.get(field), target.get(field))
    ]

def frozen_changed_fields_for(payload: dict, target: dict, fields: list) -> list:
    return [
        field
        for field in fields
        if freeze_for_compare(payload.get(field)) != freeze_for_compare(target.get(field))
    ]

def id_set(records: list | None) -> set:
    return {record.get("id") for record in records or []}

def tuple_set(records: list | None, fields: tuple[str, ...]) -> set:
    return {tuple(record.get(field) for field in fields) for record in records or []}

def append_relationship_id_diff(
    changed_fields: list,
    field_name: str,
    src_records: list,
    tgt_records: list,
    *,
    record_label: str,
    record_name: str,
) -> None:
    src_ids = id_set(src_records)
    tgt_ids = id_set(tgt_records)
    if src_ids == tgt_ids:
        return

    changed_fields.append(field_name)
    logger.info(
        "%s '%s' %s diff: src=%s tgt=%s",
        record_label,
        record_name,
        field_name,
        sorted(src_ids),
        sorted(tgt_ids),
    )

# --------------------- RELATIONSHIP HELPERS ---------------------

def mapped_id_relationships(records: list, mapping: dict, *source_id_fields: str, label_prefix: str | None = None) -> list:
    relationships = []
    for record in records:
        mapped_id = None
        for field_name in source_id_fields:
            mapped_id = map_lookup(mapping, record.get(field_name))
            if mapped_id:
                break
        if not mapped_id:
            continue

        relationship = {"id": mapped_id}
        if label_prefix:
            relationship["label"] = f"{label_prefix}-{mapped_id}"
        relationships.append(relationship)
    return relationships

def append_relationship_diffs(
    changed_fields: list,
    payload: dict,
    target: dict,
    field_names: tuple[str, ...],
    *,
    record_label: str,
    record_name: str,
) -> None:
    for field_name in field_names:
        append_relationship_id_diff(
            changed_fields,
            field_name,
            payload.get(field_name, []),
            target.get(field_name, []),
            record_label=record_label,
            record_name=record_name,
        )

def build_material_flow_relationships(
    material_flows: list,
    uo_mapping: dict,
    step_mapping: dict,
    tgt_process_id: int,
) -> tuple[list, list, list]:
    mapped_flows = []
    for flow in material_flows:
        tgt_uo_id = map_lookup(uo_mapping, flow.get("UnitOperationId"))
        tgt_step_id = map_lookup(step_mapping, flow.get("StepId"))

        if not tgt_uo_id and not tgt_step_id:
            continue

        flow_type = flow.get("flow", "Input")
        if flow_type not in {"Input", "Intermediate", "Output"}:
            flow_type = "Input"

        mapped_flows.append({
            "ProcessId": tgt_process_id,
            "UnitOperationId": tgt_uo_id,
            "StepId": tgt_step_id,
            "flow": flow_type,
        })

    uos = [
        {"id": flow["UnitOperationId"], "label": f"UO-{flow['UnitOperationId']}"}
        for flow in mapped_flows
        if flow["UnitOperationId"]
    ]
    steps = [
        {"id": flow["StepId"], "label": f"STP-{flow['StepId']}"}
        for flow in mapped_flows
        if flow["StepId"]
    ]
    return mapped_flows, uos, steps


# --------------------- PAYLOAD BUILDERS ---------------------

def sanitize_payload(src: dict, allowed_fields: list, extra_fields: dict) -> dict:
    payload = {k: src[k] for k in allowed_fields if k in src}
    payload.update(extra_fields)
    return strip_attachment_links(payload)

def build_process_payload(src_process: dict, tgt_project_id: int) -> dict:
    return sanitize_payload(src_process, ALLOWED_PROCESS_FIELDS, {"ProjectId": tgt_project_id})

def build_unit_operation_order_payload(tgt_process: dict, tgt_order: list) -> dict:
    payload = sanitize_payload(
        tgt_process,
        ALLOWED_PROCESS_FIELDS,
        {"unitOperationOrder": json.dumps(tgt_order)},
    )
    payload["id"] = tgt_process["id"]
    payload["LastVersionId"] = tgt_process["LastVersionId"]
    return payload

def build_unit_operation_payload(
    src_uo: dict,
    tgt_project_id: int,
    tgt_process_id: int,
    timepoints: list | None = None,
) -> dict:
    extra = {"ProjectId": tgt_project_id, "ProcessId": tgt_process_id}
    if timepoints is not None:
        extra["Timepoints"] = timepoints
    return sanitize_payload(src_uo, ALLOWED_UNIT_OPERATION_FIELDS, extra)

def build_unit_operation_timepoints_update_payload(
    src_uo: dict,
    tgt_uo_id: int,
    last_version_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    timepoints: list,
) -> dict:
    return sanitize_payload(
        src_uo,
        ALLOWED_UNIT_OPERATION_FIELDS,
        {
            "id": tgt_uo_id,
            "LastVersionId": last_version_id,
            "ProjectId": tgt_project_id,
            "ProcessId": tgt_process_id,
            "Timepoints": timepoints,
        },
    )

def build_step_payload(full_src: dict, tgt_project_id: int, tgt_process_id: int, tgt_uo_id: int) -> dict:
    return sanitize_payload(
        full_src,
        ALLOWED_STEP_FIELDS,
        {
            "ProjectId": tgt_project_id,
            "ProcessId": tgt_process_id,
            "UnitOperationId": tgt_uo_id,
        },
    )

def build_step_order_payload(tgt_uo: dict, tgt_uo_id: int, new_order: list) -> dict:
    return sanitize_payload(
        tgt_uo,
        ALLOWED_UNIT_OPERATION_FIELDS,
        {
            "id": tgt_uo_id,
            "LastVersionId": tgt_uo["LastVersionId"],
            "ProcessId": tgt_uo["ProcessId"],
            "stepOrder": json.dumps(new_order),
        },
    )

def build_process_component_payload(
    full_src: dict,
    tgt_project_id: int,
    tgt_process_id: int,
    uos: list,
    steps: list,
) -> dict:
    return sanitize_payload(
        full_src,
        ALLOWED_PROCESS_COMPONENT_FIELDS,
        {
            "ProjectId": tgt_project_id,
            "ProcessId": tgt_process_id,
            "UnitOperations": uos,
            "Steps": steps,
        },
    )

def build_material_payload(
    full_src: dict,
    tgt_project_id: int,
    tgt_process_id: int,
    uos: list,
    steps: list,
    material_flows: list,
) -> dict:
    return sanitize_payload(
        full_src,
        ALLOWED_MATERIAL_FIELDS,
        {
            "ProjectId": tgt_project_id,
            "ProcessId": tgt_process_id,
            "UnitOperations": uos,
            "Steps": steps,
            "MaterialFlows": material_flows,
        },
    )

def build_material_attribute_payload(
    full_src: dict,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_id: int | None,
    step_id: int | None,
    pc_id: int | None,
    mat_id: int | None,
    control_methods: list,
) -> dict:
    return sanitize_payload(
        full_src,
        ALLOWED_MATERIAL_ATTRIBUTE_FIELDS,
        {
            "ProjectId": tgt_project_id,
            "ProcessId": tgt_process_id,
            "UnitOperationId": uo_id,
            "StepId": step_id,
            "ProcessComponentId": pc_id,
            "MaterialId": mat_id,
            "ControlMethods": control_methods,
        },
    )

def build_process_parameter_payload(
    full_src: dict,
    tgt_project_id: int,
    tgt_process_id: int,
    tgt_uo_id: int | None,
    tgt_step_id: int | None,
    tgt_pc_id: int | None,
    tgt_mat_id: int | None,
) -> dict:
    extra = {
        "ProjectId": tgt_project_id,
        "ProcessId": tgt_process_id,
        "UnitOperationId": tgt_uo_id,
    }
    if tgt_step_id:
        extra["StepId"] = tgt_step_id
    if tgt_pc_id:
        extra["ProcessComponentId"] = tgt_pc_id
    if tgt_mat_id:
        extra["MaterialId"] = tgt_mat_id
    return sanitize_payload(full_src, ALLOWED_PROCESS_PARAMETER_FIELDS, extra)

def build_iqa_payload(
    full_src: dict,
    tgt_project_id: int,
    tgt_process_id: int,
    tgt_uo_id: int | None,
    tgt_step_id: int | None,
    control_methods: list,
) -> dict:
    return sanitize_payload(
        full_src,
        ALLOWED_IQA_FIELDS,
        {
            "ProjectId": tgt_project_id,
            "ProcessId": tgt_process_id,
            "UnitOperationId": tgt_uo_id,
            "StepId": tgt_step_id,
            "ControlMethods": control_methods,
        },
    )

def build_ipa_payload(
    full_src: dict,
    tgt_project_id: int,
    tgt_process_id: int,
    tgt_uo_id: int | None,
    tgt_step_id: int | None,
    control_methods: list,
) -> dict:
    return sanitize_payload(
        full_src,
        ALLOWED_IPA_FIELDS,
        {
            "ProjectId": tgt_project_id,
            "ProcessId": tgt_process_id,
            "UnitOperationId": tgt_uo_id,
            "StepId": tgt_step_id,
            "ControlMethods": control_methods,
        },
    )

def build_sample_payload(
    full_src: dict,
    tgt_project_id: int,
    tgt_process_id: int,
    tgt_uo_id: int | None,
    tgt_step_id: int | None,
    tgt_mat_id: int | None,
    tgt_matrix_mat_id: int | None,
    timepoints: list | None = None,
) -> dict:
    extra = {
        "ProjectId": tgt_project_id,
        "ProcessId": tgt_process_id,
        "UnitOperationId": tgt_uo_id,
        "StepId": tgt_step_id,
        "MaterialId": tgt_mat_id,
        "MatrixMaterialId": tgt_matrix_mat_id,
    }
    if timepoints is not None:
        extra["Timepoints"] = timepoints
    return sanitize_payload(full_src, ALLOWED_SAMPLE_FIELDS, extra)

def build_process_component_supplier_payload(
    tgt_id: int,
    name: str,
    component_type: str,
    tgt_project_id: int,
    tgt_process_id: int,
    tgt_supplier_id: int,
    last_version_id: int,
    steps: list,
    uos: list,
) -> dict:
    return {
        "id": tgt_id,
        "name": name,
        "type": component_type,
        "ProcessId": tgt_process_id,
        "ProjectId": tgt_project_id,
        "SupplierId": tgt_supplier_id,
        "LastVersionId": last_version_id,
        "Steps": steps,
        "UnitOperations": uos,
    }

def build_material_supplier_payload(
    tgt_id: int,
    name: str,
    category: str,
    material_use: str,
    tgt_process_id: int,
    tgt_supplier_id: int,
    last_version_id: int,
    steps: list,
    uos: list,
    flows: list,
) -> dict:
    return {
        "id": tgt_id,
        "name": name,
        "category": category,
        "use": material_use,
        "ProcessId": tgt_process_id,
        "SupplierId": tgt_supplier_id,
        "LastVersionId": last_version_id,
        "Steps": steps,
        "UnitOperations": uos,
        "MaterialFlows": flows,
    }

def attach_requirement_payload(payload: dict, requirement_payload: dict | None) -> None:
    if requirement_payload:
        payload.pop("AcceptanceCriteriaRanges", None)
        payload["Requirement"] = requirement_payload

def save_copy_payload(
    *,
    record_type: str,
    record_label: str,
    writer: SyncWriter,
    payload: dict,
    src_name: str,
    tgt_record: dict | None,
    tgt_id: int | None,
    changed_fields: list,
    requirement_payload: dict | None = None,
    source_id: int | None = None,
) -> int:
    if tgt_record:
        resolved_tgt_id = tgt_id or tgt_record["id"]
        if not changed_fields:
            logger.info("%s '%s' unchanged - skipping", record_label, src_name)
            return resolved_tgt_id

        payload["id"] = resolved_tgt_id
        payload["LastVersionId"] = tgt_record["LastVersionId"]
        attach_requirement_payload(payload, requirement_payload)
        logger.info(
            "Updating %s '%s' (id %s): changed fields: %s",
            record_label,
            src_name,
            resolved_tgt_id,
            changed_fields,
        )
        writer.save_record(record_type, payload, reason=f"update {record_label}")
        return resolved_tgt_id

    logger.info("Creating %s '%s'", record_label, src_name)
    attach_requirement_payload(payload, requirement_payload)
    new_record = writer.save_record(record_type, payload, reason=f"create {record_label}")
    new_id = new_record["id"]
    if source_id is not None:
        logger.info("Created %s '%s': %s -> %s", record_label, src_name, source_id, new_id)
    return new_id

# --------------------- TIMEPOINT HELPERS ---------------------

def coerce_list(value) -> list:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []
    return value if isinstance(value, list) else []

def active_timepoints(timepoints) -> list:
    return [
        tp for tp in coerce_list(timepoints)
        if isinstance(tp, dict) and not tp.get("deletedAt")
    ]

def timepoint_sort_key(timepoint: dict, *, include_id: bool = False) -> tuple:
    key = (
        timepoint.get("recordOrder") is None,
        str(timepoint.get("recordOrder")),
        str(timepoint.get("name") or ""),
    )
    if include_id:
        return key + (str(timepoint.get("id") or ""),)
    return key

def target_timepoint_lookups(timepoints) -> tuple[dict, dict]:
    active = active_timepoints(timepoints)
    return (
        unique_lookup(active, lambda tp: normalize(tp.get("name"))),
        unique_lookup(active, lambda tp: normalize(tp.get("recordOrder"))),
    )

def matching_target_timepoint(src_timepoint: dict, tgt_by_name: dict, tgt_by_order: dict) -> dict | None:
    return (
        tgt_by_name.get(normalize(src_timepoint.get("name")))
        or tgt_by_order.get(normalize(src_timepoint.get("recordOrder")))
    )

def normalize_timepoints_for_compare(timepoints) -> list:
    cleaned = [
        {field: normalize(tp.get(field)) for field in ALLOWED_TIMEPOINT_FIELDS}
        for tp in active_timepoints(timepoints)
    ]
    return sorted(
        cleaned,
        key=timepoint_sort_key,
    )

def unique_lookup(records: list, key_fn) -> dict:
    counts = Counter()
    by_key = {}
    for record in records:
        key = key_fn(record)
        if key is None:
            continue
        counts[key] += 1
        by_key[key] = record
    return {key: record for key, record in by_key.items() if counts[key] == 1}

def build_timepoints_payload(src_timepoints, tgt_timepoints, tgt_uo_id: int) -> list:
    tgt_by_name, tgt_by_order = target_timepoint_lookups(tgt_timepoints)

    payload = []
    for src_tp in active_timepoints(src_timepoints):
        item = {
            field: src_tp.get(field)
            for field in ALLOWED_TIMEPOINT_FIELDS
            if field in src_tp
        }
        item["UnitOperationId"] = tgt_uo_id

        tgt_tp = matching_target_timepoint(src_tp, tgt_by_name, tgt_by_order)
        if tgt_tp and tgt_tp.get("id"):
            item["id"] = tgt_tp["id"]

        payload.append(item)

    return sorted(
        payload,
        key=timepoint_sort_key,
    )

def _timepoint_label(timepoint: dict) -> str:
    label = timepoint.get("label")
    if label:
        return label
    return f"TP-{timepoint.get('id')} - {timepoint.get('name')}"

def build_sample_timepoints_payload(src_timepoints, tgt_uo_timepoints, tgt_uo_id: int) -> list:
    tgt_by_name, tgt_by_order = target_timepoint_lookups(tgt_uo_timepoints)

    payload = []
    for src_tp in active_timepoints(src_timepoints):
        tgt_tp = matching_target_timepoint(src_tp, tgt_by_name, tgt_by_order)
        if not tgt_tp or not tgt_tp.get("id"):
            logger.warning(
                "Skipping Sample Timepoint '%s'; no matching target Timepoint on UnitOperation %s",
                src_tp.get("name"),
                tgt_uo_id,
            )
            continue

        payload.append({
            "id": tgt_tp["id"],
            "name": tgt_tp.get("name"),
            "label": _timepoint_label(tgt_tp),
            "typeCode": tgt_tp.get("typeCode") or "TP",
            "unitOperationId": tgt_tp.get("unitOperationId") or tgt_tp.get("UnitOperationId") or tgt_uo_id,
            "recordOrder": tgt_tp.get("recordOrder"),
        })

    return sorted(
        payload,
        key=timepoint_sort_key,
    )

def normalize_sample_timepoints_for_compare(timepoints) -> list:
    cleaned = [
        {
            "id": normalize(tp.get("id")),
            "name": normalize(tp.get("name")),
            "recordOrder": normalize(tp.get("recordOrder")),
        }
        for tp in active_timepoints(timepoints)
    ]

    return sorted(
        cleaned,
        key=lambda tp: timepoint_sort_key(tp, include_id=True),
    )

# --------------------- SUPPLIER HELPERS ---------------------

def is_archived(record: dict) -> bool:
    return isinstance(record, dict) and record.get("currentState") == "Archived"

def get_target_supplier_by_name(client: QbdApiClient, name):
    data = client.list_records("Supplier")
    suppliers = data.get("instances") if isinstance(data, dict) else data
    if not isinstance(suppliers, list):
        return None

    for supplier in suppliers:
        if not isinstance(supplier, dict):
            continue
        if supplier.get("name") == name and supplier.get("currentState") != "Archived":
            return supplier.get("id")

    return None

def clean_supplier_payload(src_supplier: dict) -> dict:
    payload = {k: src_supplier[k] for k in ALLOWED_SUPPLIER_FIELDS if k in src_supplier}
    return strip_attachment_links(payload)

def create_target_supplier(writer: SyncWriter, src_supplier: dict) -> int:
    cleaned_payload = clean_supplier_payload(src_supplier)

    if not cleaned_payload.get("name"):
        raise ValueError("Supplier payload missing name")

    supplier = writer.save_record("Supplier", cleaned_payload, reason="create Supplier")

    return supplier["id"]

def resolve_target_supplier_id(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_supplier_id: int,
    supplier_cache: dict | None = None,
) -> int | None:
    if not src_supplier_id:
        return None

    supplier_cache = supplier_cache or {}
    by_id = supplier_cache.setdefault("by_id", {})
    by_name = supplier_cache.setdefault("by_name", {})

    cache_key = str(src_supplier_id)
    if cache_key in by_id:
        return by_id[cache_key]

    src_supplier = src_client.get_record("Supplier", src_supplier_id)
    if isinstance(src_supplier, dict) and src_supplier.get("currentState") == "Archived":
        logger.info(
            "Skipping archived Supplier '%s' (%s)",
            src_supplier.get("name"),
            src_supplier_id,
        )
        return None

    name = src_supplier.get("name") if isinstance(src_supplier, dict) else None
    if not name:
        logger.warning("Supplier id %s missing name; skipping remap", src_supplier_id)
        return None

    if name in by_name:
        tgt_id = by_name[name]
    else:
        tgt_id = get_target_supplier_by_name(tgt_client, name)
        if tgt_id:
            logger.info(
                "Mapped existing Supplier '%s': %s -> %s",
                name,
                src_supplier_id,
                tgt_id,
            )
        else:
            logger.info("Supplier '%s' not found in target; creating it", name)
            tgt_id = create_target_supplier(writer, src_supplier)
            logger.info(
                "Created new Supplier '%s': %s -> %s",
                name,
                src_supplier_id,
                tgt_id,
            )
        by_name[name] = tgt_id

    by_id[cache_key] = tgt_id
    return tgt_id

# --------------------- ID MAP PERSISTENCE ---------------------

ID_MAP_FILE = "process_id_map.json"

def load_id_map() -> dict:
    try:
        with open(ID_MAP_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"processes": {}}

    if isinstance(data, dict) and "processes" in data:
        return data

    return {"processes": {}}

def save_id_map(id_map: dict):
    with open(ID_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(id_map, f, indent=2)

# --------------------- CONTROL METHODS & RISK LINKS ---------------------

def map_and_diff_control_methods(
    src_cms: list,
    tgt_cms: list,
    cm_lookup: dict,
):
    """
    Maps source ControlMethods to target IDs by NAME and determines
    whether the ControlMethods have changed (by NAME).
    Returns: (mapped_payload, changed_bool)
    """
    src_names = {cm["name"] for cm in src_cms or []}
    tgt_names = {cm.get("name") for cm in tgt_cms or []}

    mapped_payload = []
    for name in src_names:
        tgt_id = cm_lookup.get(name)
        if tgt_id:
            mapped_payload.append({"id": tgt_id})

    return mapped_payload, src_names != tgt_names

def load_control_method_lookup(
    client: QbdApiClient,
    project_id: int,
    process_id: int | None = None,
) -> dict:
    params = {"processId": process_id} if process_id is not None else {}
    return {
        cm["name"]: cm["id"]
        for cm in client.list_records("ControlMethod", project_id, **params).get("instances", [])
        if cm.get("name") and cm.get("id")
    }

def changed_fields_with_control_methods(
    payload: dict,
    target: dict,
    fields: list,
    cm_changed: bool,
    *,
    skip: set | None = None,
) -> list:
    skip = set(skip or ())
    skip.add("ControlMethods")
    changed_fields = changed_fields_for(payload, target, fields, skip=skip)
    if cm_changed:
        changed_fields.append("ControlMethods")
    return changed_fields

def canonical_type_code(code: str) -> str:
    return "".join(ch for ch in str(code or "").upper() if ch.isalnum())

def normalize_applies_to_maps(applies_to_maps: dict | None) -> dict:
    aliases = {
        "MA": "MATERIALATTRIBUTE",
        "MATERIALATTRIBUTE": "MATERIALATTRIBUTE",
        "PP": "PROCESSPARAMETER",
        "PROCESSPARAMETER": "PROCESSPARAMETER",
        "PC": "PROCESSCOMPONENT",
        "PROCESSCOMPONENT": "PROCESSCOMPONENT",
        "UO": "UNITOPERATION",
        "UNITOPERATION": "UNITOPERATION",
        "STP": "STEP",
        "STEP": "STEP",
        "MAT": "MATERIAL",
        "MATERIAL": "MATERIAL",
        "IPA": "IPA",
        "IQA": "IQA",
        "SA": "SAMPLE",
        "SAMPLE": "SAMPLE",
        "FPA": "FPA",
        "FQA": "FQA",
    }

    normalized = {}
    for key, mapping in (applies_to_maps or {}).items():
        if not isinstance(mapping, dict):
            continue
        canonical = aliases.get(canonical_type_code(key), canonical_type_code(key))
        normalized[canonical] = mapping
    return normalized

def map_applies_to_ref(ref, applies_to_maps: dict) -> str | None:
    if not isinstance(ref, str):
        return None
    ref = ref.strip()
    if not ref:
        return None

    if "-" not in ref:
        return ref

    type_code, raw_id = ref.split("-", 1)
    aliases = {
        "MA": "MATERIALATTRIBUTE",
        "MATERIALATTRIBUTE": "MATERIALATTRIBUTE",
        "PP": "PROCESSPARAMETER",
        "PROCESSPARAMETER": "PROCESSPARAMETER",
        "PC": "PROCESSCOMPONENT",
        "PROCESSCOMPONENT": "PROCESSCOMPONENT",
        "UO": "UNITOPERATION",
        "UNITOPERATION": "UNITOPERATION",
        "STP": "STEP",
        "STEP": "STEP",
        "MT": "MATERIAL",
        "MATERIAL": "MATERIAL",
        "IPA": "IPA",
        "IQA": "IQA",
        "SA": "SAMPLE",
        "SAMPLE": "SAMPLE",
        "FPA": "FPA",
        "FQA": "FQA",
    }

    canonical = aliases.get(canonical_type_code(type_code), canonical_type_code(type_code))
    mapping = applies_to_maps.get(canonical)
    if not mapping:
        return ref

    mapped_id = map_lookup(mapping, raw_id)
    if mapped_id is None:
        return None

    return f"{type_code}-{mapped_id}"

def sanitize_risk_link_links(links_value, applies_to_maps: dict) -> str:
    links = links_value
    if isinstance(links, str):
        try:
            links = json.loads(links)
        except Exception:
            links = []
    elif links is None:
        links = []

    if not isinstance(links, list):
        links = []

    cleaned = []
    for item in links:
        if not isinstance(item, dict):
            continue
        if item.get("linkType") == "Attachment":
            continue

        out = dict(item)
        applies_to = out.get("appliesTo")
        if isinstance(applies_to, list):
            mapped = []
            seen = set()
            for ref in applies_to:
                mapped_ref = map_applies_to_ref(ref, applies_to_maps)
                if not mapped_ref or mapped_ref in seen:
                    continue
                mapped.append(mapped_ref)
                seen.add(mapped_ref)
            out["appliesTo"] = mapped

        cleaned.append(out)

    return json.dumps(cleaned)


def sync_risk_links(
    records,
    record_type,
    put_fn,
    link_fields,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    applies_to_maps=None,
):
    """
    Sync relationship links for IQAs, IPAs, MaterialAttributes, ProcessParameters, and Samples.

    records: {src_id: tgt_id}
    put_fn: function to PUT updated record
    link_fields: dict of {field_name: spec}
        spec may be either:
        - (resolver_fn, id_key_name)
        - {"resolver": resolver_fn, "id_key": "...", "parent_id_key": "..."}
    """
    normalized_applies_maps = normalize_applies_to_maps(applies_to_maps)

    for src_id, tgt_record_id in records.items():
        src_record = src_client.get_record(record_type, src_id)
        tgt_record = tgt_client.get_record(record_type, tgt_record_id)

        payload = tgt_record.copy()
        changed = False

        for field_name, spec in link_fields.items():
            if isinstance(spec, dict):
                resolver = spec["resolver"]
                id_key = spec["id_key"]
                parent_id_key = spec.get("parent_id_key")
            else:
                resolver, id_key = spec
                parent_id_key = None

            src_links = src_record.get(field_name, []) or []
            tgt_links = payload.get(field_name, []) or []

            tgt_ids = {l[id_key] for l in tgt_links if id_key in l}
            new_links = []

            for link in src_links:
                target_id = resolver(link)
                if not target_id:
                    continue
                if target_id not in tgt_ids:
                    new_entry = {
                        id_key: target_id,
                        "impact": link.get("impact", 1),
                        "criticality": link.get("criticality", 1),
                        "uncertainty": link.get("uncertainty", 1),
                        "effect": link.get("effect", "Adds"),
                        "justification": link.get("justification", ""),
                        "links": sanitize_risk_link_links(link.get("links", "[]"), normalized_applies_maps),
                    }

                    if parent_id_key:
                        new_entry[parent_id_key] = tgt_record_id
                    elif record_type == "MaterialAttribute":
                        new_entry["MaterialAttributeId"] = tgt_record_id
                    else:
                        new_entry[record_type + "Id"] = tgt_record_id

                    new_links.append(new_entry)

            if new_links:
                payload[field_name] = tgt_links + new_links
                changed = True

            src_by_target_id = {resolver(l): l for l in src_links if resolver(l)}
            for tgt_link in payload[field_name]:
                linked_target_id = tgt_link.get(id_key)
                src_link = src_by_target_id.get(linked_target_id)
                if not src_link:
                    continue

                for k, default in (
                    ("impact", 1),
                    ("criticality", 1),
                    ("uncertainty", 1),
                    ("effect", "Adds"),
                    ("justification", ""),
                ):
                    src_val = src_link.get(k, default)
                    if tgt_link.get(k, default) != src_val:
                        tgt_link[k] = src_val
                        changed = True

                src_links_json = sanitize_risk_link_links(src_link.get("links", "[]"), normalized_applies_maps)
                tgt_links_json = sanitize_risk_link_links(tgt_link.get("links", "[]"), normalized_applies_maps)
                if tgt_links_json != src_links_json:
                    tgt_link["links"] = src_links_json
                    changed = True

            src_target_ids = {resolver(l) for l in src_links if resolver(l)}
            filtered_links = [l for l in payload[field_name] if l[id_key] in src_target_ids]
            if len(filtered_links) != len(payload[field_name]):
                payload[field_name] = filtered_links
                changed = True

        if changed:
            logger.info("Updating risk links for %s '%s'", record_type, tgt_record["name"])
            put_fn(payload)
        else:
            logger.info("%s '%s' risk links unchanged - skipping", record_type, tgt_record["name"])


# --------------------- DRUG FLOWS & IQA LINKS ---------------------

def sync_drug_flows(
    records: dict,
    record_type: str,
    flow_field: str,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    tgt_project_id: int,
    src_process_id: int | None = None,
    tgt_process_id: int | None = None,
    uo_mapping: dict | None = None,
    step_mapping: dict | None = None,
):
    """
    Sync DrugSubstanceFlows / DrugProductFlows by matching names for
    Process, Step, and UnitOperation. When process/UO/Step mappings are
    provided, use them first so flows with null StepId or sparse nested
    relation objects are still copied correctly.
    """
    uo_mapping = uo_mapping or {}
    step_mapping = step_mapping or {}

    tgt_processes = tgt_client.list_records("Process", tgt_project_id).get("instances", [])
    tgt_steps = tgt_client.list_records("Step", tgt_project_id).get("instances", [])
    tgt_uos = tgt_client.list_records("UnitOperation", tgt_project_id).get("instances", [])

    tgt_process_by_name = {p["name"]: p["id"] for p in tgt_processes}
    tgt_step_by_name = {s["name"]: s["id"] for s in tgt_steps}
    tgt_uo_by_name = {u["name"]: u["id"] for u in tgt_uos}

    for src_id, tgt_id in records.items():
        src = src_client.get_record(record_type, src_id)
        tgt = tgt_client.get_record(record_type, tgt_id)

        src_flows = src.get(flow_field, []) or []
        tgt_flows = tgt.get(flow_field, []) or []

        def _nested_value(flow, key, value_key):
            obj = flow.get(key) or {}
            if isinstance(obj, dict):
                return obj.get(value_key)
            return None

        def _flow_key(process_id, step_id, uo_id):
            return (
                normalize_id(process_id),
                normalize_id(step_id),
                normalize_id(uo_id),
            )

        def _flow_process_id(flow):
            return flow.get("ProcessId") or _nested_value(flow, "Process", "id")

        def _flow_step_id(flow):
            return flow.get("StepId") or _nested_value(flow, "Step", "id")

        def _flow_uo_id(flow):
            return flow.get("UnitOperationId") or _nested_value(flow, "UnitOperation", "id")

        def _flow_in_target_scope(flow):
            if tgt_process_id is None:
                return True
            return normalize_id(_flow_process_id(flow)) == normalize_id(tgt_process_id)

        def resolve_ids(flow):
            src_flow_process_id = _flow_process_id(flow)
            src_flow_step_id = _flow_step_id(flow)
            src_flow_uo_id = _flow_uo_id(flow)

            if (
                src_process_id is not None
                and src_flow_process_id is not None
                and normalize_id(src_flow_process_id) != normalize_id(src_process_id)
            ):
                return None, None, None

            mapped_process_id = None
            if (
                src_process_id is not None
                and tgt_process_id is not None
                and normalize_id(src_flow_process_id) == normalize_id(src_process_id)
            ):
                mapped_process_id = tgt_process_id
            if mapped_process_id is None:
                mapped_process_id = tgt_process_by_name.get(_nested_value(flow, "Process", "name"))

            mapped_step_id = None
            if src_flow_step_id is not None:
                mapped_step_id = map_lookup(step_mapping, src_flow_step_id)
                if mapped_step_id is None:
                    mapped_step_id = tgt_step_by_name.get(_nested_value(flow, "Step", "name"))

            mapped_uo_id = None
            if src_flow_uo_id is not None:
                mapped_uo_id = map_lookup(uo_mapping, src_flow_uo_id)
                if mapped_uo_id is None:
                    mapped_uo_id = tgt_uo_by_name.get(_nested_value(flow, "UnitOperation", "name"))

            return mapped_process_id, mapped_step_id, mapped_uo_id

        tgt_by_key = {
            _flow_key(f.get("ProcessId"), f.get("StepId"), f.get("UnitOperationId")): f
            for f in tgt_flows
            if _flow_in_target_scope(f)
        }

        new_flows = []
        changed = False
        seen_keys = set()

        for src_flow in src_flows:
            mapped_process_id, mapped_step_id, mapped_uo_id = resolve_ids(src_flow)
            if not mapped_process_id:
                continue

            src_flow_step_id = _flow_step_id(src_flow)
            src_flow_uo_id = _flow_uo_id(src_flow)
            if (src_flow_step_id is not None and mapped_step_id is None) or (
                src_flow_uo_id is not None and mapped_uo_id is None
            ):
                logger.warning(
                    "Skipping %s flow for %s '%s'; missing target mapping for ProcessId=%s StepId=%s UnitOperationId=%s",
                    flow_field,
                    record_type,
                    tgt.get("name"),
                    _flow_process_id(src_flow),
                    src_flow_step_id,
                    src_flow_uo_id,
                )
                continue

            if mapped_step_id is None and mapped_uo_id is None:
                logger.warning(
                    "Skipping %s flow for %s '%s'; flow has no StepId or UnitOperationId",
                    flow_field,
                    record_type,
                    tgt.get("name"),
                )
                continue

            key = _flow_key(mapped_process_id, mapped_step_id, mapped_uo_id)
            seen_keys.add(key)

            tgt_flow = tgt_by_key.get(key)
            if not tgt_flow:
                new_flows.append({
                    "function": src_flow.get("function"),
                    "flow": src_flow.get("flow"),
                    "ProcessId": mapped_process_id,
                    "StepId": mapped_step_id,
                    "UnitOperationId": mapped_uo_id,
                    f"{record_type}Id": tgt_id,
                })
                changed = True
            else:
                if tgt_flow.get("function") != src_flow.get("function") or tgt_flow.get("flow") != src_flow.get("flow"):
                    tgt_flow["function"] = src_flow.get("function")
                    tgt_flow["flow"] = src_flow.get("flow")
                    changed = True

        filtered_flows = [
            f for f in tgt_flows
            if not _flow_in_target_scope(f)
            or _flow_key(f.get("ProcessId"), f.get("StepId"), f.get("UnitOperationId")) in seen_keys
        ]

        if len(filtered_flows) != len(tgt_flows):
            changed = True

        if changed:
            payload = tgt.copy()
            payload[flow_field] = filtered_flows + new_flows

            logger.info(
                "Updating %s flows for %s '%s'",
                flow_field,
                record_type,
                tgt.get("name"),
            )
            writer.save_record(record_type, payload, reason=f"sync {flow_field}")
        else:
            logger.info(
                "%s '%s' flows unchanged - skipping",
                record_type,
                tgt.get("name"),
            )

def sync_iqa_drug_links(
    iqa_mapping: dict,
    drug_substance_mapping: dict,
    drug_product_mapping: dict,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
):
    """
    Sync IQA links to DrugSubstance / DrugProduct.
    This must run AFTER DrugSubstance and DrugProduct flows are synced.
    """
    for src_iqa_id, tgt_iqa_id in iqa_mapping.items():
        src_iqa = get_scoped_record(src_client, "IQA", src_iqa_id, src_project_id, src_process_id, "source IQA")
        if not src_iqa:
            continue

        tgt_iqa = tgt_client.get_record("IQA", tgt_iqa_id)

        changed = False

        src_ds_id = src_iqa.get("DrugSubstanceId")
        if src_ds_id:
            tgt_ds_id = drug_substance_mapping.get(src_ds_id)
            if tgt_iqa.get("DrugSubstanceId") != tgt_ds_id:
                tgt_iqa["DrugSubstanceId"] = tgt_ds_id
                changed = True

        src_dp_id = src_iqa.get("DrugProductId")
        if src_dp_id:
            tgt_dp_id = drug_product_mapping.get(src_dp_id)
            if tgt_iqa.get("DrugProductId") != tgt_dp_id:
                tgt_iqa["DrugProductId"] = tgt_dp_id
                changed = True

        if changed:
            logger.info(
                "Updating IQA '%s' links to DrugSubstance/DrugProduct",
                tgt_iqa.get("name"),
            )
            writer.save_record("IQA", tgt_iqa, reason="sync IQA drug links")
        else:
            logger.info(
                "IQA '%s' links to DrugSubstance/DrugProduct unchanged - skipping",
                tgt_iqa.get("name"),
            )

# --------------------- COPY HELPERS ---------------------

def copy_unit_operations(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    prev_mapping: dict = None,
) -> dict:
    """
    copy Unit Operations from source to target process.
    Uses persisted prev_mapping to detect renamed UOs.
    Returns a dict: source UO ID -> target UO ID.
    """
    prev_mapping = prev_mapping or {}

    explorer = get_process_explorer(src_client, src_project_id, src_process_id)
    uo_map = explorer.get("uoMap", {})
    record_keys = convert_map_to_record_keys(uo_map)

    if not record_keys:
        logger.info("No Unit Operations found for source process %s", src_process_id)
        return {}

    src_uos = []
    for k in record_keys:
        src_uo = get_scoped_record(
            src_client,
            "UnitOperation",
            int(k.split("-")[1]),
            src_project_id,
            src_process_id,
            "source UnitOperation",
        )
        if not src_uo:
            continue
        src_uos.append(src_uo)
    src_uos.sort(key=lambda uo: uo.get("order") or 0)

    dup_uo_names = find_duplicate_keys(src_uos, lambda r: r.get("name"))
    log_duplicate_name_fallback("UnitOperation", dup_uo_names)

    tgt_instances = list_process_records(tgt_client, "UnitOperation", tgt_project_id, tgt_process_id)
    tgt_by_name = name_record_lookup([uo for uo in tgt_instances if uo.get("ProcessId") == tgt_process_id])

    mapping = {}

    for src_uo in src_uos:
        if is_archived(src_uo):
            continue
        src_id = src_uo["id"]
        src_name = src_uo.get("name")

        tgt_uo_id, tgt_uo = resolve_target_by_name(
            src_id=src_id,
            src_name=src_name,
            prev_mapping=prev_mapping,
            tgt_by_name=tgt_by_name,
            duplicate_names=dup_uo_names,
            record_type="UnitOperation",
            record_label="UnitOperation",
            tgt_client=tgt_client,
            tgt_project_id=tgt_project_id,
            tgt_process_id=tgt_process_id,
            fallback_when_mapped_invalid=True,
            fetch_fallback_full=False,
        )

        if tgt_uo:
            tgt_uo = ensure_full_record("UnitOperation", tgt_uo, tgt_client)
            if tgt_uo and "Timepoints" not in tgt_uo:
                tgt_uo = tgt_client.get_record("UnitOperation", tgt_uo["id"])

        sync_timepoints = "Timepoints" in src_uo
        timepoints_payload = None
        if sync_timepoints and tgt_uo_id:
            timepoints_payload = build_timepoints_payload(
                src_uo.get("Timepoints"),
                tgt_uo.get("Timepoints") if tgt_uo else [],
                tgt_uo_id,
            )
        payload = build_unit_operation_payload(
            src_uo,
            tgt_project_id,
            tgt_process_id,
            timepoints_payload,
        )

        changed_fields = []
        if tgt_uo:
            try:
                tgt_uo_for_diff = json.loads(json.dumps(tgt_uo))
            except Exception:
                tgt_uo_for_diff = dict(tgt_uo)
            tgt_uo_for_diff = strip_attachment_links(tgt_uo_for_diff)

            changed_fields = frozen_changed_fields_for(payload, tgt_uo_for_diff, ALLOWED_UNIT_OPERATION_FIELDS)
            if sync_timepoints:
                src_timepoints = normalize_timepoints_for_compare(payload.get("Timepoints"))
                tgt_timepoints = normalize_timepoints_for_compare(tgt_uo_for_diff.get("Timepoints"))
                if src_timepoints != tgt_timepoints:
                    changed_fields.append("Timepoints")
            if changed_fields == ["order"]:
                changed_fields = []

        if tgt_uo:
            if not changed_fields:
                logger.info("UnitOperation '%s' unchanged - skipping", src_name)
            else:
                logger.info("UnitOperation '%s' diff details:", src_name)
                for f in changed_fields:
                    logger.info(
                        "  - %s: src=%r tgt=%r",
                        f,
                        freeze_for_compare(payload.get(f)),
                        freeze_for_compare(tgt_uo_for_diff.get(f)),
                    )
                payload["id"] = tgt_uo_id
                payload["LastVersionId"] = tgt_uo["LastVersionId"]
                logger.info("Updating UnitOperation '%s' (id %s): changed fields: %s", src_name, tgt_uo_id, changed_fields)
                writer.save_record("UnitOperation", payload, reason="update UnitOperation")
        else:
            logger.info("Creating UnitOperation '%s'", src_name)
            new_uo = writer.save_record("UnitOperation", payload, reason="create UnitOperation")
            tgt_uo_id = new_uo["id"]
            logger.info("Created UnitOperation '%s': %s -> %s", src_name, src_id, tgt_uo_id)
            if sync_timepoints:
                new_uo_full = new_uo
                if not new_uo_full.get("LastVersionId"):
                    new_uo_full = tgt_client.get_record("UnitOperation", tgt_uo_id)
                timepoints_payload = build_timepoints_payload(
                    src_uo.get("Timepoints"),
                    new_uo_full.get("Timepoints", []),
                    tgt_uo_id,
                )
                if timepoints_payload:
                    timepoint_update = build_unit_operation_timepoints_update_payload(
                        src_uo,
                        tgt_uo_id,
                        new_uo_full["LastVersionId"],
                        tgt_project_id,
                        tgt_process_id,
                        timepoints_payload,
                    )
                    logger.info("Updating UnitOperation '%s' timepoints after create", src_name)
                    writer.save_record("UnitOperation", timepoint_update, reason="update UnitOperation timepoints")

        mapping[normalize_id(src_id)] = tgt_uo_id

    return mapping

def sync_unit_operation_order(
    src_process: dict,
    tgt_process: dict,
    uo_mapping: dict,
    put_process_fn,
):
    """
    uo_mapping: { source_unit_operation_id: target_unit_operation_id }
    put_process_fn: function that PUTs process payload
    """
    src_order_raw = src_process.get("unitOperationOrder")
    tgt_order_raw = tgt_process.get("unitOperationOrder")

    if not src_order_raw or not tgt_order_raw:
        return

    src_order = json.loads(src_order_raw)
    tgt_order = json.loads(tgt_order_raw)

    src_order_map = {
        entry["unitOperationId"]: entry["order"]
        for entry in src_order
    }

    changed = False

    for tgt_entry in tgt_order:
        for src_uo_id, tgt_uo_id in uo_mapping.items():
            if tgt_uo_id == tgt_entry["unitOperationId"]:
                src_order_val = map_lookup(src_order_map, src_uo_id)
                if src_order_val is not None and tgt_entry["order"] != src_order_val:
                    tgt_entry["order"] = src_order_val
                    changed = True
                break

    if not changed:
        logger.info(
            "Unit operation order unchanged for Process '%s' - skipping",
            tgt_process.get("name"),
        )
        return

    payload = build_unit_operation_order_payload(tgt_process, tgt_order)

    logger.info("Updating unit operation order on Process '%s'", tgt_process["name"])

    put_process_fn(payload)

def copy_steps(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_mapping: dict,
    prev_mapping: dict = None,
) -> dict:
    """
    copy Steps from source to target process.
    Returns dict: source Step ID -> target Step ID.
    """
    prev_mapping = prev_mapping or {}

    explorer = get_process_explorer(src_client, src_project_id, src_process_id)
    step_map = explorer.get("stpMap", {})
    record_keys = convert_map_to_record_keys(step_map)
    if not record_keys:
        logger.info("No steps found for source process %s", src_process_id)
        return {}

    src_steps = []
    for key in record_keys:
        step_id = int(key.split("-")[1])
        step = get_scoped_record(src_client, "Step", step_id, src_project_id, src_process_id, "source Step")
        if not step:
            continue
        src_steps.append(step)

    dup_step_keys = find_duplicate_keys(src_steps, lambda r: (r.get("UnitOperationId"), r.get("name")))
    if dup_step_keys:
        logger.info("Duplicate Step names in source for some UnitOperations; disabling name-based fallback for those pairs")

    steps_by_uo = {}
    for step in src_steps:
        uo_id = step.get("UnitOperationId")
        steps_by_uo.setdefault(uo_id, []).append(step)

    tgt_instances = list_process_records(tgt_client, "Step", tgt_project_id, tgt_process_id)

    tgt_by_uo_name = {}
    for s in tgt_instances:
        tgt_uo_id = s.get("UnitOperationId")
        tgt_by_uo_name.setdefault(tgt_uo_id, {})[s["name"]] = s
    tgt_lookup = {
        (uo_id, name): step
        for uo_id, steps_by_name in tgt_by_uo_name.items()
        for name, step in steps_by_name.items()
    }

    mapping = {}

    for src_uo_id, step_list in steps_by_uo.items():
        tgt_uo_id = map_lookup(uo_mapping, src_uo_id)
        if not tgt_uo_id:
            logger.warning("No target UnitOperation for source UO %s; skipping steps", src_uo_id)
            continue

        ordered_steps = []
        remaining = {s["id"]: s for s in step_list}
        while remaining:
            for s_id, s in list(remaining.items()):
                prev_id = s.get("PreviousStepId")
                if prev_id is None or prev_id not in remaining:
                    ordered_steps.append(s)
                    del remaining[s_id]

        for step in ordered_steps:
            src_step_id = step["id"]
            src_name = step["name"]

            fallback_key = None if (src_uo_id, src_name) in dup_step_keys else (tgt_uo_id, src_name)
            tgt_step_id, tgt_step = resolve_target_by_lookup(
                src_id=src_step_id,
                prev_mapping=prev_mapping,
                fallback_key=fallback_key,
                tgt_lookup=tgt_lookup,
                record_type="Step",
                record_label="Step",
                tgt_client=tgt_client,
                tgt_project_id=tgt_project_id,
                tgt_process_id=tgt_process_id,
            )

            if not tgt_step_id and (src_uo_id, src_name) in dup_step_keys:
                logger.info("Step '%s' has duplicate name in source UO %s; skipping name-based fallback", src_name, src_uo_id)

            if tgt_step:
                tgt_step = ensure_full_record("Step", tgt_step, tgt_client)

            full_src = active_source_full_record(
                src_client,
                "Step",
                step,
                src_project_id,
                src_process_id,
                "source Step",
            )
            if not full_src:
                continue
            payload = build_step_payload(full_src, tgt_project_id, tgt_process_id, tgt_uo_id)

            changed_fields = []
            if tgt_step:
                changed_fields = changed_fields_for(payload, tgt_step, ALLOWED_STEP_FIELDS)

            tgt_step_id = save_copy_payload(
                record_type="Step",
                record_label="Step",
                writer=writer,
                payload=payload,
                src_name=src_name,
                tgt_record=tgt_step,
                tgt_id=tgt_step_id,
                changed_fields=changed_fields,
                source_id=src_step_id,
            )

            mapping[src_step_id] = tgt_step_id

    return mapping

def sync_step_order(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    uo_mapping: dict,
    step_mapping: dict,
):
    """
    Sync stepOrder on target UnitOperations to match source ordering.
    """
    for src_uo_id, tgt_uo_id in uo_mapping.items():

        src_uo = src_client.get_record("UnitOperation", src_uo_id)
        tgt_uo = tgt_client.get_record("UnitOperation", tgt_uo_id)

        src_order_raw = src_uo.get("stepOrder")
        if not src_order_raw:
            continue

        src_order = json.loads(src_order_raw)

        new_order = []
        for entry in src_order:
            src_step_id = entry.get("stepId")
            tgt_step_id = map_lookup(step_mapping, src_step_id)

            if not tgt_step_id:
                continue

            tgt_step = tgt_client.get_record("Step", tgt_step_id)

            new_order.append({
                "uuid": entry.get("uuid"),
                "stepId": tgt_step_id,
                "stepVersionId": tgt_step["LastVersionId"],
                "order": entry["order"],
            })

        existing = json.loads(tgt_uo.get("stepOrder") or "[]")

        def normalize(order):
            return sorted(
                [(o["stepId"], o["order"]) for o in order],
                key=lambda x: x[1],
            )

        if normalize(existing) == normalize(new_order):
            logger.info(
                "Step order unchanged for UnitOperation %s - skipping",
                tgt_uo["name"],
            )
            continue

        payload = build_step_order_payload(tgt_uo, tgt_uo_id, new_order)

        logger.info(
            "Updating step order for UnitOperation '%s'",
            tgt_uo["name"],
        )

        writer.save_record("UnitOperation", payload, reason="sync step order")

def copy_process_components(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_mapping: dict,
    step_mapping: dict,
    prev_mapping: dict = None,
) -> dict:
    """
    copy Process Components from source to target process.
    Uses previous mapping to handle renamed components.
    Returns a dict mapping source component ID -> target component ID.
    """
    prev_mapping = prev_mapping or {}

    src_list = list_process_records(src_client, "ProcessComponent", src_project_id, src_process_id)
    if not src_list:
        logger.info("No Process Components found for source process %s", src_process_id)
        return {}

    dup_pc_names = find_duplicate_keys(src_list, lambda r: r.get("name"))
    log_duplicate_name_fallback("ProcessComponent", dup_pc_names)

    tgt_list = list_process_records(tgt_client, "ProcessComponent", tgt_project_id, tgt_process_id)
    tgt_by_name = name_record_lookup(tgt_list)

    mapping = {}

    for src_pc in src_list:
        src_id = src_pc["id"]
        src_name = src_pc["name"]

        tgt_pc_id, tgt_full = resolve_target_by_name(
            src_id=src_id,
            src_name=src_name,
            prev_mapping=prev_mapping,
            tgt_by_name=tgt_by_name,
            duplicate_names=dup_pc_names,
            record_type="ProcessComponent",
            record_label="ProcessComponent",
            tgt_client=tgt_client,
            tgt_project_id=tgt_project_id,
            tgt_process_id=tgt_process_id,
            fallback_when_mapped_invalid=True,
        )

        full_src = active_source_full_record(
            src_client,
            "ProcessComponent",
            src_pc,
            src_project_id,
            src_process_id,
            "source ProcessComponent",
        )
        if not full_src:
            continue

        uos = mapped_id_relationships(full_src.get("UnitOperations", []), uo_mapping, "UnitOperationId", "id")
        steps = mapped_id_relationships(full_src.get("Steps", []), step_mapping, "StepId", "id")

        payload = build_process_component_payload(
            full_src,
            tgt_project_id,
            tgt_process_id,
            uos,
            steps,
        )

        if tgt_full:
            if not payload.get("name"):
                payload["name"] = tgt_full.get("name")
            if not payload.get("type"):
                payload["type"] = tgt_full.get("type")

        changed_fields = []
        if tgt_full:
            tgt_full = ensure_full_record("ProcessComponent", tgt_full, tgt_client)
            tgt_full = add_tgt_acr_for_diff(tgt_full)
            changed_fields = changed_fields_for(
                payload,
                tgt_full,
                ALLOWED_PROCESS_COMPONENT_FIELDS,
                skip={"UnitOperations", "Steps"},
            )

            append_relationship_diffs(
                changed_fields,
                payload,
                tgt_full,
                ("UnitOperations", "Steps"),
                record_label="ProcessComponent",
                record_name=src_name,
            )

        tgt_pc_id = save_copy_payload(
            record_type="ProcessComponent",
            record_label="ProcessComponent",
            writer=writer,
            payload=payload,
            src_name=src_name,
            tgt_record=tgt_full,
            tgt_id=tgt_pc_id,
            changed_fields=changed_fields,
            source_id=src_id,
        )

        mapping[src_id] = tgt_pc_id

    return mapping

def copy_materials(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_mapping: dict,
    step_mapping: dict,
    prev_mapping: dict = None,
) -> dict:
    """
    copy Materials from source to target process.
    Uses persisted mapping to handle renames.
    Returns dict: source material ID -> target material ID.
    """
    prev_mapping = prev_mapping or {}

    src_materials = list_process_records(src_client, "Material", src_project_id, src_process_id)
    if not src_materials:
        logger.info("No Materials found for source process %s", src_process_id)
        return {}

    dup_material_names = find_duplicate_keys(src_materials, lambda r: r.get("name"))
    log_duplicate_name_fallback("Material", dup_material_names)

    tgt_materials = list_process_records(tgt_client, "Material", tgt_project_id, tgt_process_id)
    tgt_by_name = name_record_lookup(tgt_materials)

    mapping = {}
    seen_tgt_ids = set()

    for src_mat in src_materials:
        src_id = src_mat["id"]
        src_name = src_mat["name"]

        tgt_mat_id, tgt_full = resolve_target_by_name(
            src_id=src_id,
            src_name=src_name,
            prev_mapping=prev_mapping,
            tgt_by_name=tgt_by_name,
            duplicate_names=dup_material_names,
            record_type="Material",
            record_label="Material",
            tgt_client=tgt_client,
            tgt_project_id=tgt_project_id,
            tgt_process_id=tgt_process_id,
            fallback_when_mapped_invalid=True,
        )

        if tgt_mat_id in seen_tgt_ids:
            logger.warning("Material '%s' maps to target id %s more than once; skipping duplicate update", src_name, tgt_mat_id)
            mapping[src_id] = tgt_mat_id
            continue
        if tgt_mat_id:
            seen_tgt_ids.add(tgt_mat_id)

        full_src = active_source_full_record(
            src_client,
            "Material",
            src_mat,
            src_project_id,
            src_process_id,
            "source Material",
        )
        if not full_src:
            continue

        material_flows, uos, steps = build_material_flow_relationships(
            full_src.get("MaterialFlows", []),
            uo_mapping,
            step_mapping,
            tgt_process_id,
        )

        payload = build_material_payload(
            full_src,
            tgt_project_id,
            tgt_process_id,
            uos,
            steps,
            material_flows,
        )

        if tgt_full:
            if not payload.get("name"):
                payload["name"] = tgt_full.get("name")

        changed_fields = []

        if tgt_full:
            tgt_full = ensure_full_record("Material", tgt_full, tgt_client)
            try:
                tgt_full = json.loads(json.dumps(tgt_full))
            except Exception:
                tgt_full = dict(tgt_full)
            tgt_full = strip_attachment_links(tgt_full)
            tgt_full = add_tgt_acr_for_diff(tgt_full)
            changed_fields = changed_fields_for(payload, tgt_full, ALLOWED_MATERIAL_FIELDS)

            append_relationship_diffs(
                changed_fields,
                payload,
                tgt_full,
                ("UnitOperations", "Steps"),
                record_label="Material",
                record_name=src_name,
            )
            if tuple_set(material_flows, ("UnitOperationId", "StepId", "flow")) != tuple_set(
                tgt_full.get("MaterialFlows", []),
                ("UnitOperationId", "StepId", "flow"),
            ):
                changed_fields.append("MaterialFlows")

        tgt_mat_id = save_copy_payload(
            record_type="Material",
            record_label="Material",
            writer=writer,
            payload=payload,
            src_name=src_name,
            tgt_record=tgt_full,
            tgt_id=tgt_mat_id,
            changed_fields=changed_fields,
            source_id=src_id,
        )

        mapping[src_id] = tgt_mat_id

    return mapping

def copy_material_attributes(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_mapping: dict,
    step_mapping: dict,
    pc_mapping: dict,
    material_mapping: dict,
    prev_mapping: dict = None,
) -> dict:
    """
    copy Material Attributes from source to target process.
    Maps UnitOperations, Steps, ProcessComponents, and ControlMethods.
    Returns dict: source attribute ID -> target attribute ID.
    """
    prev_mapping = prev_mapping or {}

    src_attributes = list_process_records(src_client, "MaterialAttribute", src_project_id, src_process_id)
    if not src_attributes:
        logger.info("No Material Attributes found for source process %s", src_process_id)
        return {}


    dup_attr_names = find_duplicate_keys(src_attributes, lambda r: r.get("name"))
    log_duplicate_name_fallback("Material Attribute", dup_attr_names)

    tgt_attributes = list_process_records(tgt_client, "MaterialAttribute", tgt_project_id, tgt_process_id)
    tgt_by_name = name_record_lookup(tgt_attributes)

    cm_lookup = load_control_method_lookup(tgt_client, tgt_project_id, tgt_process_id)

    mapping = {}

    for src_attr in src_attributes:
        src_id = src_attr["id"]
        src_name = src_attr["name"]

        tgt_attr_id, tgt_full = resolve_target_by_name(
            src_id=src_id,
            src_name=src_name,
            prev_mapping=prev_mapping,
            tgt_by_name=tgt_by_name,
            duplicate_names=dup_attr_names,
            record_type="MaterialAttribute",
            record_label="Material Attribute",
            tgt_client=tgt_client,
            tgt_project_id=tgt_project_id,
            tgt_process_id=tgt_process_id,
            fallback_when_mapped_invalid=True,
        )

        full_src = active_source_full_record(
            src_client,
            "MaterialAttribute",
            src_attr,
            src_project_id,
            src_process_id,
            "source MaterialAttribute",
        )
        if not full_src:
            continue

        uo_id = map_lookup(uo_mapping, full_src.get("UnitOperationId"))
        step_id = map_lookup(step_mapping, full_src.get("StepId"))

        pc_id = map_lookup(pc_mapping, full_src.get("ProcessComponentId"))
        mat_id = map_lookup(material_mapping, full_src.get("MaterialId"))

        tgt_cms_mapped, cm_changed = map_and_diff_control_methods(
            full_src.get("ControlMethods", []),
            tgt_full.get("ControlMethods", []) if tgt_full else [],
            cm_lookup,
        )

        payload = build_material_attribute_payload(
            full_src,
            tgt_project_id,
            tgt_process_id,
            uo_id,
            step_id,
            pc_id,
            mat_id,
            tgt_cms_mapped,
        )
        requirement_payload = add_acr_to_payload(full_src, payload)

        changed_fields = []
        if tgt_full:
            tgt_full = ensure_full_record("MaterialAttribute", tgt_full, tgt_client)
            tgt_full = add_tgt_acr_for_diff(tgt_full)
            changed_fields = changed_fields_with_control_methods(
                payload,
                tgt_full,
                ALLOWED_MATERIAL_ATTRIBUTE_FIELDS,
                cm_changed,
                skip={"ControlMethods", "AcceptanceCriteriaRanges"},
            )
            if param_changed(payload.get("AcceptanceCriteriaRanges"), tgt_full.get("AcceptanceCriteriaRanges")):
                src_acr = requirement_payload.get("AcceptanceCriteriaRanges") if requirement_payload else []
                tgt_full_acr = add_tgt_acr_for_diff(tgt_full)
                tgt_acr = tgt_full_acr.get("AcceptanceCriteriaRanges") if isinstance(tgt_full_acr, dict) else None
                logger.info(
                    "Material Attribute '%s' ACR diff: src=%s tgt=%s",
                    src_name, src_acr, tgt_acr,
                )
                logger.info(
                    "Material Attribute '%s' ACR raw: Requirement=%r, AcceptanceCriteriaRanges=%r, AcceptanceCriteriaRangeLinkedVersions=%r",
                    src_name,
                    full_src.get("Requirement"),
                    full_src.get("AcceptanceCriteriaRanges"),
                    full_src.get("AcceptanceCriteriaRangeLinkedVersions"),
                )
                changed_fields.append("AcceptanceCriteriaRanges")

        tgt_attr_id = save_copy_payload(
            record_type="MaterialAttribute",
            record_label="Material Attribute",
            writer=writer,
            payload=payload,
            src_name=src_name,
            tgt_record=tgt_full,
            tgt_id=tgt_attr_id,
            changed_fields=changed_fields,
            requirement_payload=requirement_payload,
            source_id=src_id,
        )

        mapping[src_id] = tgt_attr_id

    return mapping

def copy_process_parameters(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_mapping: dict,
    step_mapping: dict,
    process_component_mapping: dict,
    material_mapping: dict,
    prev_mapping: dict = None,
) -> dict:
    """
    copy Process Parameters (PPs) from source to target process.
    Uses persisted mapping to handle renames.
    Returns dict: source PP ID -> target PP ID.
    """
    prev_mapping = prev_mapping or {}

    src_params = list_process_records(src_client, "ProcessParameter", src_project_id, src_process_id)
    if not src_params:
        logger.info("No Process Parameters found for source process %s", src_process_id)
        return {}

    tgt_instances = list_process_records(tgt_client, "ProcessParameter", tgt_project_id, tgt_process_id)

    tgt_lookup = {
        (p.get("name"), p.get("UnitOperationId"), p.get("StepId"), p.get("ProcessComponentId"), p.get("MaterialId")): p
        for p in tgt_instances
    }

    mapping = {}

    for src_pp in src_params:
        src_id = src_pp["id"]

        tgt_uo_id = map_lookup(uo_mapping, src_pp.get("UnitOperationId"))
        tgt_step_id = map_lookup(step_mapping, src_pp.get("StepId")) if src_pp.get("StepId") else None
        tgt_pc_id = map_lookup(process_component_mapping, src_pp.get("ProcessComponentId")) if src_pp.get("ProcessComponentId") else None
        tgt_mat_id = map_lookup(material_mapping, src_pp.get("MaterialId")) if src_pp.get("MaterialId") else None

        tgt_pp_id, tgt_pp_stub = resolve_target_by_lookup(
            src_id=src_id,
            prev_mapping=prev_mapping,
            fallback_key=(src_pp["name"], tgt_uo_id, tgt_step_id, tgt_pc_id, tgt_mat_id),
            tgt_lookup=tgt_lookup,
            record_type="ProcessParameter",
            record_label="ProcessParameter",
            tgt_client=tgt_client,
            tgt_project_id=tgt_project_id,
            tgt_process_id=tgt_process_id,
        )

        full_src = active_source_full_record(
            src_client,
            "ProcessParameter",
            src_pp,
            src_project_id,
            src_process_id,
            "source ProcessParameter",
        )
        if not full_src:
            continue

        payload = build_process_parameter_payload(
            full_src,
            tgt_project_id,
            tgt_process_id,
            tgt_uo_id,
            tgt_step_id,
            tgt_pc_id,
            tgt_mat_id,
        )
        requirement_payload = add_acr_to_payload(full_src, payload)

        changed_fields = []
        if tgt_pp_stub:
            tgt_pp_stub = ensure_full_record("ProcessParameter", tgt_pp_stub, tgt_client)
            tgt_pp_stub = add_tgt_acr_for_diff(tgt_pp_stub)

            fields_to_check = ALLOWED_PROCESS_PARAMETER_FIELDS + ["UnitOperationId", "StepId", "ProcessComponentId", "MaterialId"]

            changed_fields = changed_fields_for(payload, tgt_pp_stub, fields_to_check)

        mapping[src_id] = save_copy_payload(
            record_type="ProcessParameter",
            record_label="ProcessParameter",
            writer=writer,
            payload=payload,
            src_name=src_pp["name"],
            tgt_record=tgt_pp_stub,
            tgt_id=tgt_pp_id,
            changed_fields=changed_fields,
            requirement_payload=requirement_payload,
            source_id=src_id,
        )

    return mapping

def copy_iqas(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_mapping: dict,
    step_mapping: dict,
    prev_mapping: dict = None,
) -> dict:
    """
    copy IQAs from source to target process.
    ControlMethods are matched and diffed by NAME (not ID).
    Returns dict: source IQA ID -> target IQA ID.
    """

    prev_mapping = prev_mapping or {}

    src_iqas = list_process_records(src_client, "IQA", src_project_id, src_process_id)
    if not src_iqas:
        logger.info("No IQAs found for source process %s", src_process_id)
        return {}


    dup_iqa_names = find_duplicate_keys(src_iqas, lambda r: r.get("name"))
    log_duplicate_name_fallback("IQA", dup_iqa_names)

    tgt_iqas = list_process_records(tgt_client, "IQA", tgt_project_id, tgt_process_id)
    tgt_by_name = name_record_lookup(tgt_iqas)

    cm_lookup = load_control_method_lookup(tgt_client, tgt_project_id)

    mapping = {}

    for src_stub in src_iqas:
        src_id = src_stub["id"]
        src_name = src_stub["name"]

        tgt_id, tgt_full = resolve_target_by_name(
            src_id=src_id,
            src_name=src_name,
            prev_mapping=prev_mapping,
            tgt_by_name=tgt_by_name,
            duplicate_names=dup_iqa_names,
            record_type="IQA",
            record_label="IQA",
            tgt_client=tgt_client,
            tgt_project_id=tgt_project_id,
            tgt_process_id=tgt_process_id,
            fallback_when_mapped_invalid=True,
        )

        full_src = active_source_full_record(src_client, "IQA", src_stub, src_project_id, src_process_id, "source IQA")
        if not full_src:
            continue

        tgt_uo_id = map_lookup(uo_mapping, full_src.get("UnitOperationId"))
        tgt_step_id = map_lookup(step_mapping, full_src.get("StepId")) if full_src.get("StepId") else None

        tgt_cms_payload, cm_changed = map_and_diff_control_methods(
            full_src.get("ControlMethods", []),
            tgt_full.get("ControlMethods", []) if tgt_full else [],
            cm_lookup,
        )

        payload = build_iqa_payload(
            full_src,
            tgt_project_id,
            tgt_process_id,
            tgt_uo_id,
            tgt_step_id,
            tgt_cms_payload,
        )
        requirement_payload = add_acr_to_payload(full_src, payload)

        changed_fields = []

        if tgt_full:
            tgt_full = ensure_full_record("IQA", tgt_full, tgt_client)
            tgt_full = add_tgt_acr_for_diff(tgt_full)
            changed_fields = changed_fields_with_control_methods(
                payload,
                tgt_full,
                ALLOWED_IQA_FIELDS,
                cm_changed,
            )

        tgt_id = save_copy_payload(
            record_type="IQA",
            record_label="IQA",
            writer=writer,
            payload=payload,
            src_name=src_name,
            tgt_record=tgt_full,
            tgt_id=tgt_id,
            changed_fields=changed_fields,
            requirement_payload=requirement_payload,
            source_id=src_id,
        )

        mapping[src_id] = tgt_id

    return mapping

def copy_ipas(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_mapping: dict,
    step_mapping: dict,
    prev_mapping: dict = None,
) -> dict:

    prev_mapping = prev_mapping or {}

    src_ipas = list_process_records(src_client, "IPA", src_project_id, src_process_id)
    if not src_ipas:
        return {}

    tgt_ipas = list_process_records(tgt_client, "IPA", tgt_project_id, tgt_process_id)

    tgt_lookup = {}
    for ipa in tgt_ipas:
        key = (ipa.get("name"), ipa.get("UnitOperationId"), ipa.get("StepId"))
        tgt_lookup[key] = ipa

    cm_lookup = load_control_method_lookup(tgt_client, tgt_project_id)

    mapping = {}

    for src_ipa in src_ipas:
        src_id = src_ipa["id"]

        tgt_uo_id = map_lookup(uo_mapping, src_ipa.get("UnitOperationId"))
        tgt_step_id = map_lookup(step_mapping, src_ipa.get("StepId")) if src_ipa.get("StepId") else None

        tgt_ipa_id, tgt_stub = resolve_target_by_lookup(
            src_id=src_id,
            prev_mapping=prev_mapping,
            fallback_key=(src_ipa["name"], tgt_uo_id, tgt_step_id),
            tgt_lookup=tgt_lookup,
            record_type="IPA",
            record_label="IPA",
            tgt_client=tgt_client,
            tgt_project_id=tgt_project_id,
            tgt_process_id=tgt_process_id,
        )

        full_src = active_source_full_record(src_client, "IPA", src_ipa, src_project_id, src_process_id, "source IPA")
        if not full_src:
            continue

        tgt_cms_payload, cm_changed = map_and_diff_control_methods(
            full_src.get("ControlMethods", []),
            tgt_stub.get("ControlMethods", []) if tgt_stub else [],
            cm_lookup,
        )

        payload = build_ipa_payload(
            full_src,
            tgt_project_id,
            tgt_process_id,
            tgt_uo_id,
            tgt_step_id,
            tgt_cms_payload,
        )
        requirement_payload = add_acr_to_payload(full_src, payload)

        changed_fields = []

        if tgt_stub:
            tgt_stub = ensure_full_record("IPA", tgt_stub, tgt_client)
            tgt_stub = add_tgt_acr_for_diff(tgt_stub)

            fields_to_check = ALLOWED_IPA_FIELDS + ["UnitOperationId", "StepId"]

            changed_fields = changed_fields_with_control_methods(payload, tgt_stub, fields_to_check, cm_changed)

        mapping[src_id] = save_copy_payload(
            record_type="IPA",
            record_label="IPA",
            writer=writer,
            payload=payload,
            src_name=src_ipa["name"],
            tgt_record=tgt_stub,
            tgt_id=tgt_ipa_id,
            changed_fields=changed_fields,
            requirement_payload=requirement_payload,
            source_id=src_id,
        )

    return mapping

def copy_samples(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_mapping: dict,
    step_mapping: dict,
    material_mapping: dict,
    prev_mapping: dict = None,
) -> dict:
    prev_mapping = prev_mapping or {}

    src_samples = list_process_records(src_client, "Sample", src_project_id, src_process_id)
    if not src_samples:
        logger.info("No Samples found for source process %s", src_process_id)
        return {}

    tgt_samples = list_process_records(tgt_client, "Sample", tgt_project_id, tgt_process_id)

    tgt_lookup = {}
    for sample in tgt_samples:
        key = (
            sample.get("name"),
            sample.get("type"),
            sample.get("UnitOperationId"),
            sample.get("StepId"),
            sample.get("MaterialId"),
            sample.get("MatrixMaterialId"),
        )
        tgt_lookup[key] = sample

    mapping = {}
    tgt_uo_cache = {}

    def get_tgt_uo_timepoints(tgt_uo_id: int | None) -> list:
        if not tgt_uo_id:
            return []
        if tgt_uo_id not in tgt_uo_cache:
            tgt_uo = tgt_client.get_record("UnitOperation", tgt_uo_id)
            tgt_uo = validate_target_scope(tgt_uo, tgt_project_id, tgt_process_id, "UnitOperation")
            tgt_uo_cache[tgt_uo_id] = tgt_uo or {}
        return tgt_uo_cache[tgt_uo_id].get("Timepoints", [])

    for src_sample in src_samples:
        src_id = src_sample["id"]

        tgt_uo_id = map_lookup(uo_mapping, src_sample.get("UnitOperationId")) if src_sample.get("UnitOperationId") else None
        tgt_step_id = map_lookup(step_mapping, src_sample.get("StepId")) if src_sample.get("StepId") else None
        tgt_mat_id = map_lookup(material_mapping, src_sample.get("MaterialId")) if src_sample.get("MaterialId") else None
        tgt_matrix_mat_id = map_lookup(material_mapping, src_sample.get("MatrixMaterialId")) if src_sample.get("MatrixMaterialId") else None

        tgt_sample_id, tgt_stub = resolve_target_by_lookup(
            src_id=src_id,
            prev_mapping=prev_mapping,
            fallback_key=(
                src_sample.get("name"),
                src_sample.get("type"),
                tgt_uo_id,
                tgt_step_id,
                tgt_mat_id,
                tgt_matrix_mat_id,
            ),
            tgt_lookup=tgt_lookup,
            record_type="Sample",
            record_label="Sample",
            tgt_client=tgt_client,
            tgt_project_id=tgt_project_id,
            tgt_process_id=tgt_process_id,
        )

        full_src = active_source_full_record(
            src_client,
            "Sample",
            src_sample,
            src_project_id,
            src_process_id,
            "source Sample",
        )
        if not full_src:
            continue

        tgt_uo_id = map_lookup(uo_mapping, full_src.get("UnitOperationId")) if full_src.get("UnitOperationId") else None
        tgt_step_id = map_lookup(step_mapping, full_src.get("StepId")) if full_src.get("StepId") else None
        tgt_mat_id = map_lookup(material_mapping, full_src.get("MaterialId")) if full_src.get("MaterialId") else None
        tgt_matrix_mat_id = map_lookup(material_mapping, full_src.get("MatrixMaterialId")) if full_src.get("MatrixMaterialId") else None

        sync_timepoints = "Timepoints" in full_src
        timepoints_payload = None
        if sync_timepoints:
            if tgt_uo_id:
                timepoints_payload = build_sample_timepoints_payload(
                    full_src.get("Timepoints"),
                    get_tgt_uo_timepoints(tgt_uo_id),
                    tgt_uo_id,
                )
            else:
                logger.warning(
                    "Skipping Sample '%s' timepoints; sample has no target UnitOperation",
                    full_src.get("name"),
                )

        payload = build_sample_payload(
            full_src,
            tgt_project_id,
            tgt_process_id,
            tgt_uo_id,
            tgt_step_id,
            tgt_mat_id,
            tgt_matrix_mat_id,
            timepoints_payload,
        )

        changed_fields = []
        if tgt_stub:
            tgt_stub = ensure_full_record("Sample", tgt_stub, tgt_client)
            if sync_timepoints and tgt_stub and "Timepoints" not in tgt_stub:
                tgt_stub = tgt_client.get_record("Sample", tgt_stub["id"])
            changed_fields = changed_fields_for(payload, tgt_stub, ALLOWED_SAMPLE_FIELDS)
            if sync_timepoints:
                src_timepoints = normalize_sample_timepoints_for_compare(payload.get("Timepoints"))
                tgt_timepoints = normalize_sample_timepoints_for_compare(tgt_stub.get("Timepoints"))
                if src_timepoints != tgt_timepoints:
                    changed_fields.append("Timepoints")

        mapping[src_id] = save_copy_payload(
            record_type="Sample",
            record_label="Sample",
            writer=writer,
            payload=payload,
            src_name=full_src.get("name"),
            tgt_record=tgt_stub,
            tgt_id=tgt_sample_id,
            changed_fields=changed_fields,
            source_id=src_id,
        )

    return mapping

# --------------------- SUPPLIER SYNC ---------------------

def list_or_none(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return None
    return value if isinstance(value, list) else None

def required_list_fields(record: dict, field_names: tuple[str, ...]) -> tuple[dict | None, str | None]:
    values = {}
    for field_name in field_names:
        parsed = list_or_none(record.get(field_name))
        if parsed is None:
            return None, "missing " + "/".join(field_names)
        values[field_name] = parsed
    return values, None

def merged_field(src_full: dict, tgt_full: dict, field_name: str):
    return tgt_full.get(field_name) or src_full.get(field_name)

def build_process_component_supplier_update(
    src_full: dict,
    tgt_full: dict,
    tgt_supplier_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
) -> tuple[dict | None, str | None, str | None]:
    lists, skip_reason = required_list_fields(tgt_full, ("Steps", "UnitOperations"))
    if skip_reason:
        return None, tgt_full.get("name"), skip_reason

    name_val = merged_field(src_full, tgt_full, "name")
    type_val = merged_field(src_full, tgt_full, "type")
    if not name_val or not type_val:
        return None, tgt_full.get("name"), "missing name/type"

    return (
        build_process_component_supplier_payload(
            tgt_full["id"],
            name_val,
            type_val,
            tgt_project_id,
            tgt_process_id,
            tgt_supplier_id,
            tgt_full.get("LastVersionId"),
            lists["Steps"],
            lists["UnitOperations"],
        ),
        name_val,
        None,
    )

def build_material_supplier_update(
    src_full: dict,
    tgt_full: dict,
    tgt_supplier_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
) -> tuple[dict | None, str | None, str | None]:
    lists, skip_reason = required_list_fields(tgt_full, ("Steps", "UnitOperations", "MaterialFlows"))
    if skip_reason:
        return None, tgt_full.get("name"), skip_reason

    name_val = merged_field(src_full, tgt_full, "name")
    category_val = merged_field(src_full, tgt_full, "category")
    use_val = merged_field(src_full, tgt_full, "use")
    if not name_val or category_val is None or use_val is None:
        return None, tgt_full.get("name"), "missing name/category/use"

    return (
        build_material_supplier_payload(
            tgt_full["id"],
            name_val,
            category_val,
            use_val,
            tgt_process_id,
            tgt_supplier_id,
            tgt_full.get("LastVersionId"),
            lists["Steps"],
            lists["UnitOperations"],
            lists["MaterialFlows"],
        ),
        name_val,
        None,
    )

def sync_supplier_id_records(
    *,
    record_type: str,
    records: dict,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    supplier_cache: dict,
    build_update_payload,
) -> dict:
    stats = {"updated": 0, "unchanged": 0, "skipped": 0}
    if records:
        logger.info("Syncing SupplierId for %s %s records", len(records), record_type)
    else:
        logger.info("No %s records mapped for SupplierId sync", record_type)

    for src_id, tgt_id in (records or {}).items():
        src_full = src_client.get_record(record_type, src_id)
        src_full = validate_target_scope(src_full, src_project_id, src_process_id, f"source {record_type}")
        if not src_full:
            stats["skipped"] += 1
            logger.info("Skipping %s source id %s; out of scope or missing", record_type, src_id)
            continue
        if is_archived(src_full):
            stats["skipped"] += 1
            logger.info("Skipping archived %s '%s' (%s)", record_type, src_full.get("name"), src_id)
            continue

        src_supplier_id = src_full.get("SupplierId")
        if not src_supplier_id:
            stats["skipped"] += 1
            logger.info("%s '%s' (%s) has no source SupplierId - skipping", record_type, src_full.get("name"), src_id)
            continue

        tgt_supplier_id = resolve_target_supplier_id(
            src_client,
            tgt_client,
            writer,
            src_supplier_id,
            supplier_cache,
        )
        if not tgt_supplier_id:
            stats["skipped"] += 1
            logger.warning(
                "Skipping %s '%s' (%s); unable to map source SupplierId %s",
                record_type,
                src_full.get("name"),
                src_id,
                src_supplier_id,
            )
            continue

        tgt_full = tgt_client.get_record(record_type, tgt_id)
        tgt_full = validate_target_scope(tgt_full, tgt_project_id, tgt_process_id, record_type)
        if not tgt_full:
            stats["skipped"] += 1
            logger.info("Skipping target %s id %s; out of scope or missing", record_type, tgt_id)
            continue
        if is_archived(tgt_full):
            stats["skipped"] += 1
            logger.info("Skipping archived target %s '%s' (%s)", record_type, tgt_full.get("name"), tgt_id)
            continue

        tgt_full = ensure_full_record(record_type, tgt_full, tgt_client)
        if tgt_full.get("SupplierId") == tgt_supplier_id:
            stats["unchanged"] += 1
            logger.info(
                "%s '%s' (%s) SupplierId unchanged (%s) - skipping",
                record_type,
                tgt_full.get("name"),
                tgt_id,
                tgt_supplier_id,
            )
            continue

        payload, name_val, skip_reason = build_update_payload(
            src_full,
            tgt_full,
            tgt_supplier_id,
            tgt_project_id,
            tgt_process_id,
        )
        if not payload:
            stats["skipped"] += 1
            logger.warning(
                "Skipping %s '%s' (%s) supplier update; %s",
                record_type,
                name_val,
                tgt_id,
                skip_reason,
            )
            continue

        logger.info("Updating %s '%s' (%s) SupplierId -> %s", record_type, name_val, tgt_id, tgt_supplier_id)
        writer.save_record(record_type, payload, reason=f"sync {record_type} SupplierId")
        stats["updated"] += 1

    return stats

def sync_supplier_ids(
    *,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    pc_mapping: dict,
    material_mapping: dict,
):
    supplier_cache = {"by_id": {}, "by_name": {}}
    sync_supplier_id_records(
        record_type="ProcessComponent",
        records=pc_mapping,
        src_client=src_client,
        tgt_client=tgt_client,
        writer=writer,
        src_project_id=src_project_id,
        src_process_id=src_process_id,
        tgt_project_id=tgt_project_id,
        tgt_process_id=tgt_process_id,
        supplier_cache=supplier_cache,
        build_update_payload=build_process_component_supplier_update,
    )
    sync_supplier_id_records(
        record_type="Material",
        records=material_mapping,
        src_client=src_client,
        tgt_client=tgt_client,
        writer=writer,
        src_project_id=src_project_id,
        src_process_id=src_process_id,
        tgt_project_id=tgt_project_id,
        tgt_process_id=tgt_process_id,
        supplier_cache=supplier_cache,
        build_update_payload=build_material_supplier_update,
    )

# --------------------- MAIN COPY FLOW ---------------------

def copy_process_record(config: SyncConfig, proc_entry: dict, writer: SyncWriter) -> tuple[dict, int | None]:
    src_project_id = config.src_project_id
    src_process_id = config.src_process_id
    tgt_project_id = config.tgt_project_id
    src_client = config.src_client
    tgt_client = config.tgt_client

    src_process = src_client.get_record("Process", src_process_id)
    if is_archived(src_process):
        logger.info("Source process %s is archived; skipping copy", src_process_id)
        return src_process, None

    if src_process.get("ProjectId") != src_project_id:
        raise ValueError(f"Source process {src_process_id} does not belong to project {src_project_id}")

    payload = build_process_payload(src_process, tgt_project_id)

    tgt_process_id = proc_entry.get("targetProcessId")
    tgt_process_obj = None
    if tgt_process_id:
        tgt_process_obj = tgt_client.get_record("Process", tgt_process_id)
    else:
        tgt_processes = tgt_client.list_records("Process", tgt_project_id).get("instances", [])
        tgt_process_obj = next((p for p in tgt_processes if p.get("name") == payload["name"]), None)
        if tgt_process_obj:
            tgt_process_id = tgt_process_obj["id"]
            tgt_process_obj = tgt_client.get_record("Process", tgt_process_id)

    if tgt_process_obj:
        log_path = setup_logging(src_process_id, tgt_process_id)
        logger.info("Log file: %s", log_path)

        changed_fields = changed_fields_for(payload, tgt_process_obj, ALLOWED_PROCESS_FIELDS)
        if changed_fields:
            recheck = [f for f in changed_fields if param_changed(payload.get(f), tgt_process_obj.get(f))]
            if not recheck:
                logger.info("Process '%s' unchanged - skipping update", payload["name"])
                changed_fields = []
            else:
                for f in recheck:
                    logger.info("Process diff %s: src=%r tgt=%r", f, normalize(payload.get(f)), normalize(tgt_process_obj.get(f)))
                payload["id"] = tgt_process_id
                payload["LastVersionId"] = tgt_process_obj["LastVersionId"]
                logger.info("Updating Process '%s' (id %s): changed fields: %s", payload["name"], tgt_process_id, changed_fields)
                writer.save_record("Process", payload, reason="update Process")
        else:
            logger.info("Process '%s' unchanged - skipping update", payload["name"])
    else:
        log_path = setup_logging(src_process_id, tgt_process_id)
        logger.info("Log file: %s", log_path)

        logger.info("Creating Process '%s'", payload["name"])
        new_proc = writer.save_record("Process", payload, reason="create Process")
        tgt_process_id = new_proc["id"]
        logger.info("Created Process '%s': %s -> %s", payload["name"], src_process_id, tgt_process_id)

    proc_entry["targetProcessId"] = tgt_process_id
    return src_process, tgt_process_id

def copy_core_entities(
    config: SyncConfig,
    writer: SyncWriter,
    proc_entry: dict,
    src_process: dict,
    tgt_process_id: int,
) -> dict:
    src_project_id = config.src_project_id
    src_process_id = config.src_process_id
    tgt_project_id = config.tgt_project_id
    src_client = config.src_client
    tgt_client = config.tgt_client

    uo_mapping = copy_unit_operations(
        src_client, tgt_client, writer,
        src_project_id, src_process_id, tgt_project_id, tgt_process_id,
        prev_mapping=proc_entry.get("unitOperations", {}),
    )
    proc_entry["unitOperations"] = {str(k): v for k, v in uo_mapping.items()}

    tgt_process = tgt_client.get_record("Process", tgt_process_id)
    sync_unit_operation_order(
        src_process=src_process,
        tgt_process=tgt_process,
        uo_mapping=uo_mapping,
        put_process_fn=writer.save_fn("Process", reason="sync unit operation order"),
    )

    step_mapping = copy_steps(
        src_client, tgt_client, writer,
        src_project_id, src_process_id, tgt_project_id, tgt_process_id,
        uo_mapping,
        prev_mapping=proc_entry.get("steps", {}),
    )
    proc_entry["steps"] = {str(k): v for k, v in step_mapping.items()}

    sync_step_order(src_client, tgt_client, writer, uo_mapping, step_mapping)

    pc_mapping = copy_process_components(
        src_client, tgt_client, writer,
        src_project_id, src_process_id, tgt_project_id, tgt_process_id,
        uo_mapping, step_mapping,
        prev_mapping=proc_entry.get("processComponents", {}),
    )
    proc_entry["processComponents"] = {str(k): v for k, v in pc_mapping.items()}

    material_mapping = copy_materials(
        src_client, tgt_client, writer,
        src_project_id, src_process_id,
        tgt_project_id, tgt_process_id,
        uo_mapping, step_mapping,
        prev_mapping=proc_entry.get("materials", {}),
    )
    proc_entry["materials"] = {str(k): v for k, v in material_mapping.items()}

    material_attribute_mapping = copy_material_attributes(
        src_client, tgt_client, writer,
        src_project_id, src_process_id,
        tgt_project_id, tgt_process_id,
        uo_mapping, step_mapping, pc_mapping, material_mapping,
        prev_mapping=proc_entry.get("material_attributes", {})
    )
    proc_entry["material_attributes"] = {str(k): v for k, v in material_attribute_mapping.items()}

    pp_mapping = copy_process_parameters(
        src_client, tgt_client, writer,
        src_project_id, src_process_id, tgt_project_id, tgt_process_id,
        uo_mapping, step_mapping, pc_mapping, material_mapping,
        prev_mapping=proc_entry.get("processParameters", {}),
    )
    proc_entry["processParameters"] = {str(k): v for k, v in pp_mapping.items()}

    iqa_mapping = copy_iqas(
        src_client, tgt_client, writer,
        src_project_id, src_process_id,
        tgt_project_id, tgt_process_id,
        uo_mapping, step_mapping,
        prev_mapping=proc_entry.get("iqas", {}),
    )
    proc_entry["iqas"] = {str(k): v for k, v in iqa_mapping.items()}

    ipa_mapping = copy_ipas(
        src_client, tgt_client, writer,
        src_project_id, src_process_id,
        tgt_project_id, tgt_process_id,
        uo_mapping, step_mapping,
        prev_mapping=proc_entry.get("ipas", {}),
    )
    proc_entry["ipas"] = {str(k): v for k, v in ipa_mapping.items()}

    sample_mapping = copy_samples(
        src_client, tgt_client, writer,
        src_project_id, src_process_id,
        tgt_project_id, tgt_process_id,
        uo_mapping, step_mapping, material_mapping,
        prev_mapping=proc_entry.get("samples", {}),
    )
    proc_entry["samples"] = {str(k): v for k, v in sample_mapping.items()}

    return {
        "UnitOperation": uo_mapping,
        "Step": step_mapping,
        "ProcessComponent": pc_mapping,
        "Material": material_mapping,
        "MaterialAttribute": material_attribute_mapping,
        "ProcessParameter": pp_mapping,
        "IQA": iqa_mapping,
        "IPA": ipa_mapping,
        "Sample": sample_mapping,
    }

def sync_relationship_links(config: SyncConfig, writer: SyncWriter, mappings: dict):
    tgt_project_id = config.tgt_project_id
    src_client = config.src_client
    tgt_client = config.tgt_client

    tgt_fpa_by_name = build_target_lookup(tgt_client, tgt_project_id, "FPA")
    tgt_fqa_by_name = build_target_lookup(tgt_client, tgt_project_id, "FQA")

    sync_risk_links(
        records=mappings["IQA"],
        record_type="IQA",
        put_fn=writer.save_fn("IQA", reason="sync IQA risk links"),
        link_fields={
            "IQAToFPAs": (lambda link: tgt_fpa_by_name.get(link.get("FPA", {}).get("name")), "FPAId"),
            "IQAToFQAs": (lambda link: tgt_fqa_by_name.get(link.get("FQA", {}).get("name")), "FQAId"),
            "IQAToIPAs": (lambda link: mappings["IPA"].get(link.get("IPAId")), "IPAId"),
            "IQAToIQAs": {
                "resolver": lambda link: mappings["IQA"].get(link.get("TargetIQAId")),
                "id_key": "TargetIQAId",
                "parent_id_key": "IQAId",
            },
        },
        src_client=src_client,
        tgt_client=tgt_client,
        applies_to_maps=mappings,
    )

    sync_risk_links(
        records=mappings["IPA"],
        record_type="IPA",
        put_fn=writer.save_fn("IPA", reason="sync IPA risk links"),
        link_fields={
            "IPAToFPAs": (lambda link: tgt_fpa_by_name.get(link.get("FPA", {}).get("name")), "FPAId"),
            "IPAToFQAs": (lambda link: tgt_fqa_by_name.get(link.get("FQA", {}).get("name")), "FQAId"),
            "IPAToIPAs": {
                "resolver": lambda link: mappings["IPA"].get(link.get("TargetIPAId")),
                "id_key": "TargetIPAId",
                "parent_id_key": "IPAId",
            },
            "IPAToIQAs": (lambda link: mappings["IQA"].get(link.get("IQAId")), "IQAId"),
        },
        src_client=src_client,
        tgt_client=tgt_client,
        applies_to_maps=mappings,
    )

    sync_risk_links(
        records=mappings["MaterialAttribute"],
        record_type="MaterialAttribute",
        put_fn=writer.save_fn("MaterialAttribute", reason="sync MaterialAttribute risk links"),
        link_fields={
            "MaterialAttributeToFPAs": (lambda link: tgt_fpa_by_name.get(link.get("FPA", {}).get("name")), "FPAId"),
            "MaterialAttributeToFQAs": (lambda link: tgt_fqa_by_name.get(link.get("FQA", {}).get("name")), "FQAId"),
            "MaterialAttributeToIPAs": (lambda link: mappings["IPA"].get(link.get("IPAId")), "IPAId"),
            "MaterialAttributeToIQAs": (lambda link: mappings["IQA"].get(link.get("IQAId")), "IQAId"),
        },
        src_client=src_client,
        tgt_client=tgt_client,
        applies_to_maps=mappings,
    )

    sync_risk_links(
        records=mappings["ProcessParameter"],
        record_type="ProcessParameter",
        src_client=src_client,
        tgt_client=tgt_client,
        put_fn=writer.save_fn("ProcessParameter", reason="sync ProcessParameter risk links"),
        link_fields={
            "ProcessParameterToFPAs": (lambda link: tgt_fpa_by_name.get(link.get("FPA", {}).get("name")), "FPAId"),
            "ProcessParameterToFQAs": (lambda link: tgt_fqa_by_name.get(link.get("FQA", {}).get("name")), "FQAId"),
            "ProcessParameterToIPAs": (lambda link: mappings["IPA"].get(link.get("IPAId")), "IPAId"),
            "ProcessParameterToIQAs": (lambda link: mappings["IQA"].get(link.get("IQAId")), "IQAId"),
        },
        applies_to_maps=mappings,
    )

    sync_risk_links(
        records=mappings["Sample"],
        record_type="Sample",
        src_client=src_client,
        tgt_client=tgt_client,
        put_fn=writer.save_fn("Sample", reason="sync Sample relationship links"),
        link_fields={
            "SampleToIQAs": (lambda link: mappings["IQA"].get(link.get("IQAId")), "IQAId"),
            "SampleToMaterialAttributes": (
                lambda link: mappings["MaterialAttribute"].get(link.get("MaterialAttributeId")),
                "MaterialAttributeId",
            ),
            "SampleToProcessParameters": (
                lambda link: mappings["ProcessParameter"].get(link.get("ProcessParameterId")),
                "ProcessParameterId",
            ),
            "SampleToIPAs": (lambda link: mappings["IPA"].get(link.get("IPAId")), "IPAId"),
        },
        applies_to_maps=mappings,
    )

def sync_drug_mappings(config: SyncConfig, writer: SyncWriter, mappings: dict, tgt_process_id: int):
    src_project_id = config.src_project_id
    src_process_id = config.src_process_id
    tgt_project_id = config.tgt_project_id
    src_client = config.src_client
    tgt_client = config.tgt_client

    drug_substance_mapping = build_name_based_id_mapping(
        src_client,
        tgt_client,
        "DrugSubstance",
        src_project_id,
        tgt_project_id,
    )
    drug_product_mapping = build_name_based_id_mapping(
        src_client,
        tgt_client,
        "DrugProduct",
        src_project_id,
        tgt_project_id,
    )

    sync_drug_flows(
        records=drug_substance_mapping,
        record_type="DrugSubstance",
        flow_field="DrugSubstanceFlows",
        src_client=src_client,
        tgt_client=tgt_client,
        writer=writer,
        tgt_project_id=tgt_project_id,
        src_process_id=src_process_id,
        tgt_process_id=tgt_process_id,
        uo_mapping=mappings["UnitOperation"],
        step_mapping=mappings["Step"],
    )

    sync_drug_flows(
        records=drug_product_mapping,
        record_type="DrugProduct",
        flow_field="DrugProductFlows",
        src_client=src_client,
        tgt_client=tgt_client,
        writer=writer,
        tgt_project_id=tgt_project_id,
        src_process_id=src_process_id,
        tgt_process_id=tgt_process_id,
        uo_mapping=mappings["UnitOperation"],
        step_mapping=mappings["Step"],
    )
    sync_iqa_drug_links(
        iqa_mapping=mappings["IQA"],
        drug_substance_mapping=drug_substance_mapping,
        drug_product_mapping=drug_product_mapping,
        src_client=src_client,
        tgt_client=tgt_client,
        writer=writer,
        src_project_id=src_project_id,
        src_process_id=src_process_id,
    )

def sync_supplier_mappings(config: SyncConfig, writer: SyncWriter, mappings: dict, tgt_process_id: int):
    sync_supplier_ids(
        src_client=config.src_client,
        tgt_client=config.tgt_client,
        writer=writer,
        src_project_id=config.src_project_id,
        src_process_id=config.src_process_id,
        tgt_project_id=config.tgt_project_id,
        tgt_process_id=tgt_process_id,
        pc_mapping=mappings["ProcessComponent"],
        material_mapping=mappings["Material"],
    )

def copy_process(config: SyncConfig):
    src_process_id = config.src_process_id
    writer = SyncWriter(config.tgt_client)
    id_map = load_id_map()
    process_map = id_map.setdefault("processes", {})
    proc_entry = process_map.setdefault(str(src_process_id), {})

    try:
        src_process, tgt_process_id = copy_process_record(config, proc_entry, writer)
        if tgt_process_id is None:
            return

        mappings = copy_core_entities(config, writer, proc_entry, src_process, tgt_process_id)
        sync_relationship_links(config, writer, mappings)
        sync_drug_mappings(config, writer, mappings, tgt_process_id)
        sync_supplier_mappings(config, writer, mappings, tgt_process_id)
    finally:
        save_id_map(id_map)

# --------------------- ENTRYPOINT ---------------------

def main():
    try:
        config = load_config()
        copy_process(config)
    except requests.HTTPError as e:
        logger.error("HTTP error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.error("Response body: %s", e.response.text)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)

if __name__ == "__main__":
    main()
