"""
Process copy tool.
Copies a single Process and its Unit Operations, Steps, Process Params, Materials, Material Attributes, Samples, IPAs, IQAs, and Process Components
Also handles DS / DP flows
from a source project into an existing target project.

We do NOT sync the supplier list between environments. SupplierId for Materials and Process Components is remapped
by supplier name (create if missing). These are synced at the very end, after records are created - to preserve
unit ops, steps, and flows.
"""

import requests
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, List
from collections import Counter
from dotenv import load_dotenv
# --------------------- CONFIG ---------------------
# Load environment variables
load_dotenv()

SRC_PROJECT_ID = os.getenv("SOURCE_PROJECT_ID")
SRC_PROCESS_ID = os.getenv("SOURCE_PROCESS_ID")
TGT_PROJECT_ID = os.getenv("TARGET_PROJECT_ID")
SRC_KEY = os.getenv("SOURCE_KEY")
TGT_KEY = os.getenv("TARGET_KEY")
SRC_HOST = os.getenv("SOURCE_HOST")
SRC_BASE_PATH = os.getenv("SOURCE_BASE_PATH")
TGT_HOST = os.getenv("TARGET_HOST")
TGT_BASE_PATH = os.getenv("TARGET_BASE_PATH")

def _required_int(name: str, value: str | None) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        print(f"Error: {name} must be a whole number.")
        sys.exit(1)

def validate_config():
    global SRC_PROJECT_ID, SRC_PROCESS_ID, TGT_PROJECT_ID

    missing = []
    if not SRC_PROJECT_ID:
        missing.append("SOURCE_PROJECT_ID")
    if not SRC_PROCESS_ID:
        missing.append("SOURCE_PROCESS_ID")
    if not TGT_PROJECT_ID:
        missing.append("TARGET_PROJECT_ID")
    if not SRC_KEY:
        missing.append("SOURCE_KEY")
    if not TGT_KEY:
        missing.append("TARGET_KEY")
    if not SRC_HOST:
        missing.append("SOURCE_HOST")
    if not SRC_BASE_PATH:
        missing.append("SOURCE_BASE_PATH")
    if not TGT_HOST:
        missing.append("TARGET_HOST")
    if not TGT_BASE_PATH:
        missing.append("TARGET_BASE_PATH")

    if missing:
        print(f"Error: Missing required environment values: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in all required values.")
        sys.exit(1)

    SRC_PROJECT_ID = _required_int("SOURCE_PROJECT_ID", SRC_PROJECT_ID)
    SRC_PROCESS_ID = _required_int("SOURCE_PROCESS_ID", SRC_PROCESS_ID)
    TGT_PROJECT_ID = _required_int("TARGET_PROJECT_ID", TGT_PROJECT_ID)
# --------------------- LOGGING ---------------------
LOG_DIR = "logs"

def setup_logging(tgt_process_id: int | None = None) -> str:
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if tgt_process_id is None:
        tgt_process_id = "unknown"
    log_path = os.path.join(
        LOG_DIR,
        f"copy_process_src{SRC_PROCESS_ID}_tgt{tgt_process_id}_{timestamp}.log",
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

LOG_PATH = None
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
# AcceptanceCriteriaRanges
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
# --------------------- HTTP UTILITIES ---------------------
def make_base_url(host: str, base_path: str) -> str:
    """
    Constructs the hosted base URL for API requests using the provided host and
    base path.

    Args:
        host (str): The hostname for the API.
        base_path (str): The base path for the API.

    Returns:
        str: The constructed base URL.
    """
    host = host.strip().rstrip("/")
    base_path = base_path.strip().strip("/")
    if not host or not base_path:
        raise ValueError("Host and base path are required to build the API URL.")
    return f"https://{host}/{base_path}/"

def headers(api_key: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "qbdvision-api-key": api_key,
    }

def http_get(url: str, api_key: str) -> Any:
    r = requests.get(url, headers=headers(api_key))
    r.raise_for_status()
    return r.json() if r.text else {}

def http_put(url: str, api_key: str, payload: Dict[str, Any]) -> Any:
    r = requests.put(url, headers=headers(api_key), json=payload)
    r.raise_for_status()
    return r.json() if r.text else {}

def individual_record_url(base: str, record_type: str, record_id: int) -> str:
    return f"{base}editables/{record_type}/{record_id}?approved=false"
# --------------------- RECORD & PAYLOAD HELPERS ---------------------
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

# validate the record we are looking at is in the project / process we are looking at. 
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

# ensure we have the full record and arent trying to sync on partial records from lists 
def ensure_full_record(record_type: str, record: dict | None, base: str, api_key: str) -> dict | None:
    if not record or not isinstance(record, dict):
        return record

    required = REQUIRED_FIELDS_BY_TYPE.get(record_type)
    if required:
        for f in required:
            if f not in record or record.get(f) is None:
                rec_id = record.get("id")
                if not rec_id:
                    return record
                return http_get(individual_record_url(base, record_type, rec_id), api_key)

    if "LastVersionId" in record:
        return record

    rec_id = record.get("id")
    if not rec_id:
        return record

    return http_get(individual_record_url(base, record_type, rec_id), api_key)
# --------------------- PROCESS EXPLORER ---------------------
def get_process_explorer(
    base_url: str,
    api_key: str,
    project_id: int,
    process_id: int,
) -> dict:
    url = f"{base_url}processExplorer/{project_id}"
    params = {"processId": process_id}
    r = requests.get(url, headers=headers(api_key), params=params)
    r.raise_for_status()
    return r.json()
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

# account for duplicate names
def find_duplicate_keys(records, key_fn):
    counts = Counter()
    for r in records or []:
        key = key_fn(r)
        if key is None:
            continue
        counts[key] += 1
    return {k for k, c in counts.items() if c > 1}

def convert_map_to_record_keys(map_data: dict) -> List[str]:
    if not map_data:
        return []
    return [
        f"{obj['typeCode']}-{obj['id']}"
        for obj in map_data.values()
        if not obj.get("deletedAt") and obj.get("typeCode") and obj.get("id")
    ]

def build_target_lookup(
    base_url,
    api_key,
    project_id,
    record_type,
    *,
    return_full=False,
):
    url = f"{base_url}editables/{record_type}/list/{project_id}"
    resp = requests.get(url, headers=headers(api_key))
    resp.raise_for_status()
    instances = resp.json().get("instances", [])

    if return_full:
        return {r["name"]: r for r in instances if r.get("name")}
    else:
        return {r["name"]: r["id"] for r in instances if r.get("name")}
# --------------------- ACCEPTANCE CRITERIA ---------------------
def normalize_acr_value(val):
    if val in ("", None):
        return None
    if isinstance(val, str):
        try:
            obj = json.loads(val)
            if obj in ({}, []):
                return None
            if isinstance(obj, (list, dict)):
                return obj
        except Exception:
            pass
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

    src_ranges = None

    req = full_src.get("Requirement")
    if isinstance(req, str):
        try:
            req = json.loads(req)
        except Exception:
            pass
    if isinstance(req, dict):
        acr = req.get("AcceptanceCriteriaRanges")
        if isinstance(acr, str):
            try:
                acr = json.loads(acr)
            except Exception:
                pass
        if isinstance(acr, list):
            src_ranges = acr
        else:
            acr_alt = req.get("AcceptanceCriteriaRangeLinkedVersions")
            if isinstance(acr_alt, str):
                try:
                    acr_alt = json.loads(acr_alt)
                except Exception:
                    pass
            if isinstance(acr_alt, list):
                src_ranges = acr_alt

    if src_ranges is None:
        acr = full_src.get("AcceptanceCriteriaRanges")
        if isinstance(acr, str):
            try:
                acr = json.loads(acr)
            except Exception:
                pass
        if isinstance(acr, list):
            src_ranges = acr
        else:
            acr_alt = full_src.get("AcceptanceCriteriaRangeLinkedVersions")
            if isinstance(acr_alt, str):
                try:
                    acr_alt = json.loads(acr_alt)
                except Exception:
                    pass
            if isinstance(acr_alt, list):
                src_ranges = acr_alt

    if src_ranges is None:
        rv = full_src.get("RequirementVersion")
        if isinstance(rv, str):
            try:
                rv = json.loads(rv)
            except Exception:
                pass
        if isinstance(rv, dict):
            rv_acr = rv.get("AcceptanceCriteriaRanges")
            if isinstance(rv_acr, str):
                try:
                    rv_acr = json.loads(rv_acr)
                except Exception:
                    pass
            if isinstance(rv_acr, list):
                src_ranges = rv_acr
            else:
                rv_acr_alt = rv.get("AcceptanceCriteriaRangeLinkedVersions")
                if isinstance(rv_acr_alt, str):
                    try:
                        rv_acr_alt = json.loads(rv_acr_alt)
                    except Exception:
                        pass
                if isinstance(rv_acr_alt, list):
                    src_ranges = rv_acr_alt

    if not src_ranges:
        return None

    src_ranges = normalize_acceptance_criteria_ranges_list(src_ranges)
    payload["AcceptanceCriteriaRanges"] = src_ranges
    return {"AcceptanceCriteriaRanges": src_ranges}

def add_tgt_acr_for_diff(tgt_full: dict) -> dict:
    if not isinstance(tgt_full, dict):
        return tgt_full

    tgt_req = tgt_full.get("Requirement")
    if isinstance(tgt_req, str):
        try:
            tgt_req = json.loads(tgt_req)
        except Exception:
            pass
    if isinstance(tgt_req, dict):
        tgt_ranges = tgt_req.get("AcceptanceCriteriaRanges")
        if isinstance(tgt_ranges, str):
            try:
                tgt_ranges = json.loads(tgt_ranges)
            except Exception:
                pass
        if isinstance(tgt_ranges, list):
            copy = dict(tgt_full)
            copy["AcceptanceCriteriaRanges"] = normalize_acceptance_criteria_ranges_list(tgt_ranges)
            return copy

    tgt_ranges = tgt_full.get("AcceptanceCriteriaRanges")
    if isinstance(tgt_ranges, str):
        try:
            tgt_ranges = json.loads(tgt_ranges)
        except Exception:
            pass
    if isinstance(tgt_ranges, list):
        copy = dict(tgt_full)
        copy["AcceptanceCriteriaRanges"] = normalize_acceptance_criteria_ranges_list(tgt_ranges)
        return copy

    return tgt_full
# --------------------- NORMALIZATION & DIFF ---------------------
def _normalize_whitespace(text: str):
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
                return _normalize_whitespace(obj)
        except Exception:
            pass
        return _normalize_whitespace(val)

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

# --------------------- PAYLOAD & ARCHIVE HELPERS ---------------------
def sanitize_payload(src: dict, allowed_fields: list, extra_fields: dict) -> dict:
    payload = {k: src[k] for k in allowed_fields if k in src}
    payload.update(extra_fields)
    return strip_attachment_links(payload)

def is_archived(record: dict) -> bool:
    return isinstance(record, dict) and record.get("currentState") == "Archived"
# --------------------- SUPPLIER HELPERS ---------------------

def get_target_supplier_by_name(base_url, api_key, name):
    url = f"{base_url}editables/Supplier/list"
    data = http_get(url, api_key)

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

def create_target_supplier(tgt_base, tgt_key, src_supplier: dict) -> int:
    cleaned_payload = clean_supplier_payload(src_supplier)

    if not cleaned_payload.get("name"):
        raise ValueError("Supplier payload missing name")

    supplier = http_put(
        f"{tgt_base}editables/Supplier/addOrEdit",
        tgt_key,
        cleaned_payload
    )

    return supplier["id"]

def resolve_target_supplier_id(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    src_supplier = http_get(individual_record_url(src_base, "Supplier", src_supplier_id), src_key)
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
        tgt_id = get_target_supplier_by_name(tgt_base, tgt_key, name)
        if tgt_id:
            logger.info(
                "Mapped existing Supplier '%s': %s -> %s",
                name,
                src_supplier_id,
                tgt_id,
            )
        else:
            logger.info("Supplier '%s' not found in target; creating it", name)
            tgt_id = create_target_supplier(tgt_base, tgt_key, src_supplier)
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

def _canonical_type_code(code: str) -> str:
    return "".join(ch for ch in str(code or "").upper() if ch.isalnum())

def _normalize_applies_to_maps(applies_to_maps: dict | None) -> dict:
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
        canonical = aliases.get(_canonical_type_code(key), _canonical_type_code(key))
        normalized[canonical] = mapping
    return normalized

def _map_applies_to_ref(ref, applies_to_maps: dict) -> str | None:
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

    canonical = aliases.get(_canonical_type_code(type_code), _canonical_type_code(type_code))
    mapping = applies_to_maps.get(canonical)
    if not mapping:
        return ref

    mapped_id = map_lookup(mapping, raw_id)
    if mapped_id is None:
        return None

    return f"{type_code}-{mapped_id}"

def _sanitize_risk_link_links(links_value, applies_to_maps: dict) -> str:
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
                mapped_ref = _map_applies_to_ref(ref, applies_to_maps)
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
        src_key,
        tgt_key,
        src_base,
        tgt_base,
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
    normalized_applies_maps = _normalize_applies_to_maps(applies_to_maps)

    for src_id, tgt_record_id in records.items():
        src_record = http_get(individual_record_url(src_base, record_type, src_id), src_key)
        tgt_record = http_get(individual_record_url(tgt_base, record_type, tgt_record_id), tgt_key)

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
                        "links": _sanitize_risk_link_links(link.get("links", "[]"), normalized_applies_maps),
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

            # Update existing links' fields to match source
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

                src_links_json = _sanitize_risk_link_links(src_link.get("links", "[]"), normalized_applies_maps)
                tgt_links_json = _sanitize_risk_link_links(tgt_link.get("links", "[]"), normalized_applies_maps)
                if tgt_links_json != src_links_json:
                    tgt_link["links"] = src_links_json
                    changed = True

            # Remove links no longer present in source
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
    records: dict,                  # {src_ds_id: tgt_ds_id} or {src_dp_id: tgt_dp_id}
    record_type: str,               # "DrugSubstance" or "DrugProduct"
    flow_field: str,                # "DrugSubstanceFlows" or "DrugProductFlows"
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    # Fetch all processes, steps, and unit operations for the target project
    tgt_processes = http_get(f"{tgt_base}editables/Process/list/{tgt_project_id}", tgt_key).get("instances", [])
    tgt_steps = http_get(f"{tgt_base}editables/Step/list/{tgt_project_id}", tgt_key).get("instances", [])
    tgt_uos = http_get(f"{tgt_base}editables/UnitOperation/list/{tgt_project_id}", tgt_key).get("instances", [])

    tgt_process_by_name = {p["name"]: p["id"] for p in tgt_processes}
    tgt_step_by_name = {s["name"]: s["id"] for s in tgt_steps}
    tgt_uo_by_name = {u["name"]: u["id"] for u in tgt_uos}

    for src_id, tgt_id in records.items():
        src = http_get(individual_record_url(src_base, record_type, src_id), src_key)
        tgt = http_get(individual_record_url(tgt_base, record_type, tgt_id), tgt_key)

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

        # Build lookup for existing target flows by resolved IDs
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
                # Add new flow
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
                # Update if function/flow changed
                if tgt_flow.get("function") != src_flow.get("function") or tgt_flow.get("flow") != src_flow.get("flow"):
                    tgt_flow["function"] = src_flow.get("function")
                    tgt_flow["flow"] = src_flow.get("flow")
                    changed = True

        # Remove flows no longer present in source
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
            http_put(f"{tgt_base}editables/{record_type}/addOrEdit", tgt_key, payload)
        else:
            logger.info(
                "%s '%s' flows unchanged — skipping",
                record_type,
                tgt.get("name"),
            )

def sync_iqa_drug_links(
    iqa_mapping: dict,           # {src_iqa_id: tgt_iqa_id}
    drug_substance_mapping: dict,# {src_ds_id: tgt_ds_id}
    drug_product_mapping: dict,  # {src_dp_id: tgt_dp_id}
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
    src_project_id: int,
    src_process_id: int,
):
    """
    Sync IQA links to DrugSubstance / DrugProduct.
    This must run AFTER DrugSubstance and DrugProduct flows are synced.
    """
    for src_iqa_id, tgt_iqa_id in iqa_mapping.items():
        src_iqa = http_get(individual_record_url(src_base, "IQA", src_iqa_id), src_key)
        src_iqa = validate_target_scope(src_iqa, src_project_id, src_process_id, "source IQA")
        if not src_iqa:
            continue

        tgt_iqa = http_get(individual_record_url(tgt_base, "IQA", tgt_iqa_id), tgt_key)

        changed = False

        # Map DrugSubstanceId
        src_ds_id = src_iqa.get("DrugSubstanceId")
        if src_ds_id:
            tgt_ds_id = drug_substance_mapping.get(src_ds_id)
            if tgt_iqa.get("DrugSubstanceId") != tgt_ds_id:
                tgt_iqa["DrugSubstanceId"] = tgt_ds_id
                changed = True

        # Map DrugProductId
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
            http_put(f"{tgt_base}editables/IQA/addOrEdit", tgt_key, tgt_iqa)
        else:
            logger.info(
                "IQA '%s' links to DrugSubstance/DrugProduct unchanged — skipping",
                tgt_iqa.get("name"),
            )
# --------------------- COPY HELPERS ---------------------
def copy_unit_operations(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    # Get source Unit Operations
    explorer = get_process_explorer(src_base, src_key, src_project_id, src_process_id)
    uo_map = explorer.get("uoMap", {})
    record_keys = convert_map_to_record_keys(uo_map)

    if not record_keys:
        logger.info("No Unit Operations found for source process %s", src_process_id)
        return {}

    # Fetch full source UOs
    src_uos = []
    for k in record_keys:
        src_uo = http_get(individual_record_url(src_base, "UnitOperation", int(k.split('-')[1])), src_key)
        src_uo = validate_target_scope(src_uo, src_project_id, src_process_id, "source UnitOperation")
        if not src_uo:
            continue
        src_uos.append(src_uo)
    src_uos.sort(key=lambda uo: uo.get("order") or 0)

    dup_uo_names = find_duplicate_keys(src_uos, lambda r: r.get("name"))
    if dup_uo_names:
        logger.info("Duplicate UnitOperation names in source; disabling name-based fallback for: %s", ", ".join(sorted(dup_uo_names)))

    # load target UOs
    resp = requests.get(
        f"{tgt_base}editables/UnitOperation/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
        )
    resp.raise_for_status()
    tgt_instances = resp.json().get("instances", [])
    tgt_by_name = {uo["name"]: uo for uo in tgt_instances if uo.get("ProcessId") == tgt_process_id}

    mapping = {}

    for src_uo in src_uos:
        if is_archived(src_uo):
                        continue
        src_id = src_uo["id"]
        src_name = src_uo.get("name")

        # Use persisted mapping first
        tgt_uo_id = map_lookup(prev_mapping, src_id)
        tgt_uo = http_get(individual_record_url(tgt_base, "UnitOperation", tgt_uo_id), tgt_key) if tgt_uo_id else None
        tgt_uo = validate_target_scope(tgt_uo, tgt_project_id, tgt_process_id, "UnitOperation")

        # Fallback to name-based lookup
        if not tgt_uo_id and src_name in tgt_by_name and src_name not in dup_uo_names:
            tgt_uo = tgt_by_name[src_name]
            tgt_uo_id = tgt_uo["id"]

        if not tgt_uo_id and src_name in dup_uo_names:
            logger.info("UnitOperation '%s' has duplicate name in source; skipping name-based fallback", src_name)

        if tgt_uo:
            tgt_uo = ensure_full_record("UnitOperation", tgt_uo, tgt_base, tgt_key)

        payload = sanitize_payload(
            src_uo,
            ALLOWED_UNIT_OPERATION_FIELDS,
            {"ProjectId": tgt_project_id, "ProcessId": tgt_process_id},
        )

        changed_fields = []
        if tgt_uo:
            # Strip attachments from target before diff to match payload sanitation
            try:
                tgt_uo_for_diff = json.loads(json.dumps(tgt_uo))
            except Exception:
                tgt_uo_for_diff = dict(tgt_uo)
            tgt_uo_for_diff = strip_attachment_links(tgt_uo_for_diff)

            for field in ALLOWED_UNIT_OPERATION_FIELDS:
                if freeze_for_compare(payload.get(field)) != freeze_for_compare(tgt_uo_for_diff.get(field)):
                    changed_fields.append(field)
            # Ignore order-only changes
            if changed_fields == ["order"]:
                changed_fields = []

        # Create or update
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
                http_put(f"{tgt_base}editables/UnitOperation/addOrEdit", tgt_key, payload)
        else:
            logger.info("Creating UnitOperation '%s'", src_name)
            new_uo = http_put(f"{tgt_base}editables/UnitOperation/addOrEdit", tgt_key, payload)
            tgt_uo_id = new_uo["id"]
            logger.info("Created UnitOperation '%s': %s -> %s", src_name, src_id, tgt_uo_id)

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
    import json

    src_order_raw = src_process.get("unitOperationOrder")
    tgt_order_raw = tgt_process.get("unitOperationOrder")

    if not src_order_raw or not tgt_order_raw:
        return

    src_order = json.loads(src_order_raw)
    tgt_order = json.loads(tgt_order_raw)

    # source: source UO id -> order
    src_order_map = {
        entry["unitOperationId"]: entry["order"]
        for entry in src_order
    }

    changed = False

    for tgt_entry in tgt_order:
        # find source UO that maps to target UO
        for src_uo_id, tgt_uo_id in uo_mapping.items():
            if tgt_uo_id == tgt_entry["unitOperationId"]:
                src_order_val = map_lookup(src_order_map, src_uo_id)
                if src_order_val is not None and tgt_entry["order"] != src_order_val:
                    tgt_entry["order"] = src_order_val
                    changed = True
                break

    if not changed:
        logger.info(
            "Unit operation order unchanged for Process '%s' — skipping",
            tgt_process.get("name"),
        )
        return

    payload = sanitize_payload(
        tgt_process,
        ALLOWED_PROCESS_FIELDS,
        {
            "unitOperationOrder": json.dumps(tgt_order),
        },
    )

    payload["id"] = tgt_process["id"]
    payload["LastVersionId"] = tgt_process["LastVersionId"]

    logger.info("Updating unit operation order on Process '%s'", tgt_process["name"])

    put_process_fn(payload)

def copy_steps(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    # Fetch source steps
    explorer = get_process_explorer(src_base, src_key, src_project_id, src_process_id)
    step_map = explorer.get("stpMap", {})
    record_keys = convert_map_to_record_keys(step_map)
    if not record_keys:
        logger.info("No steps found for source process %s", src_process_id)
        return {}

    src_steps = []
    for key in record_keys:
        step_id = int(key.split("-")[1])
        step = http_get(individual_record_url(src_base, "Step", step_id), src_key)
        step = validate_target_scope(step, src_project_id, src_process_id, "source Step")
        if not step:
            continue
        src_steps.append(step)

    dup_step_keys = find_duplicate_keys(src_steps, lambda r: (r.get("UnitOperationId"), r.get("name")))
    if dup_step_keys:
        logger.info("Duplicate Step names in source for some UnitOperations; disabling name-based fallback for those pairs")

    # Group source steps by UnitOperation
    steps_by_uo = {}
    for step in src_steps:
        uo_id = step.get("UnitOperationId")
        steps_by_uo.setdefault(uo_id, []).append(step)

    # load target steps
    resp = requests.get(
        f"{tgt_base}editables/Step/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
        )
    resp.raise_for_status()
    tgt_instances = resp.json().get("instances", [])

    # Build lookup: mapped UO -> {step name: step obj}
    tgt_by_uo_name = {}
    for s in tgt_instances:
        tgt_uo_id = s.get("UnitOperationId")
        tgt_by_uo_name.setdefault(tgt_uo_id, {})[s["name"]] = s

    mapping = {}

    for src_uo_id, step_list in steps_by_uo.items():
        tgt_uo_id = map_lookup(uo_mapping, src_uo_id)
        if not tgt_uo_id:
            logger.warning("No target UnitOperation for source UO %s; skipping steps", src_uo_id)
            continue

        # Order steps using PreviousStepId
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

            # Use persisted mapping
            tgt_step_id = prev_mapping.get(str(src_step_id))
            tgt_step = http_get(individual_record_url(tgt_base, "Step", tgt_step_id), tgt_key) if tgt_step_id else None
            tgt_step = validate_target_scope(tgt_step, tgt_project_id, tgt_process_id, "Step")

            # Fallback to name-based lookup
            if not tgt_step_id and (src_uo_id, src_name) not in dup_step_keys:
                tgt_step_obj = tgt_by_uo_name.get(tgt_uo_id, {}).get(src_name)
                if tgt_step_obj:
                    tgt_step_id = tgt_step_obj["id"]
                    tgt_step = http_get(individual_record_url(tgt_base, "Step", tgt_step_id), tgt_key)

            if not tgt_step_id and (src_uo_id, src_name) in dup_step_keys:
                logger.info("Step '%s' has duplicate name in source UO %s; skipping name-based fallback", src_name, src_uo_id)

            if tgt_step:
                tgt_step = ensure_full_record("Step", tgt_step, tgt_base, tgt_key)

            # Full source payload
            full_src = http_get(individual_record_url(src_base, "Step", src_step_id), src_key)
            if is_archived(full_src):
                continue
            payload = sanitize_payload(full_src, ALLOWED_STEP_FIELDS, {
                "ProjectId": tgt_project_id,
                "ProcessId": tgt_process_id,
                "UnitOperationId": tgt_uo_id,
            })

            # Detect changes
            changed_fields = []
            if tgt_step:
                for field in ALLOWED_STEP_FIELDS:
                    if param_changed(payload.get(field), tgt_step.get(field)):
                        changed_fields.append(field)

            # Create or update
            if tgt_step:
                if not changed_fields:
                    logger.info("Step '%s' unchanged — skipping", src_name)
                else:
                    payload["id"] = tgt_step_id
                    payload["LastVersionId"] = tgt_step["LastVersionId"]
                    logger.info("Updating Step '%s' (id %s): changed fields: %s", src_name, tgt_step_id, changed_fields)
                    http_put(f"{tgt_base}editables/Step/addOrEdit", tgt_key, payload)
            else:
                logger.info("Creating Step '%s'", src_name)
                new_step = http_put(f"{tgt_base}editables/Step/addOrEdit", tgt_key, payload)
                tgt_step_id = new_step["id"]
                logger.info("Created Step '%s': %s -> %s", src_name, src_step_id, tgt_step_id)

            mapping[src_step_id] = tgt_step_id

    return mapping

def sync_step_order(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
    uo_mapping: dict,
    step_mapping: dict,
):  
    """
    Sync stepOrder on target UnitOperations to match source ordering.
    """
    for src_uo_id, tgt_uo_id in uo_mapping.items():

        # fetch full source + target UO 
        src_uo = http_get(individual_record_url(src_base, "UnitOperation", src_uo_id), src_key)
        tgt_uo = http_get(individual_record_url(tgt_base, "UnitOperation", tgt_uo_id), tgt_key)

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

            tgt_step = http_get(individual_record_url(tgt_base, "Step", tgt_step_id), tgt_key)

            new_order.append({
                "uuid": entry.get("uuid"),  # can be reused or regenerated, don't think it matters.
                "stepId": tgt_step_id,
                "stepVersionId": tgt_step["LastVersionId"],
                "order": entry["order"],
            })

        # diff check
        existing = json.loads(tgt_uo.get("stepOrder") or "[]")

        def normalize(order):
            return sorted(
                [(o["stepId"], o["order"]) for o in order],
                key=lambda x: x[1],
            )

        if normalize(existing) == normalize(new_order):
            logger.info(
                "Step order unchanged for UnitOperation %s — skipping",
                tgt_uo["name"],
            )
            continue

        # update UO 
        payload = sanitize_payload(
            tgt_uo,
            ALLOWED_UNIT_OPERATION_FIELDS,
            {
                "id": tgt_uo_id,
                "LastVersionId": tgt_uo["LastVersionId"],
                "ProcessId": tgt_uo["ProcessId"],
                "stepOrder": json.dumps(new_order),
            }
        )

        logger.info(
            "Updating step order for UnitOperation '%s'",
            tgt_uo["name"],
        )

        http_put(
            f"{tgt_base}editables/UnitOperation/addOrEdit",
            tgt_key,
            payload,
        )

def copy_process_components(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    # Fetch source components
    resp = requests.get(
        f"{src_base}editables/ProcessComponent/list/{src_project_id}",
        headers=headers(src_key),
        params={"processId": src_process_id},
        )
    resp.raise_for_status()
    src_list = resp.json().get("instances", [])
    if not src_list:
        logger.info("No Process Components found for source process %s", src_process_id)
        return {}

    dup_pc_names = find_duplicate_keys(src_list, lambda r: r.get("name"))
    if dup_pc_names:
        logger.info("Duplicate ProcessComponent names in source; disabling name-based fallback for: %s", ", ".join(sorted(dup_pc_names)))

    # Fetch target components for name lookup (first-time)
    resp = requests.get(
        f"{tgt_base}editables/ProcessComponent/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
        )
    resp.raise_for_status()
    tgt_list = resp.json().get("instances", [])
    tgt_by_name = {c["name"]: c for c in tgt_list}

    mapping = {}

    for src_pc in src_list:
        if is_archived(src_pc):
                        continue
        src_id = src_pc["id"]
        src_name = src_pc["name"]

        src_pc_scoped = validate_target_scope(src_pc, src_project_id, src_process_id, "source ProcessComponent")
        if not src_pc_scoped:
            continue

        # Check previous mapping first
        tgt_pc_id = map_lookup(prev_mapping, src_id)
        tgt_full = http_get(individual_record_url(tgt_base, "ProcessComponent", tgt_pc_id), tgt_key) if tgt_pc_id else None
        tgt_full = validate_target_scope(tgt_full, tgt_project_id, tgt_process_id, "ProcessComponent")

        # Fallback to name-based lookup if no mapping
        if not tgt_pc_id and src_name in tgt_by_name and src_name not in dup_pc_names:
            tgt_pc = tgt_by_name[src_name]
            tgt_pc_id = tgt_pc["id"]
            tgt_full = http_get(individual_record_url(tgt_base, "ProcessComponent", tgt_pc_id), tgt_key)

        if not tgt_pc_id and src_name in dup_pc_names:
            logger.info("ProcessComponent '%s' has duplicate name in source; skipping name-based fallback", src_name)

        # Fetch full source
        full_src = http_get(individual_record_url(src_base, "ProcessComponent", src_id), src_key)
        full_src = validate_target_scope(full_src, src_project_id, src_process_id, "source ProcessComponent")
        if not full_src:
            continue
        if is_archived(full_src):
            continue

        # Map UnitOperations and Steps
        uos = [
            {"id": map_lookup(uo_mapping, uo.get("UnitOperationId") or uo.get("id"))}
            for uo in full_src.get("UnitOperations", [])
            if map_lookup(uo_mapping, uo.get("UnitOperationId") or uo.get("id"))
        ]
        steps = [
            {"id": map_lookup(step_mapping, step.get("StepId") or step.get("id"))}
            for step in full_src.get("Steps", [])
            if map_lookup(step_mapping, step.get("StepId") or step.get("id"))
        ]

        payload = sanitize_payload(
            full_src,
            ALLOWED_PROCESS_COMPONENT_FIELDS,
            {
                "ProjectId": tgt_project_id,
                "ProcessId": tgt_process_id,
                "UnitOperations": uos,
                "Steps": steps,
            }
        )

        
        if tgt_full:
            if not payload.get("name"):
                payload["name"] = tgt_full.get("name")
            if not payload.get("type"):
                payload["type"] = tgt_full.get("type")

        # Detect changes
        changed_fields = []
        if tgt_full:
            tgt_full = ensure_full_record("ProcessComponent", tgt_full, tgt_base, tgt_key)
            tgt_full = add_tgt_acr_for_diff(tgt_full)
            for field in ALLOWED_PROCESS_COMPONENT_FIELDS:
                if field in ["UnitOperations", "Steps"]:
                    continue
                if param_changed(payload.get(field), tgt_full.get(field)):
                    changed_fields.append(field)

            # Compare UO and Step IDs separately
            src_uo_ids_set = {uo["id"] for uo in payload.get("UnitOperations", [])}
            tgt_uo_ids_set = {uo.get("id") for uo in tgt_full.get("UnitOperations", [])}
            if src_uo_ids_set != tgt_uo_ids_set:
                changed_fields.append("UnitOperations")
                logger.info(
                    "ProcessComponent '%s' UO diff: src=%s tgt=%s",
                    src_name,
                    sorted(src_uo_ids_set),
                    sorted(tgt_uo_ids_set),
                )

            src_step_ids_set = {s["id"] for s in payload.get("Steps", [])}
            tgt_step_ids_set = {s.get("id") for s in tgt_full.get("Steps", [])}
            if src_step_ids_set != tgt_step_ids_set:
                changed_fields.append("Steps")
                logger.info(
                    "ProcessComponent '%s' Step diff: src=%s tgt=%s",
                    src_name,
                    sorted(src_step_ids_set),
                    sorted(tgt_step_ids_set),
                )

        # Create or update
        if tgt_full:
            if not changed_fields:
                logger.info("ProcessComponent '%s' unchanged — skipping", src_name)
            else:
                payload["id"] = tgt_pc_id
                payload["LastVersionId"] = tgt_full.get("LastVersionId")
                logger.info("Updating ProcessComponent '%s' (id %s): changed fields: %s", src_name, tgt_pc_id, changed_fields)
                try:
                    http_put(f"{tgt_base}editables/ProcessComponent/addOrEdit", tgt_key, payload)
                except requests.HTTPError as e:
                    logger.error("HTTP error: %s", e)
                    if getattr(e, "response", None) is not None:
                        logger.error("Response body: %s", e.response.text)
                    raise
        else:
            logger.info("Creating ProcessComponent '%s'", src_name)
            try:
                new_pc = http_put(f"{tgt_base}editables/ProcessComponent/addOrEdit", tgt_key, payload)
            except requests.HTTPError as e:
                logger.error("HTTP error: %s", e)
                if getattr(e, "response", None) is not None:
                    logger.error("Response body: %s", e.response.text)
                raise
            tgt_pc_id = new_pc["id"]

        # Save mapping
        mapping[src_id] = tgt_pc_id

    return mapping

def copy_materials(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    # Fetch source materials 
    resp = requests.get(
        f"{src_base}editables/Material/list/{src_project_id}",
        headers=headers(src_key),
        params={"processId": src_process_id},
        )
    resp.raise_for_status()
    src_materials = resp.json().get("instances", [])
    if not src_materials:
        logger.info("No Materials found for source process %s", src_process_id)
        return {}


    name_counts = Counter(m.get("name") for m in src_materials if m.get("name"))
    dup_material_names = {name for name, count in name_counts.items() if count > 1}
    if dup_material_names:
        logger.info("Duplicate Material names in source; disabling name-based fallback for: %s", ", ".join(sorted(dup_material_names)))

    # 0Fetch target materials for fallback lookup
    resp = requests.get(
        f"{tgt_base}editables/Material/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
        )
    resp.raise_for_status()
    tgt_materials = resp.json().get("instances", [])
    tgt_by_name = {m["name"]: m for m in tgt_materials}

    mapping = {}
    seen_tgt_ids = set()

    for src_mat in src_materials:
        if is_archived(src_mat):
                        continue
        src_id = src_mat["id"]
        src_name = src_mat["name"]

        src_mat_scoped = validate_target_scope(src_mat, src_project_id, src_process_id, "source Material")
        if not src_mat_scoped:
            continue

        # persisted mapping or fallback by name
        tgt_mat_id = map_lookup(prev_mapping, src_id)
        tgt_full = http_get(individual_record_url(tgt_base, "Material", tgt_mat_id), tgt_key) if tgt_mat_id else None
        tgt_full = validate_target_scope(tgt_full, tgt_project_id, tgt_process_id, "Material")

        if tgt_mat_id in seen_tgt_ids:
            logger.warning("Material '%s' maps to target id %s more than once; skipping duplicate update", src_name, tgt_mat_id)
            mapping[src_id] = tgt_mat_id
            continue
        if tgt_mat_id:
            seen_tgt_ids.add(tgt_mat_id)

        tgt_mat_id_before = tgt_mat_id

        if not tgt_full and src_name in tgt_by_name and src_name not in dup_material_names:
            tgt_mat_id = tgt_by_name[src_name]["id"]
            tgt_full = http_get(individual_record_url(tgt_base, "Material", tgt_mat_id), tgt_key)

        if not tgt_full and src_name in dup_material_names:
            logger.info("Material '%s' has duplicate name in source; skipping name-based fallback", src_name)

        if tgt_mat_id and tgt_mat_id != tgt_mat_id_before:
            if tgt_mat_id in seen_tgt_ids:
                logger.warning("Material '%s' maps to target id %s more than once; skipping duplicate update", src_name, tgt_mat_id)
                mapping[src_id] = tgt_mat_id
                continue
            seen_tgt_ids.add(tgt_mat_id)

        # fetch full source material
        full_src = http_get(individual_record_url(src_base, "Material", src_id), src_key)
        full_src = validate_target_scope(full_src, src_project_id, src_process_id, "source Material")
        if not full_src:
            continue
        if is_archived(full_src):
            continue

        # map MaterialFlows
        material_flows = []
        for flow in full_src.get("MaterialFlows", []):
            tgt_uo_id = map_lookup(uo_mapping, flow.get("UnitOperationId"))
            tgt_step_id = map_lookup(step_mapping, flow.get("StepId"))

            if not tgt_uo_id and not tgt_step_id:
                continue  # skip if no mapped UO/Step

            flow_type = flow.get("flow", "Input")
            if flow_type not in {"Input", "Intermediate", "Output"}:
                flow_type = "Input"

            material_flows.append({
                "ProcessId": tgt_process_id,
                "UnitOperationId": tgt_uo_id,
                "StepId": tgt_step_id,
                "flow": flow_type
            })

        # UnitOperations and Steps payload
        uos = [{"id": mf["UnitOperationId"], "label": f"UO-{mf['UnitOperationId']}"} for mf in material_flows if mf["UnitOperationId"]]
        steps = [{"id": mf["StepId"], "label": f"STP-{mf['StepId']}"} for mf in material_flows if mf["StepId"]]

        payload = sanitize_payload(
            full_src,
            ALLOWED_MATERIAL_FIELDS,
            {
                "ProjectId": tgt_project_id,
                "ProcessId": tgt_process_id,
                "UnitOperations": uos,
                "Steps": steps,
                "MaterialFlows": material_flows
            }
        )

        
        if tgt_full:
            if not payload.get("name"):
                payload["name"] = tgt_full.get("name")

        changed_fields = []

        if tgt_full:
            tgt_full = ensure_full_record("Material", tgt_full, tgt_base, tgt_key)
            try:
                tgt_full = json.loads(json.dumps(tgt_full))
            except Exception:
                tgt_full = dict(tgt_full)
            tgt_full = strip_attachment_links(tgt_full)
            tgt_full = add_tgt_acr_for_diff(tgt_full)
            for field in ALLOWED_MATERIAL_FIELDS:
                if param_changed(payload.get(field), tgt_full.get(field)):
                    changed_fields.append(field)

            # compare relationship IDs only
            if {u["id"] for u in uos} != {u["id"] for u in tgt_full.get("UnitOperations", [])}:
                changed_fields.append("UnitOperations")

            if {s["id"] for s in steps} != {s["id"] for s in tgt_full.get("Steps", [])}:
                changed_fields.append("Steps")

            if { (mf.get("UnitOperationId"), mf.get("StepId"), mf.get("flow")) for mf in material_flows } != \
               { (mf.get("UnitOperationId"), mf.get("StepId"), mf.get("flow")) for mf in tgt_full.get("MaterialFlows", []) }:
                changed_fields.append("MaterialFlows")

        # create or update
        if tgt_full:
            if not changed_fields:
                logger.info("Material '%s' unchanged — skipping", src_name)
            else:
                payload["id"] = tgt_mat_id
                payload["LastVersionId"] = tgt_full["LastVersionId"]
                logger.info(
                    "Updating Material '%s' (id %s): changed fields: %s",
                    src_name, tgt_mat_id, changed_fields
                )
                http_put(f"{tgt_base}editables/Material/addOrEdit", tgt_key, payload)
        else:
            logger.info("Creating Material '%s'", src_name)
            new_mat = http_put(f"{tgt_base}editables/Material/addOrEdit", tgt_key, payload)
            tgt_mat_id = new_mat["id"]

        mapping[src_id] = tgt_mat_id

    return mapping

def copy_material_attributes(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    # Fetch source material attributes
    resp = requests.get(
        f"{src_base}editables/MaterialAttribute/list/{src_project_id}",
        headers=headers(src_key),
        params={"processId": src_process_id},
        )
    resp.raise_for_status()
    src_attributes = resp.json().get("instances", [])
    if not src_attributes:
        logger.info("No Material Attributes found for source process %s", src_process_id)
        return {}


    dup_attr_names = find_duplicate_keys(src_attributes, lambda r: r.get("name"))
    if dup_attr_names:
        logger.info("Duplicate Material Attribute names in source; disabling name-based fallback for: %s", ", ".join(sorted(dup_attr_names)))

    # Fetch target attributes for fallback lookup by name
    resp = requests.get(
        f"{tgt_base}editables/MaterialAttribute/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
        )
    resp.raise_for_status()
    tgt_attributes = resp.json().get("instances", [])
    tgt_by_name = {m["name"]: m for m in tgt_attributes}

    # Fetch ControlMethods for target project
    resp = requests.get(
        f"{tgt_base}editables/ControlMethod/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
        )
    resp.raise_for_status()
    target_cms = resp.json().get("instances", [])
    cm_lookup = {cm["name"]: cm["id"] for cm in target_cms}

    mapping = {}

    for src_attr in src_attributes:
        if is_archived(src_attr):
                        continue
        src_id = src_attr["id"]
        src_name = src_attr["name"]

        src_attr_scoped = validate_target_scope(src_attr, src_project_id, src_process_id, "source MaterialAttribute")
        if not src_attr_scoped:
            continue

        # Check persisted mapping
        tgt_attr_id = map_lookup(prev_mapping, src_id)
        tgt_full = http_get(individual_record_url(tgt_base, "MaterialAttribute", tgt_attr_id), tgt_key) if tgt_attr_id else None
        tgt_full = validate_target_scope(tgt_full, tgt_project_id, tgt_process_id, "MaterialAttribute")

        # Fallback by name
        if not tgt_full and src_name in tgt_by_name and src_name not in dup_attr_names:
            tgt_attr_id = tgt_by_name[src_name]["id"]
            tgt_full = http_get(individual_record_url(tgt_base, "MaterialAttribute", tgt_attr_id), tgt_key)

        if not tgt_full and src_name in dup_attr_names:
            logger.info("Material Attribute '%s' has duplicate name in source; skipping name-based fallback", src_name)

        # Fetch full source 
        full_src = http_get(individual_record_url(src_base, "MaterialAttribute", src_id), src_key)
        full_src = validate_target_scope(full_src, src_project_id, src_process_id, "source MaterialAttribute")
        if not full_src:
            continue
        if is_archived(full_src):
            continue

        # Map related IDs
        uo_id = map_lookup(uo_mapping, full_src.get("UnitOperationId"))
        step_id = map_lookup(step_mapping, full_src.get("StepId"))

        pc_id = map_lookup(pc_mapping, full_src.get("ProcessComponentId"))
        mat_id = map_lookup(material_mapping, full_src.get("MaterialId"))

        # Map ControlMethods by name
        tgt_cms_mapped, cm_changed = map_and_diff_control_methods(
        full_src.get("ControlMethods", []),
        tgt_full.get("ControlMethods", []) if tgt_full else [],
        cm_lookup,
    )

        # Build sanitized payload
        payload = sanitize_payload(
            full_src,
            ALLOWED_MATERIAL_ATTRIBUTE_FIELDS,
            {
                "ProjectId": tgt_project_id,
                "ProcessId": tgt_process_id,
                "UnitOperationId": uo_id,
                "StepId": step_id,
                "ProcessComponentId": pc_id,
                "MaterialId": mat_id,
                "ControlMethods": tgt_cms_mapped,
            }
        )
        requirement_payload = add_acr_to_payload(full_src, payload)

        # Diff detection
        changed_fields = []
        if tgt_full:
            tgt_full = ensure_full_record("MaterialAttribute", tgt_full, tgt_base, tgt_key)
            tgt_full = add_tgt_acr_for_diff(tgt_full)
            for field in ALLOWED_MATERIAL_ATTRIBUTE_FIELDS:
                if field == "ControlMethods":
                    # Compare names instead of IDs
                    src_names = {cm["name"] for cm in full_src.get("ControlMethods", [])}
                    tgt_names = {cm.get("name") for cm in tgt_full.get("ControlMethods", [])}
                    if src_names != tgt_names:
                        changed_fields.append(field)
                else:
                    if field == "ControlMethods":
                        if cm_changed:
                            changed_fields.append(field)
                    elif param_changed(payload.get(field), tgt_full.get(field)):
                        if field == "AcceptanceCriteriaRanges":
                            src_acr = requirement_payload.get("AcceptanceCriteriaRanges") if requirement_payload else []
                            tgt_full_acr = add_tgt_acr_for_diff(tgt_full)
                            tgt_acr = tgt_full_acr.get("AcceptanceCriteriaRanges") if isinstance(tgt_full_acr, dict) else None
                            logger.info(
                                "Material Attribute '%s' ACR diff: src=%s tgt=%s",
                                src_name, src_acr, tgt_acr,
                            )
                            # Raw source fields to diagnose missing ACRs
                            logger.info(
                                "Material Attribute '%s' ACR raw: Requirement=%r, AcceptanceCriteriaRanges=%r, AcceptanceCriteriaRangeLinkedVersions=%r",
                                src_name,
                                full_src.get("Requirement"),
                                full_src.get("AcceptanceCriteriaRanges"),
                                full_src.get("AcceptanceCriteriaRangeLinkedVersions"),
                            )
                        changed_fields.append(field)

        # Create or update
        if tgt_full:
            if not changed_fields:
                logger.info("Material Attribute '%s' unchanged — skipping", src_name)
            else:
                payload["id"] = tgt_attr_id
                payload["LastVersionId"] = tgt_full["LastVersionId"]
                logger.info(
                    "Updating Material Attribute '%s' (id %s): changed fields: %s",
                    src_name, tgt_attr_id, changed_fields
                )
                if requirement_payload:
                    payload.pop("AcceptanceCriteriaRanges", None)
                    payload["Requirement"] = requirement_payload
                http_put(f"{tgt_base}editables/MaterialAttribute/addOrEdit", tgt_key, payload)
        else:
            logger.info("Creating Material Attribute '%s'", src_name)
            if requirement_payload:
                payload.pop("AcceptanceCriteriaRanges", None)
                payload["Requirement"] = requirement_payload
            new_attr = http_put(f"{tgt_base}editables/MaterialAttribute/addOrEdit", tgt_key, payload)
            tgt_attr_id = new_attr["id"]

        mapping[src_id] = tgt_attr_id

    return mapping

def copy_process_parameters(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    # Fetch source PPs
    resp = requests.get(
        f"{src_base}editables/ProcessParameter/list/{src_project_id}",
        headers=headers(src_key),
        params={"processId": src_process_id},
        )
    resp.raise_for_status()
    src_params = resp.json().get("instances", [])
    if not src_params:
        logger.info("No Process Parameters found for source process %s", src_process_id)
        return {}

    # Fetch target PPs for fallback lookup
    resp = requests.get(
        f"{tgt_base}editables/ProcessParameter/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
        )
    resp.raise_for_status()
    tgt_instances = resp.json().get("instances", [])

    # Build lookup by name + IDs
    tgt_lookup = {
        (p.get("name"), p.get("UnitOperationId"), p.get("StepId"), p.get("ProcessComponentId"), p.get("MaterialId")): p
        for p in tgt_instances
    }

    mapping = {}

    for src_pp in src_params:
        if is_archived(src_pp):
                        continue
        src_id = src_pp["id"]

        src_pp_scoped = validate_target_scope(src_pp, src_project_id, src_process_id, "source ProcessParameter")
        if not src_pp_scoped:
            continue

        # Map related IDs
        tgt_uo_id = map_lookup(uo_mapping, src_pp.get("UnitOperationId"))
        tgt_step_id = map_lookup(step_mapping, src_pp.get("StepId")) if src_pp.get("StepId") else None
        tgt_pc_id = map_lookup(process_component_mapping, src_pp.get("ProcessComponentId")) if src_pp.get("ProcessComponentId") else None
        tgt_mat_id = map_lookup(material_mapping, src_pp.get("MaterialId")) if src_pp.get("MaterialId") else None  # ✅ new

        # Find target PP
        tgt_pp_id = map_lookup(prev_mapping, src_id)
        if tgt_pp_id:
            tgt_pp_stub = http_get(individual_record_url(tgt_base, "ProcessParameter", tgt_pp_id), tgt_key)
            tgt_pp_stub = validate_target_scope(tgt_pp_stub, tgt_project_id, tgt_process_id, "ProcessParameter")
        else:
            key = (src_pp["name"], tgt_uo_id, tgt_step_id, tgt_pc_id, tgt_mat_id)
            tgt_pp_stub = tgt_lookup.get(key)

        # Fetch full source
        full_src = http_get(individual_record_url(src_base, "ProcessParameter", src_id), src_key)
        full_src = validate_target_scope(full_src, src_project_id, src_process_id, "source ProcessParameter")
        if not full_src:
            continue
        if is_archived(full_src):
            continue

        # Prepare payload 
        payload_extra = {
            "ProjectId": tgt_project_id,
            "ProcessId": tgt_process_id,
            "UnitOperationId": tgt_uo_id,
        }
        if tgt_step_id:
            payload_extra["StepId"] = tgt_step_id
        if tgt_pc_id:
            payload_extra["ProcessComponentId"] = tgt_pc_id
        if tgt_mat_id: 
            payload_extra["MaterialId"] = tgt_mat_id

        payload = sanitize_payload(full_src, ALLOWED_PROCESS_PARAMETER_FIELDS, payload_extra)
        requirement_payload = add_acr_to_payload(full_src, payload)

        # Diff detection
        changed_fields = []
        if tgt_pp_stub:
            tgt_pp_stub = ensure_full_record("ProcessParameter", tgt_pp_stub, tgt_base, tgt_key)
            tgt_pp_stub = add_tgt_acr_for_diff(tgt_pp_stub)

            fields_to_check = ALLOWED_PROCESS_PARAMETER_FIELDS + ["UnitOperationId", "StepId", "ProcessComponentId", "MaterialId"]  # ✅ include MaterialId

            for field in fields_to_check:
                if param_changed(payload.get(field), tgt_pp_stub.get(field)):
                    changed_fields.append(field)

        # Create or update
        if tgt_pp_stub:
            if not changed_fields:
                logger.info("ProcessParameter '%s' unchanged — skipping", src_pp["name"])
                mapping[src_id] = tgt_pp_stub["id"]
                continue

            # Update existing
            payload["id"] = tgt_pp_stub["id"]
            payload["LastVersionId"] = tgt_pp_stub["LastVersionId"]
            logger.info(
                "Updating ProcessParameter '%s' (id %s): changed fields: %s",
                src_pp["name"], tgt_pp_stub["id"], changed_fields
            )
            if requirement_payload:
                payload.pop("AcceptanceCriteriaRanges", None)
                payload["Requirement"] = requirement_payload
            http_put(f"{tgt_base}editables/ProcessParameter/addOrEdit", tgt_key, payload)
            mapping[src_id] = tgt_pp_stub["id"]

        else:
            # Create new
            logger.info("Creating ProcessParameter '%s'", src_pp["name"])
            if requirement_payload:
                payload.pop("AcceptanceCriteriaRanges", None)
                payload["Requirement"] = requirement_payload
            new_pp = http_put(f"{tgt_base}editables/ProcessParameter/addOrEdit", tgt_key, payload)
            mapping[src_id] = new_pp["id"]

    return mapping

def copy_iqas(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    # fetch source IQAs
    resp = requests.get(
        f"{src_base}editables/IQA/list/{src_project_id}",
        headers=headers(src_key),
        params={"processId": src_process_id},
        )
    resp.raise_for_status()
    src_iqas = resp.json().get("instances", [])
    if not src_iqas:
        logger.info("No IQAs found for source process %s", src_process_id)
        return {}


    dup_iqa_names = find_duplicate_keys(src_iqas, lambda r: r.get("name"))
    if dup_iqa_names:
        logger.info("Duplicate IQA names in source; disabling name-based fallback for: %s", ", ".join(sorted(dup_iqa_names)))

    # Fetch target IQAs
    resp = requests.get(
        f"{tgt_base}editables/IQA/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
        )
    resp.raise_for_status()
    tgt_iqas = resp.json().get("instances", [])
    tgt_by_name = {iqa["name"]: iqa for iqa in tgt_iqas}

    # Fetch target ControlMethods
    resp = requests.get(
        f"{tgt_base}editables/ControlMethod/list/{tgt_project_id}",
        headers=headers(tgt_key),
        )
    resp.raise_for_status()
    tgt_cms = resp.json().get("instances", [])
    cm_lookup = {cm["name"]: cm["id"] for cm in tgt_cms}

    mapping = {}

    for src_stub in src_iqas:
        if is_archived(src_stub):
                        continue
        src_id = src_stub["id"]
        src_name = src_stub["name"]

        # find existing target IQA 
        tgt_id = map_lookup(prev_mapping, src_id)
        tgt_full = http_get(individual_record_url(tgt_base, "IQA", tgt_id), tgt_key) if tgt_id else None
        tgt_full = validate_target_scope(tgt_full, tgt_project_id, tgt_process_id, "IQA")

        if not tgt_full and src_name in tgt_by_name and src_name not in dup_iqa_names:
            tgt_id = tgt_by_name[src_name]["id"]
            tgt_full = http_get(individual_record_url(tgt_base, "IQA", tgt_id), tgt_key)

        if not tgt_full and src_name in dup_iqa_names:
            logger.info("IQA '%s' has duplicate name in source; skipping name-based fallback", src_name)

        # skip if indicates different project/process
        src_stub_scoped = validate_target_scope(src_stub, src_project_id, src_process_id, "source IQA")
        if not src_stub_scoped:
            continue

        # fetch full source
        full_src = http_get(individual_record_url(src_base, "IQA", src_id), src_key)
        full_src = validate_target_scope(full_src, src_project_id, src_process_id, "source IQA")
        if not full_src:
            continue
        if is_archived(full_src):
            continue

        # remap keys
        tgt_uo_id = map_lookup(uo_mapping, full_src.get("UnitOperationId"))
        tgt_step_id = map_lookup(step_mapping, full_src.get("StepId")) if full_src.get("StepId") else None

        # map ControlMethods by NAME
        tgt_cms_payload, cm_changed = map_and_diff_control_methods(
        full_src.get("ControlMethods", []),
        tgt_full.get("ControlMethods", []) if tgt_full else [],
        cm_lookup,
    )

        payload = sanitize_payload(
            full_src,
            ALLOWED_IQA_FIELDS,
            {
                "ProjectId": tgt_project_id,
                "ProcessId": tgt_process_id,
                "UnitOperationId": tgt_uo_id,
                "StepId": tgt_step_id,
                "ControlMethods": tgt_cms_payload,
            }
        )
        requirement_payload = add_acr_to_payload(full_src, payload)

        # diff detection
        changed_fields = []

        if tgt_full:
            tgt_full = ensure_full_record("IQA", tgt_full, tgt_base, tgt_key)
            tgt_full = add_tgt_acr_for_diff(tgt_full)
            for field in ALLOWED_IQA_FIELDS:
                if field == "ControlMethods":
                    src_names = {cm["name"] for cm in full_src.get("ControlMethods", [])}
                    tgt_names = {cm.get("name") for cm in tgt_full.get("ControlMethods", [])}
                    if src_names != tgt_names:
                        changed_fields.append(field)
                else:
                    if field == "ControlMethods":
                        if cm_changed:
                            changed_fields.append(field)
                    elif param_changed(payload.get(field), tgt_full.get(field)):
                        changed_fields.append(field)

        # create or update
        if tgt_full:
            if not changed_fields:
                logger.info("IQA '%s' unchanged — skipping", src_name)
            else:
                payload["id"] = tgt_id
                payload["LastVersionId"] = tgt_full["LastVersionId"]
                logger.info(
                    "Updating IQA '%s' (id %s): changed fields: %s",
                    src_name, tgt_id, changed_fields
                )
                if requirement_payload:
                    payload.pop("AcceptanceCriteriaRanges", None)
                    payload["Requirement"] = requirement_payload
                http_put(f"{tgt_base}editables/IQA/addOrEdit", tgt_key, payload)
        else:
            logger.info("Creating IQA '%s'", src_name)
            if requirement_payload:
                payload.pop("AcceptanceCriteriaRanges", None)
                payload["Requirement"] = requirement_payload
            new_iqa = http_put(f"{tgt_base}editables/IQA/addOrEdit", tgt_key, payload)
            tgt_id = new_iqa["id"]

        mapping[src_id] = tgt_id

    return mapping

def copy_ipas(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    uo_mapping: dict,
    step_mapping: dict,
    prev_mapping: dict = None,
) -> dict:

    prev_mapping = prev_mapping or {}

    # Fetch source IPAs
    resp = requests.get(
        f"{src_base}editables/IPA/list/{src_project_id}",
        headers=headers(src_key),
        params={"processId": src_process_id},
        )
    resp.raise_for_status()
    src_ipas = resp.json().get("instances", [])
    if not src_ipas:
        return {}

    # Fetch target IPAs
    resp = requests.get(
        f"{tgt_base}editables/IPA/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
        )
    resp.raise_for_status()
    tgt_ipas = resp.json().get("instances", [])

    tgt_lookup = {}
    for ipa in tgt_ipas:
        key = (ipa.get("name"), ipa.get("UnitOperationId"), ipa.get("StepId"))
        tgt_lookup[key] = ipa

    # Fetch target ControlMethods
    resp = requests.get(
        f"{tgt_base}editables/ControlMethod/list/{tgt_project_id}",
        headers=headers(tgt_key),
        )
    resp.raise_for_status()
    cm_lookup = {cm["name"]: cm["id"] for cm in resp.json().get("instances", [])}

    mapping = {}

    for src_ipa in src_ipas:
        if is_archived(src_ipa):
                        continue
        src_id = src_ipa["id"]

        src_ipa_scoped = validate_target_scope(src_ipa, src_project_id, src_process_id, "source IPA")
        if not src_ipa_scoped:
            continue

        tgt_uo_id = map_lookup(uo_mapping, src_ipa.get("UnitOperationId"))
        tgt_step_id = map_lookup(step_mapping, src_ipa.get("StepId")) if src_ipa.get("StepId") else None

        # Find target
        tgt_ipa_id = map_lookup(prev_mapping, src_id)
        if tgt_ipa_id:
            tgt_stub = http_get(individual_record_url(tgt_base, "IPA", tgt_ipa_id), tgt_key)
            tgt_stub = validate_target_scope(tgt_stub, tgt_project_id, tgt_process_id, "IPA")
        else:
            key = (src_ipa["name"], tgt_uo_id, tgt_step_id)
            tgt_stub = tgt_lookup.get(key)

        # Fetch full source
        full_src = http_get(individual_record_url(src_base, "IPA", src_id), src_key)
        full_src = validate_target_scope(full_src, src_project_id, src_process_id, "source IPA")
        if not full_src:
            continue
        if is_archived(full_src):
            continue

        # ControlMethods
        tgt_cms_payload, cm_changed = map_and_diff_control_methods(
            full_src.get("ControlMethods", []),
            tgt_stub.get("ControlMethods", []) if tgt_stub else [],
            cm_lookup,
        )

        payload = sanitize_payload(
            full_src,
            ALLOWED_IPA_FIELDS,
            {
                "ProjectId": tgt_project_id,
                "ProcessId": tgt_process_id,
                "UnitOperationId": tgt_uo_id,
                "StepId": tgt_step_id,
                "ControlMethods": tgt_cms_payload,
            }
        )
        requirement_payload = add_acr_to_payload(full_src, payload)

        changed_fields = []

        if tgt_stub:
            tgt_stub = ensure_full_record("IPA", tgt_stub, tgt_base, tgt_key)
            tgt_stub = add_tgt_acr_for_diff(tgt_stub)

            fields_to_check = ALLOWED_IPA_FIELDS + ["UnitOperationId", "StepId"]

            for field in fields_to_check:
                if field == "ControlMethods":
                    if cm_changed:
                        changed_fields.append(field)
                elif param_changed(payload.get(field), tgt_stub.get(field)):
                    changed_fields.append(field)

        # Create / Update
        if tgt_stub:
            if not changed_fields:
                logger.info("IPA '%s' unchanged — skipping", src_ipa["name"])
                mapping[src_id] = tgt_stub["id"]
                continue

            payload["id"] = tgt_stub["id"]
            payload["LastVersionId"] = tgt_stub["LastVersionId"]
            logger.info(
                "Updating IPA '%s' (id %s): changed fields: %s",
                src_ipa["name"],
                tgt_stub["id"],
                changed_fields,
            )
            if requirement_payload:
                payload.pop("AcceptanceCriteriaRanges", None)
                payload["Requirement"] = requirement_payload
            http_put(f"{tgt_base}editables/IPA/addOrEdit", tgt_key, payload)
            mapping[src_id] = tgt_stub["id"]

        else:
            logger.info("Creating IPA '%s'", src_ipa["name"])
            if requirement_payload:
                payload.pop("AcceptanceCriteriaRanges", None)
                payload["Requirement"] = requirement_payload
            new_ipa = http_put(f"{tgt_base}editables/IPA/addOrEdit", tgt_key, payload)
            mapping[src_id] = new_ipa["id"]

    return mapping

def copy_samples(
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
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

    resp = requests.get(
        f"{src_base}editables/Sample/list/{src_project_id}",
        headers=headers(src_key),
        params={"processId": src_process_id},
    )
    resp.raise_for_status()
    src_samples = resp.json().get("instances", [])
    if not src_samples:
        logger.info("No Samples found for source process %s", src_process_id)
        return {}

    resp = requests.get(
        f"{tgt_base}editables/Sample/list/{tgt_project_id}",
        headers=headers(tgt_key),
        params={"processId": tgt_process_id},
    )
    resp.raise_for_status()
    tgt_samples = resp.json().get("instances", [])

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

    for src_sample in src_samples:
        if is_archived(src_sample):
            continue
        src_id = src_sample["id"]

        src_sample_scoped = validate_target_scope(src_sample, src_project_id, src_process_id, "source Sample")
        if not src_sample_scoped:
            continue

        tgt_uo_id = map_lookup(uo_mapping, src_sample.get("UnitOperationId")) if src_sample.get("UnitOperationId") else None
        tgt_step_id = map_lookup(step_mapping, src_sample.get("StepId")) if src_sample.get("StepId") else None
        tgt_mat_id = map_lookup(material_mapping, src_sample.get("MaterialId")) if src_sample.get("MaterialId") else None
        tgt_matrix_mat_id = map_lookup(material_mapping, src_sample.get("MatrixMaterialId")) if src_sample.get("MatrixMaterialId") else None

        tgt_sample_id = map_lookup(prev_mapping, src_id)
        if tgt_sample_id:
            tgt_stub = http_get(individual_record_url(tgt_base, "Sample", tgt_sample_id), tgt_key)
            tgt_stub = validate_target_scope(tgt_stub, tgt_project_id, tgt_process_id, "Sample")
        else:
            key = (
                src_sample.get("name"),
                src_sample.get("type"),
                tgt_uo_id,
                tgt_step_id,
                tgt_mat_id,
                tgt_matrix_mat_id,
            )
            tgt_stub = tgt_lookup.get(key)

        full_src = http_get(individual_record_url(src_base, "Sample", src_id), src_key)
        full_src = validate_target_scope(full_src, src_project_id, src_process_id, "source Sample")
        if not full_src:
            continue
        if is_archived(full_src):
            continue

        tgt_uo_id = map_lookup(uo_mapping, full_src.get("UnitOperationId")) if full_src.get("UnitOperationId") else None
        tgt_step_id = map_lookup(step_mapping, full_src.get("StepId")) if full_src.get("StepId") else None
        tgt_mat_id = map_lookup(material_mapping, full_src.get("MaterialId")) if full_src.get("MaterialId") else None
        tgt_matrix_mat_id = map_lookup(material_mapping, full_src.get("MatrixMaterialId")) if full_src.get("MatrixMaterialId") else None

        payload = sanitize_payload(
            full_src,
            ALLOWED_SAMPLE_FIELDS,
            {
                "ProjectId": tgt_project_id,
                "ProcessId": tgt_process_id,
                "UnitOperationId": tgt_uo_id,
                "StepId": tgt_step_id,
                "MaterialId": tgt_mat_id,
                "MatrixMaterialId": tgt_matrix_mat_id,
            }
        )

        changed_fields = []
        if tgt_stub:
            tgt_stub = ensure_full_record("Sample", tgt_stub, tgt_base, tgt_key)
            for field in ALLOWED_SAMPLE_FIELDS:
                if param_changed(payload.get(field), tgt_stub.get(field)):
                    changed_fields.append(field)

        if tgt_stub:
            if not changed_fields:
                logger.info("Sample '%s' unchanged - skipping", full_src.get("name"))
                mapping[src_id] = tgt_stub["id"]
                continue

            payload["id"] = tgt_stub["id"]
            payload["LastVersionId"] = tgt_stub.get("LastVersionId")
            logger.info(
                "Updating Sample '%s' (id %s): changed fields: %s",
                full_src.get("name"),
                tgt_stub["id"],
                changed_fields,
            )
            http_put(f"{tgt_base}editables/Sample/addOrEdit", tgt_key, payload)
            mapping[src_id] = tgt_stub["id"]
        else:
            logger.info("Creating Sample '%s'", full_src.get("name"))
            new_sample = http_put(f"{tgt_base}editables/Sample/addOrEdit", tgt_key, payload)
            mapping[src_id] = new_sample["id"]

    return mapping

# --------------------- SUPPLIER SYNC ---------------------

def sync_supplier_ids(
    *,
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
    src_project_id: int,
    src_process_id: int,
    tgt_project_id: int,
    tgt_process_id: int,
    pc_mapping: dict,
    material_mapping: dict,
):
    supplier_cache = {"by_id": {}, "by_name": {}}

    def _ensure_list(value):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except Exception:
                return None
        return value if isinstance(value, list) else None

    # Process Components
    pc_stats = {"updated": 0, "unchanged": 0, "skipped": 0}
    if pc_mapping:
        logger.info("Syncing SupplierId for %s ProcessComponents", len(pc_mapping))
    else:
        logger.info("No ProcessComponents mapped for SupplierId sync")

    for src_id, tgt_id in (pc_mapping or {}).items():
        src_full = http_get(individual_record_url(src_base, "ProcessComponent", src_id), src_key)
        src_full = validate_target_scope(src_full, src_project_id, src_process_id, "source ProcessComponent")
        if not src_full:
            pc_stats["skipped"] += 1
            logger.info("Skipping ProcessComponent source id %s; out of scope or missing", src_id)
            continue
        if is_archived(src_full):
            pc_stats["skipped"] += 1
            logger.info("Skipping archived ProcessComponent '%s' (%s)", src_full.get("name"), src_id)
            continue

        src_supplier_id = src_full.get("SupplierId")
        if not src_supplier_id:
            pc_stats["skipped"] += 1
            logger.info("ProcessComponent '%s' (%s) has no source SupplierId - skipping", src_full.get("name"), src_id)
            continue

        tgt_supplier_id = resolve_target_supplier_id(
            src_base,
            tgt_base,
            src_key,
            tgt_key,
            src_supplier_id,
            supplier_cache,
        )
        if not tgt_supplier_id:
            pc_stats["skipped"] += 1
            logger.warning(
                "Skipping ProcessComponent '%s' (%s); unable to map source SupplierId %s",
                src_full.get("name"),
                src_id,
                src_supplier_id,
            )
            continue

        tgt_full = http_get(individual_record_url(tgt_base, "ProcessComponent", tgt_id), tgt_key)
        tgt_full = validate_target_scope(tgt_full, tgt_project_id, tgt_process_id, "ProcessComponent")
        if not tgt_full:
            pc_stats["skipped"] += 1
            logger.info("Skipping target ProcessComponent id %s; out of scope or missing", tgt_id)
            continue
        if is_archived(tgt_full):
            pc_stats["skipped"] += 1
            logger.info("Skipping archived target ProcessComponent '%s' (%s)", tgt_full.get("name"), tgt_id)
            continue

        tgt_full = ensure_full_record("ProcessComponent", tgt_full, tgt_base, tgt_key)

        if tgt_full.get("SupplierId") == tgt_supplier_id:
            pc_stats["unchanged"] += 1
            logger.info(
                "ProcessComponent '%s' (%s) SupplierId unchanged (%s) - skipping",
                tgt_full.get("name"),
                tgt_id,
                tgt_supplier_id,
            )
            continue

        steps = _ensure_list(tgt_full.get("Steps"))
        uos = _ensure_list(tgt_full.get("UnitOperations"))
        if steps is None or uos is None:
            pc_stats["skipped"] += 1
            logger.warning(
                "Skipping ProcessComponent '%s' (%s) supplier update; missing Steps/UnitOperations",
                tgt_full.get("name"),
                tgt_id,
            )
            continue

        name_val = tgt_full.get("name") or src_full.get("name")
        type_val = tgt_full.get("type") or src_full.get("type")
        if not name_val or not type_val:
            pc_stats["skipped"] += 1
            logger.warning(
                "Skipping ProcessComponent '%s' (%s) supplier update; missing name/type",
                tgt_full.get("name"),
                tgt_id,
            )
            continue

        payload = {
            "id": tgt_id,
            "name": name_val,
            "type": type_val,
            "ProcessId": tgt_process_id,
            "ProjectId": tgt_project_id,
            "SupplierId": tgt_supplier_id,
            "LastVersionId": tgt_full.get("LastVersionId"),
            "Steps": steps,
            "UnitOperations": uos,
        }

        logger.info(
            "Updating ProcessComponent '%s' (%s) SupplierId -> %s",
            name_val,
            tgt_id,
            tgt_supplier_id,
        )
        http_put(f"{tgt_base}editables/ProcessComponent/addOrEdit", tgt_key, payload)
        pc_stats["updated"] += 1

    # Materials
    material_stats = {"updated": 0, "unchanged": 0, "skipped": 0}
    if material_mapping:
        logger.info("Syncing SupplierId for %s Materials", len(material_mapping))
    else:
        logger.info("No Materials mapped for SupplierId sync")

    for src_id, tgt_id in (material_mapping or {}).items():
        src_full = http_get(individual_record_url(src_base, "Material", src_id), src_key)
        src_full = validate_target_scope(src_full, src_project_id, src_process_id, "source Material")
        if not src_full:
            material_stats["skipped"] += 1
            logger.info("Skipping Material source id %s; out of scope or missing", src_id)
            continue
        if is_archived(src_full):
            material_stats["skipped"] += 1
            logger.info("Skipping archived Material '%s' (%s)", src_full.get("name"), src_id)
            continue

        src_supplier_id = src_full.get("SupplierId")
        if not src_supplier_id:
            material_stats["skipped"] += 1
            logger.info("Material '%s' (%s) has no source SupplierId - skipping", src_full.get("name"), src_id)
            continue

        tgt_supplier_id = resolve_target_supplier_id(
            src_base,
            tgt_base,
            src_key,
            tgt_key,
            src_supplier_id,
            supplier_cache,
        )
        if not tgt_supplier_id:
            material_stats["skipped"] += 1
            logger.warning(
                "Skipping Material '%s' (%s); unable to map source SupplierId %s",
                src_full.get("name"),
                src_id,
                src_supplier_id,
            )
            continue

        tgt_full = http_get(individual_record_url(tgt_base, "Material", tgt_id), tgt_key)
        tgt_full = validate_target_scope(tgt_full, tgt_project_id, tgt_process_id, "Material")
        if not tgt_full:
            material_stats["skipped"] += 1
            logger.info("Skipping target Material id %s; out of scope or missing", tgt_id)
            continue
        if is_archived(tgt_full):
            material_stats["skipped"] += 1
            logger.info("Skipping archived target Material '%s' (%s)", tgt_full.get("name"), tgt_id)
            continue

        tgt_full = ensure_full_record("Material", tgt_full, tgt_base, tgt_key)

        if tgt_full.get("SupplierId") == tgt_supplier_id:
            material_stats["unchanged"] += 1
            logger.info(
                "Material '%s' (%s) SupplierId unchanged (%s) - skipping",
                tgt_full.get("name"),
                tgt_id,
                tgt_supplier_id,
            )
            continue

        steps = _ensure_list(tgt_full.get("Steps"))
        uos = _ensure_list(tgt_full.get("UnitOperations"))
        flows = _ensure_list(tgt_full.get("MaterialFlows"))
        if steps is None or uos is None or flows is None:
            material_stats["skipped"] += 1
            logger.warning(
                "Skipping Material '%s' (%s) supplier update; missing Steps/UnitOperations/MaterialFlows",
                tgt_full.get("name"),
                tgt_id,
            )
            continue

        name_val = tgt_full.get("name") or src_full.get("name")
        category_val = tgt_full.get("category") or src_full.get("category")
        use_val = tgt_full.get("use") or src_full.get("use")
        if not name_val or category_val is None or use_val is None:
            material_stats["skipped"] += 1
            logger.warning(
                "Skipping Material '%s' (%s) supplier update; missing name/category/use",
                tgt_full.get("name"),
                tgt_id,
            )
            continue

        payload = {
            "id": tgt_id,
            "name": name_val,
            "category": category_val,
            "use": use_val,
            "ProcessId": tgt_process_id,
            "SupplierId": tgt_supplier_id,
            "LastVersionId": tgt_full.get("LastVersionId"),
            "Steps": steps,
            "UnitOperations": uos,
            "MaterialFlows": flows,
        }

        logger.info(
            "Updating Material '%s' (%s) SupplierId -> %s",
            name_val,
            tgt_id,
            tgt_supplier_id,
        )
        http_put(f"{tgt_base}editables/Material/addOrEdit", tgt_key, payload)
        material_stats["updated"] += 1
# --------------------- MAIN COPY LOGIC ---------------------
def copy_process():
    src_base = make_base_url(SRC_HOST, SRC_BASE_PATH)
    tgt_base = make_base_url(TGT_HOST, TGT_BASE_PATH)

    # Load existing ID map
    id_map = load_id_map()
    process_map = id_map.setdefault("processes", {})
    proc_entry = process_map.setdefault(str(SRC_PROCESS_ID), {})


    try:

        # Fetch and copy Process
        src_process = http_get(individual_record_url(src_base, "Process", SRC_PROCESS_ID), SRC_KEY)
        if is_archived(src_process):
            logger.info("Source process %s is archived; skipping copy", SRC_PROCESS_ID)
            return

        if src_process.get("ProjectId") != SRC_PROJECT_ID:
            raise ValueError(f"Source process {SRC_PROCESS_ID} does not belong to project {SRC_PROJECT_ID}")

        payload = sanitize_payload(src_process, ALLOWED_PROCESS_FIELDS, {"ProjectId": TGT_PROJECT_ID})

        # Check if process exists in target by previous mapping or by name
        tgt_process_id = proc_entry.get("targetProcessId")
        tgt_process_obj = None
        if tgt_process_id:
            tgt_process_obj = http_get(individual_record_url(tgt_base, "Process", tgt_process_id), TGT_KEY)
        else:
            resp = requests.get(f"{tgt_base}editables/Process/list/{TGT_PROJECT_ID}", headers=headers(TGT_KEY))
            resp.raise_for_status()
            tgt_processes = resp.json().get("instances", [])
            tgt_process_obj = next((p for p in tgt_processes if p.get("name") == payload["name"]), None)
            if tgt_process_obj:
                tgt_process_id = tgt_process_obj["id"]
                # Fetch full process record for accurate diffing
                tgt_process_obj = http_get(individual_record_url(tgt_base, "Process", tgt_process_id), TGT_KEY)

        # Create or update process
        if tgt_process_obj:
            LOG_PATH = setup_logging(tgt_process_id)
            logger.info("Log file: %s", LOG_PATH)

            # Normalize both sides for comparison (handles JSON strings and empty values)
            changed_fields = []
            for f in ALLOWED_PROCESS_FIELDS:
                if param_changed(payload.get(f), tgt_process_obj.get(f)):
                    changed_fields.append(f)
            if changed_fields:
                # check ormalized values to avoid false updates
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
                    http_put(f"{tgt_base}editables/Process/addOrEdit", TGT_KEY, payload)
            else:
                logger.info("Process '%s' unchanged - skipping update", payload["name"])
        else:
            LOG_PATH = setup_logging(tgt_process_id)
            logger.info("Log file: %s", LOG_PATH)

            logger.info("Creating Process '%s'", payload["name"])
            new_proc = http_put(f"{tgt_base}editables/Process/addOrEdit", TGT_KEY, payload)
            tgt_process_id = new_proc["id"]
            logger.info("Created Process '%s': %s -> %s", payload["name"], SRC_PROCESS_ID, tgt_process_id)

        # Save process mapping
        proc_entry["targetProcessId"] = tgt_process_id
    
        # Copy UnitOperations
        uo_mapping = copy_unit_operations(
            src_base, tgt_base, SRC_KEY, TGT_KEY,
            SRC_PROJECT_ID, SRC_PROCESS_ID, TGT_PROJECT_ID, tgt_process_id,
            prev_mapping=proc_entry.get("unitOperations", {}),
        )

        proc_entry["unitOperations"] = {str(k): v for k, v in uo_mapping.items()}

           # Fetch fresh target process
        tgt_process = http_get(individual_record_url(tgt_base, "Process", tgt_process_id), TGT_KEY)

        # Sync UnitOperation order 
        sync_unit_operation_order(
            src_process=src_process,
            tgt_process=tgt_process,
            uo_mapping=uo_mapping,
            put_process_fn=lambda payload: http_put(
                f"{tgt_base}editables/Process/addOrEdit",
                TGT_KEY,
                payload
            ),
        )

        # Copy Steps
        step_mapping = copy_steps(
            src_base, tgt_base, SRC_KEY, TGT_KEY,
            SRC_PROJECT_ID, SRC_PROCESS_ID, TGT_PROJECT_ID, tgt_process_id,
            uo_mapping,
            prev_mapping=proc_entry.get("steps", {}),
        )

        proc_entry["steps"] = {str(k): v for k, v in step_mapping.items()}

        # Sync Step order
        sync_step_order(
            src_base,
            tgt_base,
            SRC_KEY,
            TGT_KEY,
            uo_mapping,
            step_mapping,
        )

        # Copy Process Components 
        pc_mapping = copy_process_components(
            src_base, tgt_base, SRC_KEY, TGT_KEY,
            SRC_PROJECT_ID, SRC_PROCESS_ID, TGT_PROJECT_ID, tgt_process_id,
            uo_mapping, step_mapping,
            prev_mapping=proc_entry.get("processComponents", {}),
        )

        proc_entry["processComponents"] = {str(k): v for k, v in pc_mapping.items()}

        # Copy Materials
        material_mapping = copy_materials(
            src_base, tgt_base, SRC_KEY, TGT_KEY,
            SRC_PROJECT_ID, SRC_PROCESS_ID,
            TGT_PROJECT_ID, tgt_process_id,
            uo_mapping, step_mapping,
            prev_mapping=proc_entry.get("materials", {}),
        )
        proc_entry["materials"] = {str(k): v for k, v in material_mapping.items()}

         # Copy Material Attributes
        material_attribute_mapping = copy_material_attributes(
            src_base, tgt_base, SRC_KEY, TGT_KEY,
            SRC_PROJECT_ID, SRC_PROCESS_ID,
            TGT_PROJECT_ID, tgt_process_id,
            uo_mapping, step_mapping, pc_mapping, material_mapping,
            prev_mapping=proc_entry.get("material_attributes", {})
        )
        proc_entry["material_attributes"] = {str(k): v for k, v in material_attribute_mapping.items()}

        # Copy Process Parameters
        pp_mapping = copy_process_parameters(
            src_base, tgt_base, SRC_KEY, TGT_KEY,
            SRC_PROJECT_ID, SRC_PROCESS_ID, TGT_PROJECT_ID, tgt_process_id,
            uo_mapping, step_mapping, pc_mapping, material_mapping,
            prev_mapping=proc_entry.get("processParameters", {}),
        )

        proc_entry["processParameters"] = {str(k): v for k, v in pp_mapping.items()}

        # Copy IQAs
        iqa_mapping = copy_iqas(
            src_base, tgt_base, SRC_KEY, TGT_KEY,
            SRC_PROJECT_ID, SRC_PROCESS_ID,
            TGT_PROJECT_ID, tgt_process_id,
            uo_mapping, step_mapping,
            prev_mapping=proc_entry.get("iqas", {}),
        )

        proc_entry["iqas"] = {str(k): v for k, v in iqa_mapping.items()}

        # Copy IPAs
        ipa_mapping = copy_ipas(
            src_base, tgt_base, SRC_KEY, TGT_KEY,
            SRC_PROJECT_ID, SRC_PROCESS_ID,
            TGT_PROJECT_ID, tgt_process_id,
            uo_mapping, step_mapping,
            prev_mapping=proc_entry.get("ipas", {}),
        )

        proc_entry["ipas"] = {str(k): v for k, v in ipa_mapping.items()}

        # Copy Samples
        sample_mapping = copy_samples(
            src_base, tgt_base, SRC_KEY, TGT_KEY,
            SRC_PROJECT_ID, SRC_PROCESS_ID,
            TGT_PROJECT_ID, tgt_process_id,
            uo_mapping, step_mapping, material_mapping,
            prev_mapping=proc_entry.get("samples", {}),
        )
        proc_entry["samples"] = {str(k): v for k, v in sample_mapping.items()}
        # --------------------- SYNC RISK LINKS AFTER ALL RECORDS ARE CREATED ---------------------
        # Build name lookup for target FPA/FQA (name -> id)
        tgt_fpa_by_name = build_target_lookup(tgt_base, TGT_KEY, TGT_PROJECT_ID, "FPA")
        tgt_fqa_by_name = build_target_lookup(tgt_base, TGT_KEY, TGT_PROJECT_ID, "FQA")

        applies_to_maps = {
            "IQA": iqa_mapping,
            "IPA": ipa_mapping,
            "ProcessParameter": pp_mapping,
            "MaterialAttribute": material_attribute_mapping,
            "ProcessComponent": pc_mapping,
            "Material": material_mapping,
            "Step": step_mapping,
            "UnitOperation": uo_mapping,
            "Sample": sample_mapping,
        }

        # Sync IQA risk links
        sync_risk_links(
            records=iqa_mapping,
            record_type="IQA",
            put_fn=lambda payload: http_put(f"{tgt_base}editables/IQA/addOrEdit", TGT_KEY, payload),
            link_fields={
                "IQAToFPAs": (lambda link: tgt_fpa_by_name.get(link.get("FPA", {}).get("name")), "FPAId"),
                "IQAToFQAs": (lambda link: tgt_fqa_by_name.get(link.get("FQA", {}).get("name")), "FQAId"),
                "IQAToIPAs": (lambda link: ipa_mapping.get(link.get("IPAId")), "IPAId"),
                "IQAToIQAs": {
                    "resolver": lambda link: iqa_mapping.get(link.get("TargetIQAId")),
                    "id_key": "TargetIQAId",
                    "parent_id_key": "IQAId",
                },
            },
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            src_base=src_base,
            tgt_base=tgt_base,
            applies_to_maps=applies_to_maps,
        )

        # Sync IPA risk links
        sync_risk_links(
            records=ipa_mapping,
            record_type="IPA",
            put_fn=lambda payload: http_put(f"{tgt_base}editables/IPA/addOrEdit", TGT_KEY, payload),
            link_fields={
                "IPAToFPAs": (lambda link: tgt_fpa_by_name.get(link.get("FPA", {}).get("name")), "FPAId"),
                "IPAToFQAs": (lambda link: tgt_fqa_by_name.get(link.get("FQA", {}).get("name")), "FQAId"),
                "IPAToIPAs": {
                    "resolver": lambda link: ipa_mapping.get(link.get("TargetIPAId")),
                    "id_key": "TargetIPAId",
                    "parent_id_key": "IPAId",
                },
                "IPAToIQAs": (lambda link: iqa_mapping.get(link.get("IQAId")), "IQAId"),
            },
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            src_base=src_base,
            tgt_base=tgt_base,
            applies_to_maps=applies_to_maps,
        )

        # Sync MaterialAttribute risk links
        sync_risk_links(
            records=material_attribute_mapping,
            record_type="MaterialAttribute",
            put_fn=lambda payload: http_put(f"{tgt_base}editables/MaterialAttribute/addOrEdit", TGT_KEY, payload),
            link_fields={
                "MaterialAttributeToFPAs": (lambda link: tgt_fpa_by_name.get(link.get("FPA", {}).get("name")), "FPAId"),
                "MaterialAttributeToFQAs": (lambda link: tgt_fqa_by_name.get(link.get("FQA", {}).get("name")), "FQAId"),
                "MaterialAttributeToIPAs": (lambda link: ipa_mapping.get(link.get("IPAId")), "IPAId"),
                "MaterialAttributeToIQAs": (lambda link: iqa_mapping.get(link.get("IQAId")), "IQAId"),
            },
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            src_base=src_base,
            tgt_base=tgt_base,
            applies_to_maps=applies_to_maps,
        )
        # Sync Process Parameter risk links 
        sync_risk_links(
            records=pp_mapping,
            record_type="ProcessParameter",
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            src_base=src_base,
            tgt_base=tgt_base,
            put_fn=lambda payload: http_put(
                f"{tgt_base}editables/ProcessParameter/addOrEdit",
                TGT_KEY,
                payload,
            ),
            link_fields={
                "ProcessParameterToFPAs": (
                    lambda link: tgt_fpa_by_name.get(link.get("FPA", {}).get("name")),
                    "FPAId",
                ),
                "ProcessParameterToFQAs": (
                    lambda link: tgt_fqa_by_name.get(link.get("FQA", {}).get("name")),
                    "FQAId",
                ),
                "ProcessParameterToIPAs": (
                    lambda link: ipa_mapping.get(link.get("IPAId")),
                    "IPAId",
                ),
                "ProcessParameterToIQAs": (
                    lambda link: iqa_mapping.get(link.get("IQAId")),
                    "IQAId",
                ),
            },
            applies_to_maps=applies_to_maps,
        )
        # Sync Sample relationship links - they arent risks but the structure is similar
        sync_risk_links(
            records=sample_mapping,
            record_type="Sample",
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            src_base=src_base,
            tgt_base=tgt_base,
            put_fn=lambda payload: http_put(
                f"{tgt_base}editables/Sample/addOrEdit",
                TGT_KEY,
                payload,
            ),
            link_fields={
                "SampleToIQAs": (
                    lambda link: iqa_mapping.get(link.get("IQAId")),
                    "IQAId",
                ),
                "SampleToMaterialAttributes": (
                    lambda link: material_attribute_mapping.get(link.get("MaterialAttributeId")),
                    "MaterialAttributeId",
                ),
                "SampleToProcessParameters": (
                    lambda link: pp_mapping.get(link.get("ProcessParameterId")),
                    "ProcessParameterId",
                ),
                "SampleToIPAs": (
                    lambda link: ipa_mapping.get(link.get("IPAId")),
                    "IPAId",
                ),
            },
            applies_to_maps=applies_to_maps,
        )
    # --------------------- SYNC DRUG SUBSTANCE / DRUG PRODUCT FLOWS ---------------------
        # Fetch all source DrugSubstances
        src_url_ds = f"{src_base}editables/DrugSubstance/list/{SRC_PROJECT_ID}"
        src_drug_substances = {
            ds["id"]: ds
            for ds in http_get(src_url_ds, SRC_KEY).get("instances", [])
        }

        # Fetch all target DrugSubstances
        tgt_url_ds = f"{tgt_base}editables/DrugSubstance/list/{TGT_PROJECT_ID}"
        tgt_ds_by_name = {
            ds["name"]: ds["id"]
            for ds in http_get(tgt_url_ds, TGT_KEY).get("instances", [])
        }

        # Fetch all source DrugProducts
        src_url_dp = f"{src_base}editables/DrugProduct/list/{SRC_PROJECT_ID}"
        src_drug_products = {
            dp["id"]: dp
            for dp in http_get(src_url_dp, SRC_KEY).get("instances", [])
        }

        # Fetch all target DrugProducts
        tgt_url_dp = f"{tgt_base}editables/DrugProduct/list/{TGT_PROJECT_ID}"
        tgt_dp_by_name = {
            dp["name"]: dp["id"]
            for dp in http_get(tgt_url_dp, TGT_KEY).get("instances", [])
        }

        #  Build mapping by name
        drug_substance_mapping = {
            src_id: tgt_ds_by_name.get(src_rec["name"])
            for src_id, src_rec in src_drug_substances.items()
            if src_rec.get("name") in tgt_ds_by_name
        }

        drug_product_mapping = {
            src_id: tgt_dp_by_name.get(src_rec["name"])
            for src_id, src_rec in src_drug_products.items()
            if src_rec.get("name") in tgt_dp_by_name
        }

        # Sync DrugSubstance flows
        sync_drug_flows(
            records=drug_substance_mapping,
            record_type="DrugSubstance",
            flow_field="DrugSubstanceFlows",
            src_base=src_base,
            tgt_base=tgt_base,
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            tgt_project_id=TGT_PROJECT_ID,
            src_process_id=SRC_PROCESS_ID,
            tgt_process_id=tgt_process_id,
            uo_mapping=uo_mapping,
            step_mapping=step_mapping,
        )

        #  Sync DrugProduct flows
        sync_drug_flows(
            records=drug_product_mapping,
            record_type="DrugProduct",
            flow_field="DrugProductFlows",
            src_base=src_base,
            tgt_base=tgt_base,
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            tgt_project_id=TGT_PROJECT_ID,
            src_process_id=SRC_PROCESS_ID,
            tgt_process_id=tgt_process_id,
            uo_mapping=uo_mapping,
            step_mapping=step_mapping,
        )
        # Sync IQA DS/DP links 
        sync_iqa_drug_links(
            iqa_mapping=iqa_mapping,
            drug_substance_mapping=drug_substance_mapping,
            drug_product_mapping=drug_product_mapping,
            src_base=src_base,
            tgt_base=tgt_base,
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            src_project_id=SRC_PROJECT_ID,
            src_process_id=SRC_PROCESS_ID,
        )

        # Sync SupplierId for Materials and Process Components
        sync_supplier_ids(
            src_base=src_base,
            tgt_base=tgt_base,
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            src_project_id=SRC_PROJECT_ID,
            src_process_id=SRC_PROCESS_ID,
            tgt_project_id=TGT_PROJECT_ID,
            tgt_process_id=tgt_process_id,
            pc_mapping=pc_mapping,
            material_mapping=material_mapping,
        )
    finally:
        # Persist mapping
        save_id_map(id_map)
# --------------------- MAIN ---------------------
def main():
    validate_config()
    try:
        copy_process()
    except requests.HTTPError as e:
        logger.error("HTTP error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.error("Response body: %s", e.response.text)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)

if __name__ == "__main__":
    main()
