"""
Project copy tool with TPP, General Attributes, Control Methods, FQA, FPA, DS, and DP mapping.
RMPs are not synced. Logic is: If RMP with same name exists in target, use it. If not, create it, then use it. 
If RMP is created, the script will break and stop after creating the RMP. You must manually approve the RMP to use it, then
you can run this again and it will continue.

We do NOT sync the supplier list between environments, so ControlMethod.SupplierId
is remapped by supplier name (create if missing) much like the RMP.

Smart Content and User fields are not synced

You may end up with more TPPs and GAs in the target if any of the defaults are archived in the source.
This does not handle archiving records in the target.
"""

import requests
import json
import logging
import uuid
import os
import sys
import re
from datetime import datetime
from typing import Any, Dict, Optional
from dotenv import load_dotenv
# --------------------- CONFIG ---------------------
# Load environment variables
load_dotenv()

SRC_PROJECT_ID = os.getenv("SOURCE_PROJECT_ID")
SRC_KEY = os.getenv("SOURCE_KEY")
TGT_KEY = os.getenv("TARGET_KEY")
SRC_HOST = os.getenv("SOURCE_HOST")
SRC_BASE_PATH = os.getenv("SOURCE_BASE_PATH")
TGT_HOST = os.getenv("TARGET_HOST")
TGT_BASE_PATH = os.getenv("TARGET_BASE_PATH")
LOG_DIR = "logs"

def _required_int(name: str, value: str | None) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        print(f"Error: {name} must be a whole number.")
        sys.exit(1)

def validate_config():
    global SRC_PROJECT_ID

    missing = []
    if not SRC_PROJECT_ID:
        missing.append("SOURCE_PROJECT_ID")
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

LOG_PATH = None
logger = logging.getLogger("qbd_copy_project_only")
# --------------------- ALLOWED FIELDS ---------------------
ID_MAP_FILE = "project_id_map.json"
ALLOWED_PROJECT_FIELDS = [
    "name", "customProjectId", "type", "category", "links",
    "purposeAndScope", "objectives", "purposeLinks", "qualityByDesignPhase",
    "cmcPhase", "validationPhase", "dosageForm", "routeOfAdministration",
    "regulatoryPath", "regulatoryPhase", "referenceListedDrug",
    "productRiskAssessmentType", "riskAssessmentMethod",
    "manufacturingLeadId", "RMPId"
]
PROJECT_SYNC_FIELDS = [
    f for f in ALLOWED_PROJECT_FIELDS
    if f != "RMPId"
]
ALLOWED_FPA_FIELDS = [
    "name", "scope", "type", "category", "description", "GeneralAttributes", "TPPSections",
    "FPAToGeneralAttributeRisks", "recommendedActions", "criticalityJustification", "riskLinks", "dataSpace", "measure", "measurementUnits",
    "group", "label", "lowerLimit", "target", "upperLimit", "targetJustification", "ControlMethods", "samplingPlan",
    "stabilityIndicating", "acceptanceCriteriaLinks", "AcceptanceCriteriaRanges", "estimatedSampleSize", "capabilityRisk", "capabilityJustification",
    "detectabilityRisk", "detectabilityJustification", "controlStrategy", "ccp",
    "controlStrategyJustification", "riskControlLinks", "referencesLinks",
]
ALLOWED_FQA_FIELDS = [
    "name", "type", "scope", "category", "description", "GeneralAttributes", "TPPSections",
    "FQAToGeneralAttributeRisks", "recommendedActions", "criticalityJustification", "riskLinks",
    "dataSpace", "measure", "group", "label", "lowerLimit",
    "target", "upperLimit", "targetJustification", "measurementUnits",
    "ControlMethods", "samplingPlan", "stabilityIndicating", "acceptanceCriteriaLinks", "AcceptanceCriteriaRanges",
    "estimatedSampleSize", "capabilityRisk", "capabilityJustification", "detectabilityRisk", "detectabilityJustification",
    "controlStrategy", "ccp", "riskControlLinks", "controlStrategyJustification",
    "referencesLinks",
]
NON_RISKRANKING_FPA_FQA_FIELDS = [
    "name", "type", "scope", "category", "description", "GeneralAttributes", "TPPSections",
    "recommendedActions", "criticalityJustification", "acceptanceCriteriaLinks", "riskLinks", "riskControlLinks",
    "referencesLinks", "dataSpace", "measure", "group", "label", "lowerLimit", "target", "upperLimit",
    "measurementUnits", "targetJustification", "ControlMethods", "samplingPlan",
    "stabilityIndicating", "estimatedSampleSize", "controlStrategy", "ccp", "controlStrategyJustification",
    "AcceptanceCriteriaRanges", "impact", "uncertainty", "riskAssessmentMethod",
    "capabilityRisk", "detectabilityRisk", "processRisk", "RPN",
]
NON_RISKRANKING_RELATION_FIELDS = ("TPPSections", "GeneralAttributes", "ControlMethods")
ACR_FIELDS = [
    "group", "label", "isDefault", "lowerLimit",
    "target", "upperLimit", "measurementUnits", "targetJustification",
]
ALLOWED_TPP_FIELDS = [
    "name", "target", "annotations", "comments", "links"
    ]
ALLOWED_GENERAL_ATTRIBUTE_FIELDS = [
    "name", "target", "links"
    ]
ALLOWED_CONTROL_METHOD_FIELDS = [
    "name", "type", "category", "compendialStandard", "internalId",
    "SupplierId", "description", "equipment", "controlMethodLinks", "status",
    "stabilityIndicating", "developmentLinks", "referencesLinks"
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
ALLOWED_DRUG_SUBSTANCE_FIELDS = [
    "name", "ctdFiledName", "description", "internalPartNumber",
    "links", "drugSubstanceType", "form", "empiricalFormula", "density", "densityConditions", 
    "chemicalStructure", "molecularWeight", "molecularFormula",
    "chemicalNameCAS", "chemicalNameIUPAC", "otherNames", "innUsan",
    "casRegistryNumber", "compendialStandard", "certificateOfAnalysis",
    "propertiesLinks", "referencesLinks", "DrugSubstanceToFQAs"
]
ALLOWED_DRUG_PRODUCT_FIELDS = [
    "name", "ctdFiledName", "description", "internalPartNumber",
    "links", "form", "empiricalFormula", "density", "densityConditions", 
    "chemicalStructure", "molecularWeight", "chemicalNameCAS", "chemicalNameIUPAC", "otherNames", "innUsan",
    "casRegistryNumber", "compendialStandard", "certificateOfAnalysis",
    "propertiesLinks", "referencesLinks", "DrugProductToFQAs"
]
ENTITY_CONFIG = {
    "Project": {
        "endpoint": "editables/Project",
        "allowed_fields": ALLOWED_PROJECT_FIELDS,
        "diff_fields": PROJECT_SYNC_FIELDS,
        "sync_fields": PROJECT_SYNC_FIELDS,
        "remap": None,
    },

    "TPPSection": {
        "endpoint": "editables/TPPSection",
        "allowed_fields": ALLOWED_TPP_FIELDS,
        "diff_fields": ALLOWED_TPP_FIELDS,
        "remap": None,
    },
    "GeneralAttribute": {
        "endpoint": "editables/GeneralAttribute",
        "allowed_fields": ALLOWED_GENERAL_ATTRIBUTE_FIELDS,
        "diff_fields": ALLOWED_GENERAL_ATTRIBUTE_FIELDS,
        "remap": None,
    },
    "ControlMethod": {
        "endpoint": "editables/ControlMethod",
        "allowed_fields": ALLOWED_CONTROL_METHOD_FIELDS,
        "diff_fields": ALLOWED_CONTROL_METHOD_FIELDS,
        "remap": None,
    },
    "FPA": {
        "endpoint": "editables/FPA",
        "allowed_fields": ALLOWED_FPA_FIELDS,
        "diff_fields": [
            f for f in ALLOWED_FPA_FIELDS
            if f not in ("TPPSections", "GeneralAttributes", "FPAToGeneralAttributeRisks", "ControlMethods")
        ],
        "remap": "apply_remap",
    },
    "FQA": {
        "endpoint": "editables/FQA",
        "allowed_fields": ALLOWED_FQA_FIELDS,
        "diff_fields": [
            f for f in ALLOWED_FQA_FIELDS
            if f not in ("TPPSections", "GeneralAttributes", "FQAToGeneralAttributeRisks", "ControlMethods")
        ],
        "remap": "apply_remap",
    },
    "DrugSubstance": {
        "endpoint": "editables/DrugSubstance",
        "allowed_fields": ALLOWED_DRUG_SUBSTANCE_FIELDS,
        "diff_fields": [
            f for f in ALLOWED_DRUG_SUBSTANCE_FIELDS
            if f != "DrugSubstanceToFQAs"
        ],
        "remap": "apply_remap",
    },
    "DrugProduct": {
        "endpoint": "editables/DrugProduct",
        "allowed_fields": ALLOWED_DRUG_PRODUCT_FIELDS,
        "diff_fields": [
            f for f in ALLOWED_DRUG_PRODUCT_FIELDS
            if f != "DrugProductToFQAs"
        ],
        "remap": "apply_remap"
    }
}
# --------------------- FIELDS TO REMOVE (RMP ONLY) ---------------------
RMP_STRIP_FIELDS = {
    "id",
    "majorVersion",
    "minorVersion",
    "clonedFromVersionId",
    "clonedFromModel",
    "createdByUserId",
    "createdAt",
    "updatedAt",
    "deletedAt",
    "RMPId",
    "LastVersionTransitionId",
    "model",
}
RMP_NESTED_ID_FIELDS = [
    "RMPToImpacts",
    "RMPToUncertainties",
    "RMPToCapabilityRisks",
    "RMPToDetectabilityRisks",
    "RMPToCriticalityScales",
    "RMPToProcessRiskScales",
    "RMPToRPNScales",
]
# --------------------- LOGGING ---------------------
def setup_logging(tgt_project_id: int | None = None) -> str:
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if tgt_project_id is None:
        tgt_project_id = "unknown"
    log_path = os.path.join(LOG_DIR, f"copy_project_src{SRC_PROJECT_ID}_tgt{tgt_project_id}_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return log_path
# --------------------- ID MAP PERSISTENCE ---------------------
def load_id_map() -> dict:
    try:
        with open(ID_MAP_FILE) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"projects": {}}

    if "projects" in data and isinstance(data["projects"], dict):
        return {"projects": data["projects"]}
    return {"projects": {}}

def save_id_map(id_map: dict):
    with open(ID_MAP_FILE, "w") as f:
        json.dump(id_map, f, indent=2)
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
    return {"Content-Type": "application/json", "qbdvision-api-key": api_key}

def http_get(url: str, api_key: str) -> Any:
    r = requests.get(url, headers=headers(api_key))
    r.raise_for_status()
    return r.json() if r.text else {}

def http_put(url: str, api_key: str, payload: Dict[str, Any]) -> Any:
    r = requests.put(url, headers=headers(api_key), json=payload)
    r.raise_for_status()
    return r.json() if r.text else {}

def individual_record_url(base: str, entity_type: str, entity_id: int) -> str:
    return f"{base}editables/{entity_type}/{entity_id}?approved=false"
# --------------------- LINK & PAYLOAD HELPERS ---------------------
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

def clean_rmp_payload(src_rmp: dict) -> dict:
    payload = {}

    for k, v in src_rmp.items():
        if k in RMP_STRIP_FIELDS:
            continue

        if k in RMP_NESTED_ID_FIELDS and isinstance(v, list):
            cleaned_items = []
            for item in v:
                if not isinstance(item, dict):
                    continue

                cleaned = {
                    ik: iv
                    for ik, iv in item.items()
                    if ik not in {
                        "id",
                        "createdByUserId",
                        "createdAt",
                        "updatedAt",
                        "deletedAt",
                        "RMPVersionId",
                        "RMPVersion",
                    }
                }
                cleaned_items.append(cleaned)

            payload[k] = cleaned_items
        else:
            payload[k] = v

    return payload

def sanitize_payload(payload: Dict[str, Any], allowed_fields: list, extra_fields: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    sanitized = {k: payload[k] for k in allowed_fields if k in payload}
    if extra_fields:
        sanitized.update(extra_fields)
    return sanitized

def is_archived(record: dict) -> bool:
    return isinstance(record, dict) and record.get("currentState") == "Archived"

def validate_target_scope(record: dict | None, project_id: int, label: str) -> dict | None:
    if not record or not isinstance(record, dict):
        return None

    rec_project = record.get("ProjectId") or record.get("projectId")
    if rec_project is not None and rec_project != project_id:
        logger.warning(
            "Ignoring %s id %s from project %s (expected %s)",
            label,
            record.get("id"),
            rec_project,
            project_id,
        )
        return None

    return record

def _normalize_risk_assessment_method(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[^A-Za-z0-9]+", "", str(value)).lower()

def _resolve_project_risk_model(remap_ctx: dict | None) -> str:
    ctx = remap_ctx or {}
    candidates = [
        ctx.get("target_project_risk_assessment_method"),
        ctx.get("target_project_product_risk_assessment_type"),
        ctx.get("project_risk_assessment_method"),
        ctx.get("project_product_risk_assessment_type"),
    ]

    for candidate in candidates:
        normalized = _normalize_risk_assessment_method(candidate)
        if normalized == "riskranking":
            return "riskranking"
        if normalized == "classification":
            return "classification"
        if normalized in ("pha", "preliminaryhazardsanalysis", "preliminaryhazardsanalysispha"):
            return "pha"

    return ""

def is_risk_ranking_method(remap_ctx: dict | None) -> bool:
    return _resolve_project_risk_model(remap_ctx) == "riskranking"

def is_classification_method(remap_ctx: dict | None) -> bool:
    return _resolve_project_risk_model(remap_ctx) == "classification"

def resolve_non_riskranking_method(src_full: dict, remap_ctx: dict | None) -> str | None:
    project_model = _resolve_project_risk_model(remap_ctx)
    if project_model == "classification":
        return "Classification"
    if project_model == "pha":
        return "PHA"

    source_project = src_full.get("project") if isinstance(src_full, dict) else None
    if not isinstance(source_project, dict):
        source_project = {}

    candidates = [
        src_full.get("riskAssessmentMethod") if isinstance(src_full, dict) else None,
        source_project.get("riskAssessmentMethod"),
        source_project.get("productRiskAssessmentType"),
    ]

    for candidate in candidates:
        normalized = _normalize_risk_assessment_method(candidate)
        if normalized == "classification":
            return "Classification"
        if normalized in ("pha", "preliminaryhazardsanalysis", "preliminaryhazardsanalysispha"):
            return "PHA"
        if normalized == "riskranking":
            return "RiskRanking"

    return None

def get_effective_entity_fields(
    entity_type: str,
    cfg: dict,
    remap_ctx: dict | None = None,
) -> tuple[list, list, list]:
    allowed_fields = list(cfg["allowed_fields"])
    diff_fields_cfg = list(cfg["diff_fields"])
    sync_fields_cfg = list(cfg.get("sync_fields", allowed_fields))

    if entity_type in ("FPA", "FQA") and not is_risk_ranking_method(remap_ctx):
        allowed_fields = list(NON_RISKRANKING_FPA_FQA_FIELDS)
        diff_fields_cfg = [
            f for f in allowed_fields
            if f not in NON_RISKRANKING_RELATION_FIELDS
        ]
        sync_fields_cfg = list(allowed_fields)

    return allowed_fields, diff_fields_cfg, sync_fields_cfg

def _risk_info_value(src_full: dict, section_name: str) -> Any:
    if not isinstance(src_full, dict):
        return None
    risk_info = src_full.get("riskInfo")
    if not isinstance(risk_info, dict):
        return None
    section = risk_info.get(section_name)
    if not isinstance(section, dict):
        return None
    return section.get("value")

def apply_non_riskranking_risk_values(src_full: dict, payload: dict) -> dict:
    if not isinstance(payload, dict):
        return payload

    capability = src_full.get("capabilityRisk") if isinstance(src_full, dict) else None
    if capability is None:
        capability = _risk_info_value(src_full, "Capability Risk")
    payload["capabilityRisk"] = capability

    detectability = src_full.get("detectabilityRisk") if isinstance(src_full, dict) else None
    if detectability is None:
        detectability = _risk_info_value(src_full, "Detectability Risk")
    payload["detectabilityRisk"] = detectability

    process_risk = None
    if isinstance(src_full, dict):
        if "processRisk" in src_full:
            process_risk = src_full.get("processRisk")
        elif "ProcessRisk" in src_full:
            process_risk = src_full.get("ProcessRisk")
    if process_risk is None:
        process_risk = _risk_info_value(src_full, "Process Risk")
    payload["processRisk"] = process_risk

    rpn_value = None
    if isinstance(src_full, dict):
        if "RPN" in src_full:
            rpn_value = src_full.get("RPN")
        elif "rpn" in src_full:
            rpn_value = src_full.get("rpn")
    if rpn_value is None:
        rpn_value = _risk_info_value(src_full, "RPN")
    payload["RPN"] = rpn_value

    return payload
# --------------------- FETCH & DIFF HELPERS ---------------------
def fetch_full_editable(base: str, key: str, entity_type: str, entity_id: int) -> dict:
    return http_get(
        individual_record_url(base, entity_type, entity_id),
        key
    )

_WHITESPACE_RE = re.compile(r"\s+")

def _normalize_whitespace(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", value).strip()

def _normalize_value_for_compare(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return _normalize_whitespace(value)
    if isinstance(value, list):
        return [_normalize_value_for_compare(v) for v in value]
    if isinstance(value, dict):
        return {k: _normalize_value_for_compare(v) for k, v in value.items()}
    return value

def _values_equal_ignoring_whitespace(a: Any, b: Any) -> bool:
    if isinstance(a, str) and b is None:
        return _normalize_whitespace(a) == ""
    if isinstance(b, str) and a is None:
        return _normalize_whitespace(b) == ""
    return _normalize_value_for_compare(a) == _normalize_value_for_compare(b)

def _preserve_whitespace_only_changes(src_payload: dict, tgt_payload: dict, fields: list) -> dict:
    if not isinstance(tgt_payload, dict):
        return src_payload
    for field in fields:
        if field in src_payload:
            tgt_val = tgt_payload.get(field)
            if _values_equal_ignoring_whitespace(src_payload[field], tgt_val):
                src_payload[field] = tgt_val
    return src_payload

def diff_fields(
    src: dict,
    tgt: dict,
    fields: list
) -> Dict[str, Dict[str, Any]]:
    """
    Returns:
    {
      "fieldName": { "from": old, "to": new }
    }
    """
    diffs = {}

    for field in fields:
        src_val = src.get(field)
        tgt_val = tgt.get(field)

        if _values_equal_ignoring_whitespace(src_val, tgt_val):
            continue

        if src_val != tgt_val:
            diffs[field] = {
                "from": tgt_val,
                "to": src_val
            }

    return diffs
# --------------------- RMP HELPERS ---------------------
def get_target_rmp_by_name(base_url, api_key, name):
    url = f"{base_url}editables/RMP/list"
    data = http_get(url, api_key)

    for rmp in data.get("instances", []):
        if (
            rmp.get("name") == name
            and rmp.get("currentState") == "Approved"
        ):
            return rmp["id"]

    return None

def create_target_rmp(tgt_base, tgt_key, src_rmp: dict) -> int:
    """
    Creates an RMP in target using a cleaned source payload.
    Returns target RMP id.
    """
    cleaned_payload = clean_rmp_payload(src_rmp)

    # make sure name is present
    if not cleaned_payload.get("name"):
        raise ValueError("RMP payload missing name")

    rmp = http_put(
        f"{tgt_base}editables/RMP/addOrEdit",
        tgt_key,
        cleaned_payload
    )

    return rmp["id"]
# --------------------- SUPPLIER HELPERS ---------------------
def get_target_supplier_by_name(base_url, api_key, name):
    url = f"{base_url}editables/Supplier/list"
    data = http_get(url, api_key)

    suppliers = data.get("instances") if isinstance(data, dict) else data
    if not isinstance(suppliers, list):
        return None

    for supplier in suppliers:
        if (
            isinstance(supplier, dict)
            and supplier.get("name") == name
            and not is_archived(supplier)
        ):
            return supplier.get("id")

    return None

def clean_supplier_payload(src_supplier: dict) -> dict:
    payload = sanitize_payload(src_supplier, ALLOWED_SUPPLIER_FIELDS)
    return strip_attachment_links(payload)

def create_target_supplier(tgt_base, tgt_key, src_supplier: dict) -> int:
    # Creates a Supplier in target using a cleaned source payload.
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
    if is_archived(src_supplier):
        logger.info(
            "Skipping archived Supplier '%s' (%s)",
            src_supplier.get("name"),
            src_supplier_id,
        )
        return None

    name = src_supplier.get("name")
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
# --------------------- ACCEPTANCE CRITERIA ---------------------
def normalize_acceptance_criteria_ranges_list(ranges: list) -> list:
    cleaned = []
    for r in ranges:
        if not isinstance(r, dict):
            continue
        cleaned.append({k: r.get(k) for k in ACR_FIELDS if k in r})

    def _sortable(v):
        if v is None:
            return (0, "")
        return (1, str(v))

    def _key(item):
        return tuple(_sortable(item.get(f)) for f in ACR_FIELDS)

    return sorted(cleaned, key=_key)

def extract_acceptance_criteria_ranges_from_source(src_full: dict) -> list:
    if not isinstance(src_full, dict):
        return []

    req = src_full.get("Requirement")
    if isinstance(req, dict):
        acr = req.get("AcceptanceCriteriaRanges")
        if isinstance(acr, list):
            return acr

    acr = src_full.get("AcceptanceCriteriaRanges")
    if isinstance(acr, list):
        return acr

    rv = src_full.get("RequirementVersion")
    if isinstance(rv, dict):
        linked = rv.get("AcceptanceCriteriaRangeLinkedVersions")
        if isinstance(linked, list):
            return linked

    return []

def build_acceptance_criteria_ranges(src_full: dict) -> list:
    ranges = extract_acceptance_criteria_ranges_from_source(src_full)
    if not ranges:
        return []

    return normalize_acceptance_criteria_ranges_list(ranges)
# --------------------- REMAP HELPERS ---------------------
def remap_tpp_sections(
    tpp_sections: list,
    tpp_id_map: Dict[str, int]
) -> list:
    remapped = []

    for tpp in tpp_sections or []:
        src_id = str(tpp.get("id"))
        tgt_id = tpp_id_map.get(src_id)
        if not tgt_id:
            logger.warning("No target TPP mapping for source TPP %s", src_id)
            continue

        remapped.append({
            "id": tgt_id,
            "typeCode": "TPP"
        })

    return remapped

def remap_general_attributes(
    general_attributes: list,
    ga_id_map: Dict[str, int],
) -> list:
    remapped = []

    for ga in general_attributes or []:
        src_id = str(ga.get("id"))
        tgt_id = ga_id_map.get(src_id)
        if not tgt_id:
            logger.warning("No target GA mapping for source GA %s", src_id)
            continue

        remapped.append({
            "id": tgt_id,
            "typeCode": "GA"
        })

    return remapped

def remap_general_attribute_risks(
    risks: list,
    ga_id_map: Dict[str, int],
    tgt_ga_name_map: Dict[int, str],
    source:str,
) -> list:
    """
    Build GA risk objects in the exact shape required by FPA/FQA addOrEdit.
    """
    remapped = []

    for r in risks or []:
        src_ga_id = r.get("GeneralAttributeId")
        if not src_ga_id:
            continue

        tgt_ga_id = ga_id_map.get(str(src_ga_id))
        if not tgt_ga_id:
            logger.warning(
                "No target GA mapping for source GA %s", src_ga_id
            )
            continue

        remapped.append({
            "uuid": str(uuid.uuid4()),
            "impact": r.get("impact"),
            "uncertainty": r.get("uncertainty"),
            "justification": r.get("justification"),
            "links": "[]",
            "typeCode": "GA",
            "source": source,
            "GeneralAttributeId": tgt_ga_id,
            "GeneralAttribute": {
                "id": tgt_ga_id,
                "name": tgt_ga_name_map.get(tgt_ga_id, ""),
                "typeCode": "GA"
            }
        })

    return remapped

def remap_control_methods(
    cms: list,
    cm_id_map: Dict[str, int]
) -> list:
    remapped = []

    for cm in cms or []:
        src_cm_id = str(cm.get("id"))
        tgt_cm_id = cm_id_map.get(src_cm_id)
        if not tgt_cm_id:
            logger.warning("No target ControlMethod mapping for source CM %s", src_cm_id)
            continue

        remapped.append({
            "id": tgt_cm_id,
            "typeCode": "CM"
        })

    return remapped

def remap_fqa_links(
    payload: dict,
    field_name: str,
    fqa_id_map: Dict[str, int],
) -> dict:
    """
    Generic remapper for *ToFQAs relationships in DS and DP objects
    Only remaps FQAId using source ??? target FQA ID map.
    """
    remapped = []

    for link in payload.get(field_name, []) or []:
        src_fqa_id = link.get("FQAId")
        if not src_fqa_id:
            continue

        tgt_fqa_id = fqa_id_map.get(str(src_fqa_id))
        if not tgt_fqa_id:
            logger.warning(
                "No target FQA mapping for source FQA %s (%s link skipped)",
                src_fqa_id,
                field_name
            )
            continue

        remapped.append({
            "FQAId": tgt_fqa_id
        })

    payload[field_name] = remapped
    return payload

def apply_remap(entity_type: str, payload: dict, ctx: dict) -> dict:
    if entity_type in ("FPA", "FQA"):
        payload["TPPSections"] = remap_tpp_sections(
            payload.get("TPPSections", []),
            ctx["tpp_id_map"],
        )

        ga_field = (
            "FPAToGeneralAttributeRisks"
            if entity_type == "FPA"
            else "FQAToGeneralAttributeRisks"
        )

        if is_risk_ranking_method(ctx):
            payload[ga_field] = remap_general_attribute_risks(
                payload.get(ga_field, []),
                ctx["ga_id_map"],
                ctx["tgt_ga_name_map"],
                source=entity_type,
            )
            payload.pop("GeneralAttributes", None)
        else:
            payload["GeneralAttributes"] = remap_general_attributes(
                payload.get("GeneralAttributes", []),
                ctx["ga_id_map"],
            )
            payload.pop("FPAToGeneralAttributeRisks", None)
            payload.pop("FQAToGeneralAttributeRisks", None)

        payload["ControlMethods"] = remap_control_methods(
            payload.get("ControlMethods", []),
            ctx["cm_id_map"],
        )

        return payload

    if entity_type == "DrugSubstance":
        return remap_fqa_links(
            payload,
            field_name="DrugSubstanceToFQAs",
            fqa_id_map=ctx["fqa_id_map"],
        )

    if entity_type == "DrugProduct":
        return remap_fqa_links(
            payload,
            field_name="DrugProductToFQAs",
            fqa_id_map=ctx["fqa_id_map"],
        )

    return payload
# --------------------- ENTITY HELPERS ---------------------
def get_ga_name_map(base_url: str, api_key: str, project_id: int) -> Dict[int, str]:
    url = f"{base_url}editables/GeneralAttribute/list/{project_id}"
    data = http_get(url, api_key)
    ga_list = data.get("instances", [])
    return {
        ga["id"]: ga["name"]
        for ga in ga_list
        if isinstance(ga, dict) and ga.get("id") and ga.get("name")
    }

def get_entities(base_url: str, api_key: str, project_id: int, endpoint: str, name_field: str = "name") -> Dict[str, int]:
    """Generic fetch function returning name -> id map."""
    url = f"{base_url}{endpoint}/{project_id}"
    data = http_get(url, api_key)

    entities_list = data.get("instances") if isinstance(data, dict) else data
    if not isinstance(entities_list, list):
        return {}

    return {e[name_field].strip(): e["id"] for e in entities_list if isinstance(e, dict) and e.get(name_field) and e.get("id") and not is_archived(e)}
# --------------------- SYNC LOGIC ---------------------
def sync_entity(
    *,
    entity_type: str,
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
    src_id: int,
    tgt_id: int,
    project_id: int,
    remap_ctx: dict | None = None,
):
    """
    Sync a single editable entity from source to target.

    Always includes required fields, LastVersionId, and logs actual changes.
    """
    cfg = ENTITY_CONFIG[entity_type]
    allowed_fields, diff_fields_cfg, sync_fields_cfg = get_effective_entity_fields(entity_type, cfg, remap_ctx)

    # Fetch full source and target
    src_full = fetch_full_editable(src_base, src_key, entity_type, src_id)
    src_full = validate_target_scope(src_full, SRC_PROJECT_ID, f"source {entity_type}")
    if not src_full:
        return
    if is_archived(src_full):
        logger.info("Skipping archived %s '%s'", entity_type, src_full.get("name"))
        return
    tgt_full = fetch_full_editable(tgt_base, tgt_key, entity_type, tgt_id)
    tgt_full = validate_target_scope(tgt_full, project_id, f"target {entity_type}")
    if not tgt_full:
        return

    # Sanitize source payload
    sanitized_src = sanitize_payload(src_full, allowed_fields)
    requirement_payload = None
    if entity_type in ("FPA", "FQA"):
        src_ranges = build_acceptance_criteria_ranges(src_full)
        if src_ranges:
            sanitized_src["AcceptanceCriteriaRanges"] = src_ranges
            requirement_payload = {"AcceptanceCriteriaRanges": src_ranges}
        if not is_risk_ranking_method(remap_ctx):
            sanitized_src = apply_non_riskranking_risk_values(src_full, sanitized_src)
            resolved_method = resolve_non_riskranking_method(src_full, remap_ctx)
            if not resolved_method and is_classification_method(remap_ctx):
                resolved_method = "Classification"
            if resolved_method:
                sanitized_src["riskAssessmentMethod"] = resolved_method
    sanitized_src = strip_attachment_links(sanitized_src)

    # Include LastVersionId from target
    sanitized_src["LastVersionId"] = tgt_full.get("LastVersionId")

    # Apply remap if required
    if cfg.get("remap") and remap_ctx:
        remap_fn = globals()[cfg["remap"]]
        sanitized_src = remap_fn(entity_type, sanitized_src, remap_ctx)

    if entity_type == "ControlMethod":
        supplier_cache = remap_ctx.get("supplier_cache") if remap_ctx else None
        src_supplier_id = sanitized_src.get("SupplierId")
        if src_supplier_id:
            tgt_supplier_id = resolve_target_supplier_id(
                src_base,
                tgt_base,
                src_key,
                tgt_key,
                src_supplier_id,
                supplier_cache,
            )
            if tgt_supplier_id:
                sanitized_src["SupplierId"] = tgt_supplier_id
            else:
                logger.warning(
                    "Unable to map SupplierId %s for ControlMethod '%s' (%s); leaving target value unchanged",
                    src_supplier_id,
                    sanitized_src.get("name"),
                    src_id,
                )
                if isinstance(tgt_full, dict) and "SupplierId" in tgt_full:
                    sanitized_src["SupplierId"] = tgt_full.get("SupplierId")
                else:
                    sanitized_src.pop("SupplierId", None)

    # diffs for logging only
    if entity_type in ("FPA", "FQA"):
        tgt_req = tgt_full.get("Requirement") if isinstance(tgt_full, dict) else None
        if isinstance(tgt_req, dict):
            tgt_ranges = tgt_req.get("AcceptanceCriteriaRanges")
            if isinstance(tgt_ranges, list):
                tgt_full = dict(tgt_full)
                tgt_full["AcceptanceCriteriaRanges"] = normalize_acceptance_criteria_ranges_list(tgt_ranges)
    sync_fields_for_update = sync_fields_cfg
    sanitized_src = _preserve_whitespace_only_changes(sanitized_src, tgt_full, sync_fields_for_update)

    diffs = diff_fields(sanitized_src, tgt_full, diff_fields_cfg)
    if entity_type in ("FPA", "FQA"):
        rel_diffs = {}

        def _ids(items, id_keys=("id",)):
            ids = set()
            for item in items or []:
                if not isinstance(item, dict):
                    continue
                for key in id_keys:
                    val = item.get(key)
                    if val is not None:
                        ids.add(val)
                        break
            return ids

        ga_field = (
            "FPAToGeneralAttributeRisks"
            if entity_type == "FPA"
            else "FQAToGeneralAttributeRisks"
        )

        def _ga_tuples(items):
            tuples = set()
            for item in items or []:
                if not isinstance(item, dict):
                    continue
                ga_id = item.get("GeneralAttributeId")
                if not ga_id and isinstance(item.get("GeneralAttribute"), dict):
                    ga_id = item["GeneralAttribute"].get("id")
                if not ga_id:
                    continue
                tuples.add((ga_id, item.get("impact"), item.get("uncertainty"), item.get("justification")))
            return tuples

        src_tpp_ids = _ids(sanitized_src.get("TPPSections", []))
        tgt_tpp_ids = _ids(tgt_full.get("TPPSections", []))
        if src_tpp_ids != tgt_tpp_ids:
            rel_diffs["TPPSections"] = {
                "from": sorted(tgt_tpp_ids),
                "to": sorted(src_tpp_ids),
            }

        src_general_attribute_ids = _ids(sanitized_src.get("GeneralAttributes", []))
        tgt_general_attribute_ids = _ids(tgt_full.get("GeneralAttributes", []))
        if src_general_attribute_ids != tgt_general_attribute_ids:
            rel_diffs["GeneralAttributes"] = {
                "from": sorted(tgt_general_attribute_ids),
                "to": sorted(src_general_attribute_ids),
            }

        src_cm_ids = _ids(sanitized_src.get("ControlMethods", []), id_keys=("id", "ControlMethodId"))
        tgt_cm_ids = _ids(tgt_full.get("ControlMethods", []), id_keys=("id", "ControlMethodId"))
        if src_cm_ids != tgt_cm_ids:
            rel_diffs["ControlMethods"] = {
                "from": sorted(tgt_cm_ids),
                "to": sorted(src_cm_ids),
            }

        if is_risk_ranking_method(remap_ctx):
            src_ga = _ga_tuples(sanitized_src.get(ga_field, []))
            tgt_ga = _ga_tuples(tgt_full.get(ga_field, []))
            if src_ga != tgt_ga:
                rel_diffs[ga_field] = {
                    "from": sorted(tgt_ga),
                    "to": sorted(src_ga),
                }

        if rel_diffs:
            diffs.update(rel_diffs)

    if entity_type in ("DrugSubstance", "DrugProduct"):
        rel_diffs = {}

        def _fqa_ids(items):
            ids = set()
            for item in items or []:
                if not isinstance(item, dict):
                    continue
                val = item.get("FQAId")
                if val is not None:
                    ids.add(val)
            return ids

        field_name = (
            "DrugSubstanceToFQAs"
            if entity_type == "DrugSubstance"
            else "DrugProductToFQAs"
        )

        src_ids = _fqa_ids(sanitized_src.get(field_name, []))
        tgt_ids = _fqa_ids(tgt_full.get(field_name, []))
        if src_ids != tgt_ids:
            rel_diffs[field_name] = {
                "from": sorted(tgt_ids),
                "to": sorted(src_ids),
            }

        if rel_diffs:
            diffs.update(rel_diffs)

    if not diffs:
        logger.info(
            "No changes detected for %s '%s' (%s)",
            entity_type,
            sanitized_src.get("name"),
            tgt_id
        )
        return  # skip PUT if nothing changed
    else:
        logger.info(
            "Updating %s '%s' (%s): %s",
            entity_type,
            sanitized_src.get("name"),
            tgt_id,
            list(diffs.keys())
        )
        for field, change in diffs.items():
            logger.info("  - %s: %r → %r", field, change["from"], change["to"])

        # Always send full allowed_fields + LastVersionId + id + ProjectId
        payload = {"id": tgt_id, "ProjectId": project_id, "LastVersionId": sanitized_src["LastVersionId"]}
        sync_fields = sync_fields_cfg
        payload.update({k: sanitized_src[k] for k in sync_fields if k in sanitized_src})
        if requirement_payload:
            payload.pop("AcceptanceCriteriaRanges", None)
            payload["Requirement"] = requirement_payload
        if entity_type == "Project":
            rmp_id = None
            if remap_ctx:
                rmp_id = remap_ctx.get("project_rmp_id")
            if not rmp_id and isinstance(tgt_full, dict):
                rmp_id = tgt_full.get("RMPId")
            if rmp_id:
                payload["RMPId"] = rmp_id

    try:
        http_put(f"{tgt_base}{cfg['endpoint']}/addOrEdit", tgt_key, payload)
    except requests.HTTPError:
        logger.error(
            "Failed updating %s '%s' (%s)",
            entity_type,
            sanitized_src.get("name"),
            src_id,
        )
        raise

def sync_or_create_entities(
    entity_type: str,
    src_base: str,
    tgt_base: str,
    src_key: str,
    tgt_key: str,
    project_id: int,
    remap_ctx: dict | None = None,
    prev_mapping: dict | None = None,
) -> Dict[int, int]:
    """
    Syncs all source entities of a given type to target.
    Returns mapping: source_id -> target_id
    """
    prev_mapping = prev_mapping or {}

    # Fetch source and target entities (name -> id)
    src_entities = get_entities(src_base, src_key, SRC_PROJECT_ID, f"editables/{entity_type}/list")
    tgt_entities = get_entities(tgt_base, tgt_key, project_id, f"editables/{entity_type}/list")

    logger.info("Syncing %s: %s source, %s target", entity_type, len(src_entities), len(tgt_entities))

    mapping: Dict[int, int] = {}

    for src_name, src_id in src_entities.items():
        tgt_id = None

        # Prefer persisted mapping by source ID to handle renames
        prev_tgt_id = prev_mapping.get(src_id) or prev_mapping.get(str(src_id))
        if prev_tgt_id:
            try:
                tgt_full = fetch_full_editable(tgt_base, tgt_key, entity_type, prev_tgt_id)
            except requests.HTTPError:
                logger.warning(
                    "Mapped target %s id %s not found; falling back to name lookup",
                    entity_type,
                    prev_tgt_id,
                )
            else:
                tgt_full = validate_target_scope(tgt_full, project_id, f"target {entity_type}")
                if tgt_full:
                    tgt_id = prev_tgt_id

        if not tgt_id:
            tgt_id = tgt_entities.get(src_name)

        if tgt_id:
            # Existing entity -> diff check + sync
            sync_entity(
                entity_type=entity_type,
                src_id=src_id,
                tgt_id=tgt_id,
                src_base=src_base,
                tgt_base=tgt_base,
                src_key=src_key,
                tgt_key=tgt_key,
                project_id=project_id,
                remap_ctx=remap_ctx,
            )
            mapping[src_id] = tgt_id
        else:
            # New entity -> create full payload
            src_full = fetch_full_editable(src_base, src_key, entity_type, src_id)
            src_full = validate_target_scope(src_full, SRC_PROJECT_ID, f"source {entity_type}")
            if not src_full:
                continue
            if is_archived(src_full):
                logger.info("Skipping archived %s '%s'", entity_type, src_full.get("name"))
                continue
            cfg = ENTITY_CONFIG[entity_type]
            allowed_fields, _, _ = get_effective_entity_fields(entity_type, cfg, remap_ctx)

            payload = sanitize_payload(src_full, allowed_fields)
            if entity_type in ("FPA", "FQA"):
                src_ranges = build_acceptance_criteria_ranges(src_full)
                if src_ranges:
                    payload["Requirement"] = {"AcceptanceCriteriaRanges": src_ranges}
                payload.pop("AcceptanceCriteriaRanges", None)
                if not is_risk_ranking_method(remap_ctx):
                    payload = apply_non_riskranking_risk_values(src_full, payload)
                    resolved_method = resolve_non_riskranking_method(src_full, remap_ctx)
                    if not resolved_method and is_classification_method(remap_ctx):
                        resolved_method = "Classification"
                    if resolved_method:
                        payload["riskAssessmentMethod"] = resolved_method
            payload = strip_attachment_links(payload)
            if entity_type == "ControlMethod":
                supplier_cache = remap_ctx.get("supplier_cache") if remap_ctx else None
                src_supplier_id = payload.get("SupplierId")
                if src_supplier_id:
                    tgt_supplier_id = resolve_target_supplier_id(
                        src_base,
                        tgt_base,
                        src_key,
                        tgt_key,
                        src_supplier_id,
                        supplier_cache,
                    )
                    if tgt_supplier_id:
                        payload["SupplierId"] = tgt_supplier_id
                    else:
                        logger.warning(
                            "Unable to map SupplierId %s for ControlMethod '%s' (%s); creating without supplier",
                            src_supplier_id,
                            src_name,
                            src_id,
                        )
                        payload.pop("SupplierId", None)
            payload["ProjectId"] = project_id

            # Apply remap if required
            if cfg.get("remap") and remap_ctx:
                remap_fn = globals()[cfg["remap"]]
                payload = remap_fn(entity_type, payload, remap_ctx)

            try:
                new_entity = http_put(f"{tgt_base}{cfg['endpoint']}/addOrEdit", tgt_key, payload)
            except requests.HTTPError:
                logger.error(
                    "Failed creating %s '%s' (%s)",
                    entity_type,
                    src_name,
                    src_id,
                )
                raise

            tgt_id = new_entity.get("id")
            logger.info("Created new %s '%s': %s -> %s", entity_type, src_name, src_id, tgt_id)

            mapping[src_id] = tgt_id

    return mapping
# --------------------- MAIN ---------------------
def main():
    validate_config()
    src_base = make_base_url(SRC_HOST, SRC_BASE_PATH)
    tgt_base = make_base_url(TGT_HOST, TGT_BASE_PATH)
    logger.info("Source base URL: %s", src_base)
    logger.info("Target base URL: %s", tgt_base)

    id_map = load_id_map()
    projects_map = id_map.setdefault("projects", {})
    existing_project = projects_map.get(str(SRC_PROJECT_ID), {})
    existing_tgt_id = existing_project.get("targetProjectId")

    global LOG_PATH
    LOG_PATH = setup_logging(existing_tgt_id)
    logger.info("Log file: %s", LOG_PATH)
    logger.info("Starting sync run for source project %s", SRC_PROJECT_ID)

    saved_id_map = False

    try:
        # FETCH SOURCE PROJECT
        src_project = http_get(individual_record_url(src_base, "Project", SRC_PROJECT_ID), SRC_KEY)
        if is_archived(src_project):
            logger.info("Source project %s is archived; skipping copy", SRC_PROJECT_ID)
            return
        sanitized_src_project = sanitize_payload(src_project, ALLOWED_PROJECT_FIELDS)
        sanitized_src_project = strip_attachment_links(sanitized_src_project)
        # REMAP RMP
        src_rmp_id = sanitized_src_project.get("RMPId")
        tgt_rmp_id = None

        if src_rmp_id:
            src_rmp = http_get(individual_record_url(src_base, "RMP", src_rmp_id), SRC_KEY)
            src_rmp_name = src_rmp.get("name")

            tgt_rmp_id = get_target_rmp_by_name(
                tgt_base,
                TGT_KEY,
                src_rmp_name
            )

            if tgt_rmp_id:
                logger.info(
                    "Mapped existing RMP '%s': %s → %s",
                    src_rmp_name,
                    src_rmp_id,
                    tgt_rmp_id
                )
            else:
                logger.info(
                    "RMP '%s' not found in target; creating it",
                    src_rmp_name
                )
                tgt_rmp_id = create_target_rmp(
                    tgt_base,
                    TGT_KEY,
                    src_rmp
                )
                logger.info(
                    "Created new RMP '%s': %s → %s",
                    src_rmp_name,
                    src_rmp_id,
                    tgt_rmp_id
                )

            sanitized_src_project["RMPId"] = tgt_rmp_id
        # CREATE OR REUSE TARGET PROJECT
        if str(SRC_PROJECT_ID) not in projects_map:
            logger.info("No existing mapping found. Creating new project.")

            new_project = http_put(f"{tgt_base}editables/Project/addOrEdit", TGT_KEY, sanitized_src_project)
            tgt_project_id = new_project["id"]

            LOG_PATH = setup_logging(tgt_project_id)
            logger.info("Log file: %s", LOG_PATH)

            projects_map[str(SRC_PROJECT_ID)] = {
                "targetProjectId": tgt_project_id,
            }

            logger.info("Created new project: %s → %s", SRC_PROJECT_ID, tgt_project_id)
        else:
            tgt_project_id = projects_map[str(SRC_PROJECT_ID)]["targetProjectId"]
            logger.info("Using existing project mapping: %s → %s", SRC_PROJECT_ID, tgt_project_id)

        project_state = projects_map[str(SRC_PROJECT_ID)]
        # SYNC PROJECT RECORD DATA
        sync_entity(
            entity_type="Project",
            src_id=SRC_PROJECT_ID,
            tgt_id=tgt_project_id,
            src_base=src_base,
            tgt_base=tgt_base,
            src_key=SRC_KEY,
            tgt_key=TGT_KEY,
            project_id=tgt_project_id,
            remap_ctx={"project_rmp_id": tgt_rmp_id},
        )

        # SYNC ALL ENTITY TYPES
        # First build remap context
        remap_ctx = {}
        remap_ctx["supplier_cache"] = {"by_id": {}, "by_name": {}}
        remap_ctx["project_risk_assessment_method"] = src_project.get("riskAssessmentMethod")
        remap_ctx["project_product_risk_assessment_type"] = src_project.get("productRiskAssessmentType")

        tgt_project = http_get(individual_record_url(tgt_base, "Project", tgt_project_id), TGT_KEY)
        remap_ctx["target_project_risk_assessment_method"] = tgt_project.get("riskAssessmentMethod")
        remap_ctx["target_project_product_risk_assessment_type"] = tgt_project.get("productRiskAssessmentType")

        # TPPSections
        tpp_mapping = sync_or_create_entities(
            "TPPSection", src_base, tgt_base, SRC_KEY, TGT_KEY,
            tgt_project_id, remap_ctx,
            prev_mapping=project_state.get("tppSections", {}),
        )
        project_state["tppSections"] = {str(k): v for k, v in tpp_mapping.items()}
        remap_ctx["tpp_id_map"] = {str(k): v for k, v in tpp_mapping.items()}

        # GeneralAttributes
        ga_mapping = sync_or_create_entities(
            "GeneralAttribute", src_base, tgt_base, SRC_KEY, TGT_KEY,
            tgt_project_id, remap_ctx,
            prev_mapping=project_state.get("generalAttributes", {}),
        )
        project_state["generalAttributes"] = {str(k): v for k, v in ga_mapping.items()}
        tgt_ga_name_map = get_ga_name_map(tgt_base, TGT_KEY, tgt_project_id)
        remap_ctx["ga_id_map"] = {str(k): v for k, v in ga_mapping.items()}
        remap_ctx["tgt_ga_name_map"] = tgt_ga_name_map

        # ControlMethods
        cm_mapping = sync_or_create_entities(
            "ControlMethod", src_base, tgt_base, SRC_KEY, TGT_KEY,
            tgt_project_id, remap_ctx,
            prev_mapping=project_state.get("controlMethods", {}),
        )
        project_state["controlMethods"] = {str(k): v for k, v in cm_mapping.items()}
        remap_ctx["cm_id_map"] = {str(k): v for k, v in cm_mapping.items()}

        # FPAs
        fpa_mapping = sync_or_create_entities(
            "FPA", src_base, tgt_base, SRC_KEY, TGT_KEY,
            tgt_project_id, remap_ctx,
            prev_mapping=project_state.get("fpas", {}),
        )
        project_state["fpas"] = {str(k): v for k, v in fpa_mapping.items()}

        # FQAs
        fqa_mapping = sync_or_create_entities(
            "FQA", src_base, tgt_base, SRC_KEY, TGT_KEY,
            tgt_project_id, remap_ctx,
            prev_mapping=project_state.get("fqas", {}),
        )
        project_state["fqas"] = {str(k): v for k, v in fqa_mapping.items()}

        # Drug Substances
        ds_mapping = sync_or_create_entities(
            "DrugSubstance",
            src_base,
            tgt_base,
            SRC_KEY,
            TGT_KEY,
            tgt_project_id,
            remap_ctx={
                "fqa_id_map": {str(k): v for k, v in fqa_mapping.items()}
            },
            prev_mapping=project_state.get("drugSubstances", {}),
        )
        project_state["drugSubstances"] = {str(k): v for k, v in ds_mapping.items()}

        # Drug Products
        dp_mapping = sync_or_create_entities(
            "DrugProduct",
            src_base,
            tgt_base,
            SRC_KEY,
            TGT_KEY,
            tgt_project_id,
            remap_ctx={
                "fqa_id_map": {str(k): v for k, v in fqa_mapping.items()}
            },
            prev_mapping=project_state.get("drugProducts", {}),
        )
        project_state["drugProducts"] = {str(k): v for k, v in dp_mapping.items()}

        # SAVE STATE
        save_id_map(id_map)
        saved_id_map = True
        logger.info("Sync complete for source project %s", SRC_PROJECT_ID)

    except requests.HTTPError as e:
        logger.error("HTTP error: %s", e)
        if e.response is not None:
            logger.error("Response body: %s", e.response.text)
    except Exception:
        logger.exception("Unexpected error")
    finally:
        if not saved_id_map:
            save_id_map(id_map)
            
if __name__ == "__main__":
    main()
