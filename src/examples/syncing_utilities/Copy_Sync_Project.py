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

import logging
import uuid
import re
from dataclasses import dataclass
from typing import Any, Dict

import requests
from dotenv import load_dotenv

from sync_common import (
    QbdApiClient,
    SyncWriter,
    is_archived,
    load_id_map_file,
    normalize_acceptance_criteria_ranges_list as normalize_acr_ranges,
    required_env,
    required_int,
    resolve_target_supplier_id as resolve_supplier_id,
    sanitize_payload,
    save_id_map_file,
    setup_file_logging,
    strip_attachment_links,
)
# --------------------- CONFIG ---------------------
load_dotenv()

LOG_DIR = "logs"

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
def setup_logging(src_project_id: int, tgt_project_id: int | None = None) -> str:
    return setup_file_logging(
        log_dir=LOG_DIR,
        filename_prefix="copy_project",
        src_id=src_project_id,
        tgt_id=tgt_project_id,
    )

# --------------------- ID MAP PERSISTENCE ---------------------
def load_id_map() -> dict:
    return load_id_map_file(ID_MAP_FILE, "projects")

def save_id_map(id_map: dict):
    save_id_map_file(ID_MAP_FILE, id_map)

@dataclass(frozen=True)
class SyncConfig:
    src_project_id: int
    src_client: QbdApiClient
    tgt_client: QbdApiClient

def load_config() -> SyncConfig:
    return SyncConfig(
        src_project_id=required_int("SOURCE_PROJECT_ID", required_env("SOURCE_PROJECT_ID")),
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
# --------------------- LINK & PAYLOAD HELPERS ---------------------
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
def fetch_full_editable(client: QbdApiClient, entity_type: str, entity_id: int) -> dict:
    return client.get_record(entity_type, entity_id)

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
def get_target_rmp_by_name(client: QbdApiClient, name):
    data = client.list_records("RMP")

    for rmp in data.get("instances", []):
        if (
            rmp.get("name") == name
            and rmp.get("currentState") == "Approved"
        ):
            return rmp["id"]

    return None

def create_target_rmp(writer: SyncWriter, src_rmp: dict) -> int:
    cleaned_payload = clean_rmp_payload(src_rmp)

    if not cleaned_payload.get("name"):
        raise ValueError("RMP payload missing name")

    rmp = writer.save_record("RMP", cleaned_payload, reason="create RMP")
    return rmp["id"]
# --------------------- SUPPLIER HELPERS ---------------------
def resolve_target_supplier_id(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_supplier_id: int,
    supplier_cache: dict | None = None,
) -> int | None:
    return resolve_supplier_id(
        src_client,
        tgt_client,
        writer,
        src_supplier_id,
        supplier_cache,
        ALLOWED_SUPPLIER_FIELDS,
        logger=logger,
    )
# --------------------- ACCEPTANCE CRITERIA ---------------------
def normalize_acceptance_criteria_ranges_list(ranges: list) -> list:
    return normalize_acr_ranges(
        ranges,
        ACR_FIELDS,
        normalize_values=False,
        include_missing=False,
    )

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

for _entity_cfg in ENTITY_CONFIG.values():
    if _entity_cfg.get("remap") == "apply_remap":
        _entity_cfg["remap"] = apply_remap

# --------------------- ENTITY HELPERS ---------------------
def get_ga_name_map(client: QbdApiClient, project_id: int) -> Dict[int, str]:
    data = client.list_records("GeneralAttribute", project_id)
    ga_list = data.get("instances", [])
    return {
        ga["id"]: ga["name"]
        for ga in ga_list
        if isinstance(ga, dict) and ga.get("id") and ga.get("name")
    }

def get_entities(client: QbdApiClient, project_id: int, entity_type: str, name_field: str = "name") -> Dict[str, int]:
    data = client.list_records(entity_type, project_id)

    entities_list = data.get("instances") if isinstance(data, dict) else data
    if not isinstance(entities_list, list):
        return {}

    return {
        e[name_field].strip(): e["id"]
        for e in entities_list
        if isinstance(e, dict) and e.get(name_field) and e.get("id") and not is_archived(e)
    }

def active_source_record(
    client: QbdApiClient,
    entity_type: str,
    source_id: int,
    source_project_id: int,
) -> dict | None:
    src_full = fetch_full_editable(client, entity_type, source_id)
    src_full = validate_target_scope(src_full, source_project_id, f"source {entity_type}")
    if not src_full:
        return None
    if is_archived(src_full):
        logger.info("Skipping archived %s '%s'", entity_type, src_full.get("name"))
        return None
    return src_full

def apply_fpa_fqa_payload_rules(entity_type: str, src_full: dict, payload: dict, remap_ctx: dict | None) -> tuple[dict, dict | None]:
    requirement_payload = None
    if entity_type not in ("FPA", "FQA"):
        return payload, requirement_payload

    src_ranges = build_acceptance_criteria_ranges(src_full)
    if src_ranges:
        payload["AcceptanceCriteriaRanges"] = src_ranges
        requirement_payload = {"AcceptanceCriteriaRanges": src_ranges}
    if not is_risk_ranking_method(remap_ctx):
        payload = apply_non_riskranking_risk_values(src_full, payload)
        resolved_method = resolve_non_riskranking_method(src_full, remap_ctx)
        if not resolved_method and is_classification_method(remap_ctx):
            resolved_method = "Classification"
        if resolved_method:
            payload["riskAssessmentMethod"] = resolved_method
    return payload, requirement_payload

def remap_entity_payload(entity_type: str, payload: dict, cfg: dict, remap_ctx: dict | None) -> dict:
    remap_fn = cfg.get("remap")
    if remap_fn and remap_ctx:
        payload = remap_fn(entity_type, payload, remap_ctx)
    return payload

def remap_control_method_supplier(
    payload: dict,
    *,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_id: int,
    target_full: dict | None,
    remap_ctx: dict | None,
    drop_on_missing: bool,
) -> dict:
    supplier_cache = remap_ctx.get("supplier_cache") if remap_ctx else None
    src_supplier_id = payload.get("SupplierId")
    if not src_supplier_id:
        return payload

    tgt_supplier_id = resolve_target_supplier_id(
        src_client,
        tgt_client,
        writer,
        src_supplier_id,
        supplier_cache,
    )
    if tgt_supplier_id:
        payload["SupplierId"] = tgt_supplier_id
        return payload

    if drop_on_missing:
        logger.warning(
            "Unable to map SupplierId %s for ControlMethod '%s' (%s); creating without supplier",
            src_supplier_id,
            payload.get("name"),
            src_id,
        )
        payload.pop("SupplierId", None)
        return payload

    logger.warning(
        "Unable to map SupplierId %s for ControlMethod '%s' (%s); leaving target value unchanged",
        src_supplier_id,
        payload.get("name"),
        src_id,
    )
    if isinstance(target_full, dict) and "SupplierId" in target_full:
        payload["SupplierId"] = target_full.get("SupplierId")
    else:
        payload.pop("SupplierId", None)
    return payload

def build_entity_payload(
    *,
    entity_type: str,
    src_full: dict,
    cfg: dict,
    remap_ctx: dict | None,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_id: int,
    target_full: dict | None = None,
    drop_supplier_on_missing: bool = False,
) -> tuple[dict, dict | None, list, list]:
    allowed_fields, diff_fields_cfg, sync_fields_cfg = get_effective_entity_fields(entity_type, cfg, remap_ctx)
    payload = sanitize_payload(src_full, allowed_fields)
    payload, requirement_payload = apply_fpa_fqa_payload_rules(entity_type, src_full, payload, remap_ctx)
    payload = strip_attachment_links(payload)
    payload = remap_entity_payload(entity_type, payload, cfg, remap_ctx)

    if entity_type == "ControlMethod":
        payload = remap_control_method_supplier(
            payload,
            src_client=src_client,
            tgt_client=tgt_client,
            writer=writer,
            src_id=src_id,
            target_full=target_full,
            remap_ctx=remap_ctx,
            drop_on_missing=drop_supplier_on_missing,
        )

    return payload, requirement_payload, diff_fields_cfg, sync_fields_cfg

def relationship_ids(items, id_keys=("id",)) -> set:
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

def general_attribute_risk_tuples(items) -> set:
    tuples = set()
    for item in items or []:
        if not isinstance(item, dict):
            continue
        ga_id = item.get("GeneralAttributeId")
        if not ga_id and isinstance(item.get("GeneralAttribute"), dict):
            ga_id = item["GeneralAttribute"].get("id")
        if ga_id:
            tuples.add((ga_id, item.get("impact"), item.get("uncertainty"), item.get("justification")))
    return tuples

def append_id_relationship_diff(diffs: dict, field_name: str, src_items: list, tgt_items: list, id_keys=("id",)) -> None:
    src_ids = relationship_ids(src_items, id_keys)
    tgt_ids = relationship_ids(tgt_items, id_keys)
    if src_ids != tgt_ids:
        diffs[field_name] = {
            "from": sorted(tgt_ids),
            "to": sorted(src_ids),
        }

def relationship_diffs(entity_type: str, sanitized_src: dict, tgt_full: dict, remap_ctx: dict | None) -> dict:
    diffs = {}
    if entity_type in ("FPA", "FQA"):
        ga_field = "FPAToGeneralAttributeRisks" if entity_type == "FPA" else "FQAToGeneralAttributeRisks"
        append_id_relationship_diff(
            diffs,
            "TPPSections",
            sanitized_src.get("TPPSections", []),
            tgt_full.get("TPPSections", []),
        )
        append_id_relationship_diff(
            diffs,
            "GeneralAttributes",
            sanitized_src.get("GeneralAttributes", []),
            tgt_full.get("GeneralAttributes", []),
        )
        append_id_relationship_diff(
            diffs,
            "ControlMethods",
            sanitized_src.get("ControlMethods", []),
            tgt_full.get("ControlMethods", []),
            id_keys=("id", "ControlMethodId"),
        )
        if is_risk_ranking_method(remap_ctx):
            src_ga = general_attribute_risk_tuples(sanitized_src.get(ga_field, []))
            tgt_ga = general_attribute_risk_tuples(tgt_full.get(ga_field, []))
            if src_ga != tgt_ga:
                diffs[ga_field] = {
                    "from": sorted(tgt_ga),
                    "to": sorted(src_ga),
                }
    elif entity_type in ("DrugSubstance", "DrugProduct"):
        field_name = "DrugSubstanceToFQAs" if entity_type == "DrugSubstance" else "DrugProductToFQAs"
        append_id_relationship_diff(
            diffs,
            field_name,
            sanitized_src.get(field_name, []),
            tgt_full.get(field_name, []),
            id_keys=("FQAId",),
        )
    return diffs

def target_with_requirement_ranges(entity_type: str, target: dict) -> dict:
    if entity_type not in ("FPA", "FQA"):
        return target
    tgt_req = target.get("Requirement") if isinstance(target, dict) else None
    if not isinstance(tgt_req, dict):
        return target
    tgt_ranges = tgt_req.get("AcceptanceCriteriaRanges")
    if not isinstance(tgt_ranges, list):
        return target
    target = dict(target)
    target["AcceptanceCriteriaRanges"] = normalize_acceptance_criteria_ranges_list(tgt_ranges)
    return target

def build_update_payload(
    *,
    entity_type: str,
    sanitized_src: dict,
    target_full: dict,
    target_id: int,
    project_id: int,
    sync_fields: list,
    requirement_payload: dict | None,
    remap_ctx: dict | None,
) -> dict:
    payload = {"id": target_id, "ProjectId": project_id, "LastVersionId": sanitized_src["LastVersionId"]}
    payload.update({k: sanitized_src[k] for k in sync_fields if k in sanitized_src})
    if requirement_payload:
        payload.pop("AcceptanceCriteriaRanges", None)
        payload["Requirement"] = requirement_payload
    if entity_type == "Project":
        rmp_id = remap_ctx.get("project_rmp_id") if remap_ctx else None
        if not rmp_id and isinstance(target_full, dict):
            rmp_id = target_full.get("RMPId")
        if rmp_id:
            payload["RMPId"] = rmp_id
    return payload

def resolve_target_entity(
    *,
    tgt_client: QbdApiClient,
    entity_type: str,
    source_id: int,
    source_name: str,
    target_project_id: int,
    target_entities: Dict[str, int],
    prev_mapping: dict,
) -> int | None:
    prev_tgt_id = prev_mapping.get(source_id) or prev_mapping.get(str(source_id))
    if prev_tgt_id:
        try:
            tgt_full = fetch_full_editable(tgt_client, entity_type, prev_tgt_id)
        except requests.HTTPError:
            logger.warning(
                "Mapped target %s id %s not found; falling back to name lookup",
                entity_type,
                prev_tgt_id,
            )
        else:
            tgt_full = validate_target_scope(tgt_full, target_project_id, f"target {entity_type}")
            if tgt_full:
                return prev_tgt_id

    return target_entities.get(source_name)
# --------------------- SYNC LOGIC ---------------------


def sync_entity(
    *,
    entity_type: str,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_id: int,
    tgt_id: int,
    source_project_id: int,
    target_project_id: int,
    remap_ctx: dict | None = None,
):
    cfg = ENTITY_CONFIG[entity_type]

    src_full = active_source_record(src_client, entity_type, src_id, source_project_id)
    if not src_full:
        return

    tgt_full = fetch_full_editable(tgt_client, entity_type, tgt_id)
    tgt_full = validate_target_scope(tgt_full, target_project_id, f"target {entity_type}")
    if not tgt_full:
        return

    sanitized_src, requirement_payload, diff_fields_cfg, sync_fields_cfg = build_entity_payload(
        entity_type=entity_type,
        src_full=src_full,
        cfg=cfg,
        remap_ctx=remap_ctx,
        src_client=src_client,
        tgt_client=tgt_client,
        writer=writer,
        src_id=src_id,
        target_full=tgt_full,
        drop_supplier_on_missing=False,
    )
    sanitized_src["LastVersionId"] = tgt_full.get("LastVersionId")

    tgt_full = target_with_requirement_ranges(entity_type, tgt_full)
    sanitized_src = _preserve_whitespace_only_changes(sanitized_src, tgt_full, sync_fields_cfg)

    diffs = diff_fields(sanitized_src, tgt_full, diff_fields_cfg)
    diffs.update(relationship_diffs(entity_type, sanitized_src, tgt_full, remap_ctx))

    if not diffs:
        logger.info(
            "No changes detected for %s '%s' (%s)",
            entity_type,
            sanitized_src.get("name"),
            tgt_id,
        )
        return

    logger.info(
        "Updating %s '%s' (%s): %s",
        entity_type,
        sanitized_src.get("name"),
        tgt_id,
        list(diffs.keys()),
    )
    for field, change in diffs.items():
        logger.info("  - %s: %r -> %r", field, change["from"], change["to"])

    payload = build_update_payload(
        entity_type=entity_type,
        sanitized_src=sanitized_src,
        target_full=tgt_full,
        target_id=tgt_id,
        project_id=target_project_id,
        sync_fields=sync_fields_cfg,
        requirement_payload=requirement_payload,
        remap_ctx=remap_ctx,
    )

    try:
        writer.save_record(entity_type, payload, reason=f"update {entity_type}")
    except requests.HTTPError:
        logger.error(
            "Failed updating %s '%s' (%s)",
            entity_type,
            sanitized_src.get("name"),
            src_id,
        )
        raise

def create_entity(
    *,
    entity_type: str,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_id: int,
    src_name: str,
    source_project_id: int,
    target_project_id: int,
    remap_ctx: dict | None = None,
) -> int | None:
    src_full = active_source_record(src_client, entity_type, src_id, source_project_id)
    if not src_full:
        return None

    cfg = ENTITY_CONFIG[entity_type]
    payload, requirement_payload, _, _ = build_entity_payload(
        entity_type=entity_type,
        src_full=src_full,
        cfg=cfg,
        remap_ctx=remap_ctx,
        src_client=src_client,
        tgt_client=tgt_client,
        writer=writer,
        src_id=src_id,
        drop_supplier_on_missing=True,
    )
    if requirement_payload:
        payload.pop("AcceptanceCriteriaRanges", None)
        payload["Requirement"] = requirement_payload
    payload["ProjectId"] = target_project_id

    try:
        new_entity = writer.save_record(entity_type, payload, reason=f"create {entity_type}")
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
    return tgt_id

def sync_or_create_entities(
    entity_type: str,
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    source_project_id: int,
    target_project_id: int,
    remap_ctx: dict | None = None,
    prev_mapping: dict | None = None,
) -> Dict[int, int]:
    prev_mapping = prev_mapping or {}

    src_entities = get_entities(src_client, source_project_id, entity_type)
    tgt_entities = get_entities(tgt_client, target_project_id, entity_type)

    logger.info("Syncing %s: %s source, %s target", entity_type, len(src_entities), len(tgt_entities))

    mapping: Dict[int, int] = {}

    for src_name, src_id in src_entities.items():
        tgt_id = resolve_target_entity(
            tgt_client=tgt_client,
            entity_type=entity_type,
            source_id=src_id,
            source_name=src_name,
            target_project_id=target_project_id,
            target_entities=tgt_entities,
            prev_mapping=prev_mapping,
        )

        if tgt_id:
            sync_entity(
                entity_type=entity_type,
                src_id=src_id,
                tgt_id=tgt_id,
                src_client=src_client,
                tgt_client=tgt_client,
                writer=writer,
                source_project_id=source_project_id,
                target_project_id=target_project_id,
                remap_ctx=remap_ctx,
            )
            mapping[src_id] = tgt_id
            continue

        tgt_id = create_entity(
            entity_type=entity_type,
            src_client=src_client,
            tgt_client=tgt_client,
            writer=writer,
            src_id=src_id,
            src_name=src_name,
            source_project_id=source_project_id,
            target_project_id=target_project_id,
            remap_ctx=remap_ctx,
        )
        if tgt_id:
            mapping[src_id] = tgt_id

    return mapping
# --------------------- MAIN ---------------------
def resolve_project_rmp(config: SyncConfig, writer: SyncWriter, sanitized_src_project: dict) -> int | None:
    src_rmp_id = sanitized_src_project.get("RMPId")
    if not src_rmp_id:
        return None

    src_rmp = config.src_client.get_record("RMP", src_rmp_id)
    src_rmp_name = src_rmp.get("name")
    tgt_rmp_id = get_target_rmp_by_name(config.tgt_client, src_rmp_name)

    if tgt_rmp_id:
        logger.info("Mapped existing RMP '%s': %s -> %s", src_rmp_name, src_rmp_id, tgt_rmp_id)
    else:
        logger.info("RMP '%s' not found in target; creating it", src_rmp_name)
        tgt_rmp_id = create_target_rmp(writer, src_rmp)
        logger.info("Created new RMP '%s': %s -> %s", src_rmp_name, src_rmp_id, tgt_rmp_id)

    sanitized_src_project["RMPId"] = tgt_rmp_id
    return tgt_rmp_id

def copy_project_record(
    config: SyncConfig,
    writer: SyncWriter,
    projects_map: dict,
) -> tuple[dict, int | None, dict]:
    src_project_id = config.src_project_id
    src_project = config.src_client.get_record("Project", src_project_id)
    if is_archived(src_project):
        logger.info("Source project %s is archived; skipping copy", src_project_id)
        return src_project, None, {}

    sanitized_src_project = sanitize_payload(src_project, ALLOWED_PROJECT_FIELDS)
    sanitized_src_project = strip_attachment_links(sanitized_src_project)
    tgt_rmp_id = resolve_project_rmp(config, writer, sanitized_src_project)

    project_key = str(src_project_id)
    if project_key not in projects_map:
        logger.info("No existing mapping found. Creating new project.")
        new_project = writer.save_record("Project", sanitized_src_project, reason="create Project")
        tgt_project_id = new_project["id"]
        projects_map[project_key] = {"targetProjectId": tgt_project_id}
        logger.info("Created new project: %s -> %s", src_project_id, tgt_project_id)
    else:
        tgt_project_id = projects_map[project_key]["targetProjectId"]
        logger.info("Using existing project mapping: %s -> %s", src_project_id, tgt_project_id)

    sync_entity(
        entity_type="Project",
        src_id=src_project_id,
        tgt_id=tgt_project_id,
        src_client=config.src_client,
        tgt_client=config.tgt_client,
        writer=writer,
        source_project_id=src_project_id,
        target_project_id=tgt_project_id,
        remap_ctx={"project_rmp_id": tgt_rmp_id},
    )

    return src_project, tgt_project_id, projects_map[project_key]

def build_project_remap_context(config: SyncConfig, tgt_project_id: int, src_project: dict) -> dict:
    tgt_project = config.tgt_client.get_record("Project", tgt_project_id)
    return {
        "supplier_cache": {"by_id": {}, "by_name": {}},
        "project_risk_assessment_method": src_project.get("riskAssessmentMethod"),
        "project_product_risk_assessment_type": src_project.get("productRiskAssessmentType"),
        "target_project_risk_assessment_method": tgt_project.get("riskAssessmentMethod"),
        "target_project_product_risk_assessment_type": tgt_project.get("productRiskAssessmentType"),
    }

def persist_mapping(project_state: dict, state_key: str, mapping: dict) -> dict:
    project_state[state_key] = {str(k): v for k, v in mapping.items()}
    return project_state[state_key]

def sync_project_entities(
    config: SyncConfig,
    writer: SyncWriter,
    project_state: dict,
    src_project: dict,
    tgt_project_id: int,
) -> None:
    remap_ctx = build_project_remap_context(config, tgt_project_id, src_project)

    tpp_mapping = sync_or_create_entities(
        "TPPSection",
        config.src_client,
        config.tgt_client,
        writer,
        config.src_project_id,
        tgt_project_id,
        remap_ctx,
        prev_mapping=project_state.get("tppSections", {}),
    )
    remap_ctx["tpp_id_map"] = persist_mapping(project_state, "tppSections", tpp_mapping)

    ga_mapping = sync_or_create_entities(
        "GeneralAttribute",
        config.src_client,
        config.tgt_client,
        writer,
        config.src_project_id,
        tgt_project_id,
        remap_ctx,
        prev_mapping=project_state.get("generalAttributes", {}),
    )
    remap_ctx["ga_id_map"] = persist_mapping(project_state, "generalAttributes", ga_mapping)
    remap_ctx["tgt_ga_name_map"] = get_ga_name_map(config.tgt_client, tgt_project_id)

    cm_mapping = sync_or_create_entities(
        "ControlMethod",
        config.src_client,
        config.tgt_client,
        writer,
        config.src_project_id,
        tgt_project_id,
        remap_ctx,
        prev_mapping=project_state.get("controlMethods", {}),
    )
    remap_ctx["cm_id_map"] = persist_mapping(project_state, "controlMethods", cm_mapping)

    fpa_mapping = sync_or_create_entities(
        "FPA",
        config.src_client,
        config.tgt_client,
        writer,
        config.src_project_id,
        tgt_project_id,
        remap_ctx,
        prev_mapping=project_state.get("fpas", {}),
    )
    persist_mapping(project_state, "fpas", fpa_mapping)

    fqa_mapping = sync_or_create_entities(
        "FQA",
        config.src_client,
        config.tgt_client,
        writer,
        config.src_project_id,
        tgt_project_id,
        remap_ctx,
        prev_mapping=project_state.get("fqas", {}),
    )
    persist_mapping(project_state, "fqas", fqa_mapping)

    drug_remap_ctx = {"fqa_id_map": {str(k): v for k, v in fqa_mapping.items()}}
    ds_mapping = sync_or_create_entities(
        "DrugSubstance",
        config.src_client,
        config.tgt_client,
        writer,
        config.src_project_id,
        tgt_project_id,
        drug_remap_ctx,
        prev_mapping=project_state.get("drugSubstances", {}),
    )
    persist_mapping(project_state, "drugSubstances", ds_mapping)

    dp_mapping = sync_or_create_entities(
        "DrugProduct",
        config.src_client,
        config.tgt_client,
        writer,
        config.src_project_id,
        tgt_project_id,
        drug_remap_ctx,
        prev_mapping=project_state.get("drugProducts", {}),
    )
    persist_mapping(project_state, "drugProducts", dp_mapping)

def main():
    try:
        config = load_config()
    except ValueError as e:
        print(f"Error: {e}")
        return

    writer = SyncWriter(config.tgt_client)
    logger.info("Source base URL: %s", config.src_client.base_url)
    logger.info("Target base URL: %s", config.tgt_client.base_url)

    id_map = load_id_map()
    projects_map = id_map.setdefault("projects", {})
    existing_project = projects_map.get(str(config.src_project_id), {})
    existing_tgt_id = existing_project.get("targetProjectId")

    global LOG_PATH
    LOG_PATH = setup_logging(config.src_project_id, existing_tgt_id)
    logger.info("Log file: %s", LOG_PATH)
    logger.info("Starting sync run for source project %s", config.src_project_id)

    saved_id_map = False

    try:
        src_project, tgt_project_id, project_state = copy_project_record(config, writer, projects_map)
        if tgt_project_id is None:
            return

        sync_project_entities(config, writer, project_state, src_project, tgt_project_id)
        save_id_map(id_map)
        saved_id_map = True
        logger.info("Sync complete for source project %s", config.src_project_id)

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
