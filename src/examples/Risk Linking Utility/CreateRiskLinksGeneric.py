import csv
import json
import os
import re
import sys
import uuid
import requests
from dotenv import load_dotenv

# Load environment variables from .env.
load_dotenv()

API_KEY = os.getenv("API_KEY")
API_HOST = os.getenv("API_HOST")
API_BASE_PATH = os.getenv("API_BASE_PATH")
CSV_FILE = os.getenv("CSV_FILE")

def validate_config():
    global API_KEY, API_HOST, API_BASE_PATH, CSV_FILE

    required_values = {
        "API_KEY": API_KEY,
        "API_HOST": API_HOST,
        "API_BASE_PATH": API_BASE_PATH,
        "CSV_FILE": CSV_FILE,
    }
    missing = [name for name, value in required_values.items() if not value or not value.strip()]
    if missing:
        print(f"Error: Missing required environment values: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in all required values.")
        sys.exit(1)

    API_KEY = API_KEY.strip()
    API_HOST = API_HOST.strip().rstrip("/")
    API_BASE_PATH = API_BASE_PATH.strip().strip("/")
    CSV_FILE = CSV_FILE.strip()

    if not os.path.isfile(CSV_FILE):
        print(f"Error: CSV_FILE not found: {CSV_FILE}")
        sys.exit(1)

def headers():
    return {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "qbdvision-api-key": API_KEY,
    }

RECORD_REF_PATTERN = re.compile(r"^\s*([A-Za-z_]+)\s*-\s*(\d+)\s*$")
TARGET_RECORD_COLUMN = "target_record"
LINKED_RECORD_COLUMN = "linked_record"

RECORD_CONFIG = {
    "IQA": {
        "label": "IQA",
        "endpoint": "IQA",
        "owner_id_key": "IQAId",
        "self_link_id_key": "LinkedIQAId",
        "self_link_target_key": "TargetIQAId",
        "uncertainty_key": "uncertainty",
        "link_specs": {
            "FPA": {"field": "IQAToFPAs", "id_key": "FPAId"},
            "FQA": {"field": "IQAToFQAs", "id_key": "FQAId"},
            "IPA": {"field": "IQAToIPAs", "id_key": "IPAId"},
            "IQA": {
                "field": "IQAToIQAs",
                "id_key": "LinkedIQAId",
                "target_id_key": "TargetIQAId",
            },
        },
    },
    "IPA": {
        "label": "IPA",
        "endpoint": "IPA",
        "owner_id_key": "IPAId",
        "self_link_id_key": "LinkedIPAId",
        "self_link_target_key": "TargetIPAId",
        "uncertainty_key": "uncertainty",
        "link_specs": {
            "FPA": {"field": "IPAToFPAs", "id_key": "FPAId"},
            "FQA": {"field": "IPAToFQAs", "id_key": "FQAId"},
            "IQA": {"field": "IPAToIQAs", "id_key": "IQAId"},
            "IPA": {
                "field": "IPAToIPAs",
                "id_key": "LinkedIPAId",
                "target_id_key": "TargetIPAId",
            },
        },
    },
    "MA": {
        "label": "MA",
        "endpoint": "MaterialAttribute",
        "owner_id_key": "MaterialAttributeId",
        "uncertainty_key": "uncertainty",
        "link_specs": {
            "FPA": {"field": "MaterialAttributeToFPAs", "id_key": "FPAId"},
            "FQA": {"field": "MaterialAttributeToFQAs", "id_key": "FQAId"},
            "IPA": {"field": "MaterialAttributeToIPAs", "id_key": "IPAId"},
            "IQA": {"field": "MaterialAttributeToIQAs", "id_key": "IQAId"},
        },
    },
    "PP": {
        "label": "PP",
        "endpoint": "ProcessParameter",
        "owner_id_key": "ProcessParameterId",
        "uncertainty_key": "uncertainty",
        "link_specs": {
            "FPA": {"field": "ProcessParameterToFPAs", "id_key": "FPAId"},
            "FQA": {"field": "ProcessParameterToFQAs", "id_key": "FQAId"},
            "IPA": {"field": "ProcessParameterToIPAs", "id_key": "IPAId"},
            "IQA": {"field": "ProcessParameterToIQAs", "id_key": "IQAId"},
        },
    },
    "FPA": {
        "label": "FPA",
        "endpoint": "FPA",
        "link_specs": {
            "GA": {
                "field": "FPAToGeneralAttributeRisks",
                "id_key": "GeneralAttributeId",
                "payload_type": "ga_risk",
                "source": "FPA",
            },
        },
    },
    "FQA": {
        "label": "FQA",
        "endpoint": "FQA",
        "link_specs": {
            "GA": {
                "field": "FQAToGeneralAttributeRisks",
                "id_key": "GeneralAttributeId",
                "payload_type": "ga_risk",
                "source": "FQA",
            },
        },
    },
}

VALID_RECORD_TYPES = set(RECORD_CONFIG.keys()) | {"GA"}

def build_api_path(relative_path):
    relative_path = relative_path.lstrip("/")
    base_path = API_BASE_PATH.strip().strip("/")
    if base_path:
        return f"/{base_path}/{relative_path}"
    return f"/{relative_path}"

def request(method, path, body=None):
    url = f"https://{API_HOST}{path}"
    try:
        response = requests.request(
            method,
            url,
            headers=headers(),
            data=body,
            timeout=60,
        )
    except requests.RequestException:
        return 0, ""

    return response.status_code, (response.text or "")

def parse_optional_uncertainty(row):
    raw = None
    if "uncertainty" in row:
        raw = row.get("uncertainty")
    elif "Uncertainty" in row:
        raw = row.get("Uncertainty")
    if raw is None:
        return None
    if isinstance(raw, str):
        raw = raw.strip()
    if raw in ("", None):
        return None
    try:
        if isinstance(raw, (int, float)):
            return raw
        text_val = str(raw)
        if text_val.isdigit() or (text_val.startswith("-") and text_val[1:].isdigit()):
            return int(text_val)
        return float(text_val)
    except ValueError:
        return str(raw)

def parse_optional_impact(row):
    raw = None
    if "impact" in row:
        raw = row.get("impact")
    elif "Impact" in row:
        raw = row.get("Impact")
    if raw is None:
        return None
    if isinstance(raw, str):
        raw = raw.strip()
    if raw in ("", None):
        return None
    try:
        if isinstance(raw, (int, float)):
            return int(raw)
        return int(str(raw))
    except ValueError:
        print(f"Warning: Could not parse impact as int: {raw}")
        return None

def normalize_optional_string(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
    if not value:
        return None
    return str(value)

def normalize_effect(value):
    return normalize_optional_string(value)

def parse_record_ref(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = RECORD_REF_PATTERN.match(text)
    if not match:
        raise ValueError(f"Invalid record reference '{value}'. Expected format like IQA-2 or MA-1.")
    record_type = str(match.group(1)).strip().upper()
    if record_type not in VALID_RECORD_TYPES:
        raise ValueError(f"Unsupported record type in '{value}'.")
    return {
        "type": record_type,
        "id": int(match.group(2)),
        "raw": text,
    }

def get_full_record(target_ref):
    config = RECORD_CONFIG[target_ref["type"]]
    path = build_api_path(f"editables/{config['endpoint']}/{target_ref['id']}")
    status, data = request("GET", path)
    if 200 <= status < 300:
        return json.loads(data)
    print(f"Not updated {config['label']} {target_ref['id']}")
    return None

def update_record(target_ref, record_data):
    config = RECORD_CONFIG[target_ref["type"]]
    path = build_api_path(f"editables/{config['endpoint']}/addOrEdit")
    body = json.dumps(record_data)
    status, _ = request("PUT", path, body=body)
    if 200 <= status < 300:
        print(f"Updated {config['label']} {target_ref['id']}")
    else:
        print(f"Not updated {config['label']} {target_ref['id']}")

def get_general_attribute_name(ga_id):
    path = build_api_path(f"editables/GeneralAttribute/{ga_id}")
    status, data = request("GET", path)
    if 200 <= status < 300:
        try:
            ga = json.loads(data)
        except json.JSONDecodeError:
            return ""
        return ga.get("name", "")
    return ""

def build_link_payload(target_ref, linked_ref, justification, effect, impact, uncertainty, link_spec):
    config = RECORD_CONFIG[target_ref["type"]]
    justification_value = normalize_optional_string(justification) or ""
    if link_spec.get("payload_type") == "ga_risk":
        link_payload = {
            "uuid": str(uuid.uuid4()),
            "justification": justification_value,
            "links": "[]",
            "typeCode": "GA",
            "source": link_spec["source"],
            "GeneralAttributeId": linked_ref["id"],
            "GeneralAttribute": {
                "id": linked_ref["id"],
                "name": get_general_attribute_name(linked_ref["id"]),
                "typeCode": "GA",
            },
        }
        if impact is not None:
            link_payload["impact"] = impact
        if uncertainty is not None:
            link_payload["uncertainty"] = uncertainty
        return link_payload

    link_payload = {
        "justification": justification_value,
        "links": "[]",
        link_spec["id_key"]: linked_ref["id"],
        config["owner_id_key"]: target_ref["id"],
    }

    if impact is not None:
        link_payload["impact"] = impact

    effect_value = normalize_effect(effect)
    if effect_value is not None:
        link_payload["effect"] = effect_value

    uncertainty_key = config.get("uncertainty_key")
    if uncertainty is not None:
        link_payload[uncertainty_key] = uncertainty

    target_id_key = link_spec.get("target_id_key")
    if target_id_key:
        link_payload[target_id_key] = linked_ref["id"]

    return link_payload

def append_link_if_missing(record_data, target_ref, linked_ref, justification, effect, impact, uncertainty):
    config = RECORD_CONFIG[target_ref["type"]]
    link_spec = config["link_specs"].get(linked_ref["type"])
    if not link_spec:
        print(
            f"Skipping unsupported link: {config['label']} {target_ref['id']} "
            f"cannot link to {linked_ref['type']} {linked_ref['id']}."
        )
        return False

    field_name = link_spec["field"]
    if field_name not in record_data or record_data[field_name] is None:
        record_data[field_name] = []

    id_key = link_spec["id_key"]
    target_id_key = link_spec.get("target_id_key")
    already_linked = any(
        link.get(id_key) == linked_ref["id"] or (target_id_key and link.get(target_id_key) == linked_ref["id"])
        for link in record_data[field_name]
    )
    if already_linked:
        print(
            f"{config['label']} {target_ref['id']} already linked to "
            f"{linked_ref['type']} {linked_ref['id']}. Skipping."
        )
        return False

    record_data[field_name].append(
        build_link_payload(
            target_ref=target_ref,
            linked_ref=linked_ref,
            justification=justification,
            effect=effect,
            impact=impact,
            uncertainty=uncertainty,
            link_spec=link_spec,
        )
    )
    return True

def add_links_to_record(target_ref, linked_record, justification, effect, impact, uncertainty):
    record_data = get_full_record(target_ref)
    if not record_data:
        return

    updated = append_link_if_missing(
        record_data=record_data,
        target_ref=target_ref,
        linked_ref=linked_record,
        justification=justification,
        effect=effect,
        impact=impact,
        uncertainty=uncertainty,
    )

    if updated:
        update_record(target_ref, record_data)
    else:
        print(f"Not updated {RECORD_CONFIG[target_ref['type']]['label']} {target_ref['id']}")

def find_first_non_blank(row, keys):
    for key in keys:
        value = row.get(key)
        if isinstance(value, str):
            value = value.strip()
        if value not in ("", None):
            return value
    return None

def parse_generic_target(row):
    target_value = find_first_non_blank(row, (TARGET_RECORD_COLUMN,))
    if target_value is None:
        return None
    target_ref = parse_record_ref(target_value)
    if target_ref["type"] not in RECORD_CONFIG:
        raise ValueError(
            f"Unsupported target record '{target_ref['raw']}'. "
            "Only MA, PP, IPA, IQA, FPA, and FQA are valid target record types."
        )
    return target_ref

def parse_generic_linked_record(row):
    value = row.get(LINKED_RECORD_COLUMN)
    if isinstance(value, str):
        value = value.strip()

    if value in ("", None):
        raise ValueError("Missing linked_record. Expected a value like FPA-10, FQA-11, IPA-12, IQA-13, or GA-14.")

    return parse_record_ref(value)

def parse_row(row):
    target_ref = parse_generic_target(row)
    if not target_ref:
        raise ValueError("Missing target_record. Expected a value like IQA-2, IPA-3, MA-1, PP-4, FPA-5, or FQA-6.")

    linked_record = parse_generic_linked_record(row)

    justification = row.get("justification")
    if justification is None:
        justification = row.get("jutification")

    return {
        "target_ref": target_ref,
        "linked_record": linked_record,
        "justification": normalize_optional_string(justification),
        "effect": normalize_effect(row.get("effect")),
        "impact": parse_optional_impact(row),
        "uncertainty": parse_optional_uncertainty(row),
    }

def read_csv(file_path):
    links = []
    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row_number, row in enumerate(reader, start=2):
            try:
                links.append(parse_row(row))
            except ValueError as error:
                print(f"Skipping row {row_number} due to invalid data: {error}. Row: {row}")
    return links

def main():
    validate_config()
    links_to_add = read_csv(CSV_FILE)

    for link in links_to_add:
        add_links_to_record(
            target_ref=link["target_ref"],
            linked_record=link["linked_record"],
            justification=link.get("justification"),
            effect=link.get("effect"),
            impact=link.get("impact"),
            uncertainty=link.get("uncertainty"),
        )

if __name__ == "__main__":
    main()
