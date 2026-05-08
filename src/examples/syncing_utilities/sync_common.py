import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict

import requests


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

def setup_file_logging(
    *,
    log_dir: str,
    filename_prefix: str,
    src_id: int,
    tgt_id: int | str | None = None,
) -> str:
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if tgt_id is None:
        tgt_id = "unknown"
    log_path = os.path.join(log_dir, f"{filename_prefix}_src{src_id}_tgt{tgt_id}_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return log_path

def load_id_map_file(path: str, root_key: str, *, preserve_extra: bool = False) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {root_key: {}}

    if isinstance(data, dict) and isinstance(data.get(root_key), dict):
        return data if preserve_extra else {root_key: data[root_key]}
    return {root_key: {}}

def save_id_map_file(path: str, id_map: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(id_map, f, indent=2)

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

    def url(self, path: str) -> str:
        return self.base_url + path.lstrip("/")

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
            logging.getLogger(__name__).debug("Saving %s: %s", record_type, reason)
        return self.client.save_record(record_type, payload)

    def save_fn(self, record_type: str, *, reason: str | None = None):
        return lambda payload: self.save_record(record_type, payload, reason=reason)

def strip_attachment_links(payload: Dict[str, Any]) -> Dict[str, Any]:
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
                            obj[k] = json.dumps([
                                item for item in data
                                if not (isinstance(item, dict) and item.get("linkType") == "Attachment")
                            ])
                        continue

                    if isinstance(v, list):
                        obj[k] = [
                            item for item in v
                            if not (isinstance(item, dict) and item.get("linkType") == "Attachment")
                        ]
                    continue

                if isinstance(v, (dict, list)):
                    stack.append(v)
            continue

        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return payload

def sanitize_payload(src: dict, allowed_fields: list, extra_fields: dict | None = None) -> dict:
    payload = {k: src[k] for k in allowed_fields if k in src}
    if extra_fields:
        payload.update(extra_fields)
    return strip_attachment_links(payload)

def is_archived(record: dict) -> bool:
    return isinstance(record, dict) and record.get("currentState") == "Archived"

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

def parse_json_container(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return value

def parsed_list_or_none(value) -> list | None:
    parsed = parse_json_container(value)
    return parsed if isinstance(parsed, list) else None

def parsed_dict_or_none(value) -> dict | None:
    parsed = parse_json_container(value)
    return parsed if isinstance(parsed, dict) else None

def acceptance_criteria_ranges_from_container(container) -> list | None:
    container = parse_json_container(container)
    if not container:
        return None
    for field in ("AcceptanceCriteriaRanges", "AcceptanceCriteriaRangeLinkedVersions"):
        ranges = parsed_list_or_none(container.get(field)) if isinstance(container, dict) else None
        if ranges is not None:
            return ranges
    return None

def normalize_acceptance_criteria_ranges_list(
    ranges: list,
    fields: list,
    *,
    normalize_values: bool = True,
    include_missing: bool = True,
) -> list:
    cleaned = []
    for r in ranges:
        if not isinstance(r, dict):
            continue
        item = {}
        for k in fields:
            if not include_missing and k not in r:
                continue
            value = r.get(k)
            item[k] = normalize_acr_value(value) if normalize_values else value
        cleaned.append(item)

    def _sortable(v):
        if v is None:
            return (0, "")
        return (1, str(v))

    def _key(item):
        return tuple(_sortable(item.get(f)) for f in fields)

    return sorted(cleaned, key=_key)

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

def normalize_whitespace(text: str):
    collapsed = " ".join(text.split())
    return collapsed if collapsed else None

def normalize(val):
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

def get_target_supplier_by_name(client: QbdApiClient, name):
    data = client.list_records("Supplier")
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

def clean_supplier_payload(src_supplier: dict, allowed_fields: list) -> dict:
    return sanitize_payload(src_supplier, allowed_fields)

def create_target_supplier(
    writer: SyncWriter,
    src_supplier: dict,
    allowed_fields: list,
) -> int:
    cleaned_payload = clean_supplier_payload(src_supplier, allowed_fields)

    if not cleaned_payload.get("name"):
        raise ValueError("Supplier payload missing name")

    supplier = writer.save_record("Supplier", cleaned_payload, reason="create Supplier")
    return supplier["id"]

def resolve_target_supplier_id(
    src_client: QbdApiClient,
    tgt_client: QbdApiClient,
    writer: SyncWriter,
    src_supplier_id: int,
    supplier_cache: dict | None,
    allowed_fields: list,
    *,
    logger: logging.Logger | None = None,
) -> int | None:
    if not src_supplier_id:
        return None

    log = logger or logging.getLogger(__name__)
    supplier_cache = supplier_cache or {}
    by_id = supplier_cache.setdefault("by_id", {})
    by_name = supplier_cache.setdefault("by_name", {})

    cache_key = str(src_supplier_id)
    if cache_key in by_id:
        return by_id[cache_key]

    src_supplier = src_client.get_record("Supplier", src_supplier_id)
    if is_archived(src_supplier):
        log.info(
            "Skipping archived Supplier '%s' (%s)",
            src_supplier.get("name"),
            src_supplier_id,
        )
        return None

    name = src_supplier.get("name") if isinstance(src_supplier, dict) else None
    if not name:
        log.warning("Supplier id %s missing name; skipping remap", src_supplier_id)
        return None

    if name in by_name:
        tgt_id = by_name[name]
    else:
        tgt_id = get_target_supplier_by_name(tgt_client, name)
        if tgt_id:
            log.info(
                "Mapped existing Supplier '%s': %s -> %s",
                name,
                src_supplier_id,
                tgt_id,
            )
        else:
            log.info("Supplier '%s' not found in target; creating it", name)
            tgt_id = create_target_supplier(writer, src_supplier, allowed_fields)
            log.info(
                "Created new Supplier '%s': %s -> %s",
                name,
                src_supplier_id,
                tgt_id,
            )
        by_name[name] = tgt_id

    by_id[cache_key] = tgt_id
    return tgt_id