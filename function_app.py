import os
import io
import re
import json
import base64
import logging
import mimetypes
from datetime import datetime
import time
import uuid
import requests
import xml.etree.ElementTree as ET
from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy,
)
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import ResourceExistsError
from typing import Optional, Dict, Any, List
import azure.functions as func
from azure.storage.blob import BlobServiceClient

from PIL import Image

# ── json_repair: generic fallback net for anything the schema constraint
#    and stutter-strip don't catch (e.g. MAX_TOKENS truncation mid-object) ────
from json_repair import repair_json

# ── google-genai SDK (replaces vertexai SDK) ─────────────────────────────
from google import genai
from google.genai import types

SERIAL_ONLY_SUPPLIERS = {
    "515057206",  # הראל ייבוא ושיווק - Apple/Nokia importer
}

# ──────────────────────────────────────────────────────────────────────────────
# Azure Function
# ──────────────────────────────────────────────────────────────────────────────
app = func.FunctionApp()

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

# ── Vertex AI credentials (FALLBACK is tried FIRST) ──────────────────────────
FALLBACK_CREDS_BLOB = os.getenv(
    "FALLBACK_CREDS_BLOB",
    "https://kabutatestfunction.blob.core.windows.net/credentials/kabuta-backend-backup-2134236604a3.json",
)
PRIMARY_CREDS_BLOB = os.getenv(
    "PRIMARY_CREDS_BLOB",
    "https://kabutatestfunction.blob.core.windows.net/credentials/kabuta-f2a1b-8facf2e97ddd.json",
)

FALLBACK_PROJECT_ID = os.getenv("FALLBACK_PROJECT_ID", "kabuta-backend-backup")
PRIMARY_PROJECT_ID  = os.getenv("PRIMARY_PROJECT_ID",  "kabuta-f2a1b")

# ── GEMINI 3 MODEL — global endpoint required ─────────────────────────────────
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "global")
MODEL_NAME      = os.getenv("VERTEX_MODEL_NAME", "gemini-3.5-flash")
# MODEL_NAME      = os.getenv("VERTEX_MODEL_NAME", "gemini-3.5-pro-preview")

# ── Comax API ─────────────────────────────────────────────────────────────────
COMAX_SOAP_URL = os.getenv(
    "COMAX_SOAP_URL",
    "http://ws.comax.co.il/WS_WRK/Work_Comax_WS/OCRDocument_Service.asmx",
)
COMAX_LOGIN_ID       = os.getenv("COMAX_LOGIN_ID", "")
COMAX_LOGIN_PASSWORD = os.getenv("COMAX_LOGIN_PASSWORD", "")

# ── Azure Storage ─────────────────────────────────────────────────────────────
AZURE_STORAGE_CONNECTION_STRING = (
    os.getenv("AzureWebJobsStorage")
)
BLOB_CONTAINER_NAME    = "document-uploads"
RESULTS_CONTAINER_NAME = "processing-results"
PROCESSING_QUEUE_NAME  = "processing-queue"
AI_RAW_REPONSE = "ai-raw"

# ── Execution Log (Azure Table Storage) ───────────────────────────────────────
EXECUTION_LOG_TABLE_NAME = os.getenv("EXECUTION_LOG_TABLE_NAME", "ExecutionLog")

# ── Processor endpoint (kept for reference) ───────────────────────────────────
PROCESSOR_ENDPOINT = os.getenv(
    "PROCESSOR_ENDPOINT",
    "https://comax-genenral-backend.azurewebsites.net/api/process_document_queue",
)

# ──────────────────────────────────────────────────────────────────────────────
# Multi-page PNG splitting
# ──────────────────────────────────────────────────────────────────────────────
# A stitched multi-page PNG is very tall relative to its width. We detect the
# horizontal dark bands between pages (footers/headers butting together) and cut
# there; if none are found we fall back to fixed A4-ratio slicing. Each segment
# overlaps its neighbour slightly (SEAM_PAD) so a row split across the cut is
# still fully visible in at least one segment — the prompt tells Gemini to
# de-duplicate overlapping rows.
# ──────────────────────────────────────────────────────────────────────────────

_STITCHED_THRESHOLD = 1.9      # aspect ratio (h/w) above which we suspect >1 page
_PAGE_ASPECT        = 1.414    # A4 portrait h/w, used for fixed-slice fallback
_MAX_SEGMENTS       = 8
_SEAM_PAD_FRACTION  = 0.02


def _find_page_seams(img: Image.Image) -> List[int]:
    """Find y-coordinates of the gaps between stitched pages via dark row bands."""
    gray = img.convert("L")
    w, h = gray.size
    # Collapse each row to a single average brightness value.
    rows = list(gray.resize((1, h)).getdata())

    paper_level = sorted(rows)[int(len(rows) * 0.75)]
    dark_thresh = paper_level * 0.55

    bands, start = [], None
    for y, v in enumerate(rows):
        if v < dark_thresh:
            if start is None:
                start = y
        elif start is not None:
            bands.append((start, y))
            start = None
    if start is not None:
        bands.append((start, h))

    min_band = max(8, h // 400)
    min_page = int(h * 0.25)
    seams = [
        (a + b) // 2
        for (a, b) in bands
        if (b - a) >= min_band and a > min_page and b < (h - min_page)
    ]
    merged: List[int] = []
    for s in seams:
        if not merged or s - merged[-1] > min_page:
            merged.append(s)
    return merged[: _MAX_SEGMENTS - 1]


def _split_tall_image(file_content: bytes) -> List[bytes]:
    """
    Split a stitched multi-page image into per-page PNG byte segments.
    Returns a single-element list (the original bytes) when the image is a
    single page or cannot be opened.
    """
    try:
        img = Image.open(io.BytesIO(file_content))
        img.load()
    except Exception as e:
        logging.warning(f"[Split] Could not open image, sending as-is: {e}")
        return [file_content]

    w, h = img.size
    aspect = h / w if w else 0
    if aspect <= _STITCHED_THRESHOLD:
        logging.info(f"[Split] {w}x{h} (aspect {aspect:.2f}) — single page, no split")
        return [file_content]

    seams = _find_page_seams(img)
    if seams:
        cuts = [0] + seams + [h]
        logging.info(f"[Split] {w}x{h}: detected {len(seams)} seam(s) at y={seams}")
    else:
        n = max(2, min(_MAX_SEGMENTS, round(aspect / _PAGE_ASPECT)))
        cuts = [int(h * i / n) for i in range(n + 1)]
        logging.info(f"[Split] {w}x{h}: no seams found, fixed slicing into {n}")

    pad = int(h * _SEAM_PAD_FRACTION)
    segments: List[bytes] = []
    for i in range(len(cuts) - 1):
        top    = max(0, cuts[i] - pad)
        bottom = min(h, cuts[i + 1] + pad)
        crop = img.crop((0, top, w, bottom))
        buf = io.BytesIO()
        crop.save(buf, format="PNG")
        segments.append(buf.getvalue())
        logging.info(f"[Split] Segment {i+1}: rows {top}-{bottom}")
    return segments


# ──────────────────────────────────────────────────────────────────────────────
# Schema and Prompt
# ──────────────────────────────────────────────────────────────────────────────
STRICT_SCHEMA = {
    "supplier_account": "",
    "supplier_name": "",
    "tax_id": "",
    "receiver_name": "",
    "receiver_tax_id": "",
    "receiver_address": "",
    "supplier_document": "",
    "document_date": "",
    "due_date": "",
    "notes": "",
    "warehouse": "",
    "warehouse_name": "",
    "purchase_order": "",
    "total": "",
    "discount_percent": "",
    "discount": "",
    "total_before_vat": "",
    "vat_percent": "",
    "vat_amount": "",
    "total_including_vat": "",
    "line_items": [
        {
            "line_number": "",
            "item_name": "",
            "barcode": "",
            "quantity": "",
            "unit_price": "",
            "discount_percent": "",
            "amount": "",
            "bonus_item": "",
            "line_note": "",
            "return_reason": "",
            "line_reference": "",
            "batch_series": "",
            "expiry_date": "",
            "production_date": "",
            "manufacturer_code": "",
            "serial_number": "",
        }
    ],
}

EXTRACTION_PROMPT = """
You are an information extraction engine for invoices/receipts.
Return ONLY valid JSON in this EXACT structure and keys.
Do not add extra keys, comments or text.
If a field does not exist, leave it as an empty string ("") or empty list ([]).

IMPORTANT: For all numeric fields (amounts, prices, percentages), extract the raw numeric value WITHOUT any formatting:
- Remove commas, currency symbols, and thousands separators
- Use decimal point (.) for decimals
- Examples: "4,992.52" → "4992.52", "$1,234.56" → "1234.56", "15%" → "15"
- "warehouse" field MUST return a numeric warehouse code (integer). NEVER return warehouse names or Hebrew text. If no warehouse code exists, return an empty string.
- DATE PARSING:
  - Source invoice dates are usually in Israeli/European day-first format:
    DD/MM/YYYY, DD/MM/YY, DD.MM.YYYY, or DD.MM.YY.
  - ALWAYS interpret numeric dates as DAY-FIRST: DD/MM/YY or DD/MM/YYYY.
  - The first number is the DAY, the second number is the MONTH, the last number is the YEAR.
  - For two-digit years, assume 20xx unless impossible.
  - Example: "24/06/26" = 24 June 2026 → output "2026-06-24".
  - Example: "06/06/22" = 6 June 2022 → output "2022-06-06".
  - NEVER interpret "24/06/26" as "2024-06-26".
  - Output all dates only in ISO 8601 full format: YYYY-MM-DD.
- receiver_name is the CUSTOMER the invoice is addressed TO — the recipient /
  "ship to" / "bill to" party (often after "לכבוד" or "לקוח") who RECEIVES the
  goods. This is NEVER the supplier/sender. Do not put the supplier here.
- receiver_tax_id is that recipient's ח.פ / ע.מ / עוסק מורשה number (the
  RECEIVER's, not the sender's). If not shown, "".
- receiver_address is that recipient's full address (street, city). If absent, "".
- supplier_document is the invoice id or delivery note id. Extract as a string.
- BARCODE VS SERIAL_NUMBER:
   - If the code in the product line is purely numeric and fits a standard format, map it to "barcode".
   - CRITICAL: If the code contains LETTERS (e.g., "SH3RHP...") the entire column should be mapped it to "serial_number" instead of "barcode".
- line_number should be sequential, if you see skips, it's probably item codes or SKUs, not line numbers.
- tax_id - ONLY REFER TO THE SENDER of the invoice, not the reciever of the invoice.
  It's usually next to עוסק מורשה ,ח.פ, ח"פ, ע"מ, but NEVER return the receiver's tax_id.
- CRITICAL: Serial numbers may appear BELOW the main table as a separate list/block,
  often highlighted or labeled "מספרים סידוריים" or similar.
  Extract ALL of them into the serial_number field of the relevant line item.
  They are comma-separated or line-separated alphanumeric codes.
  The barcode column in the table row (e.g. "61") should still be extracted as barcode,
  even if serial numbers also exist for that line.
  quantity should come from the כמות column, NOT the barcode column.
- SERIAL_NUMBER FORMATTING (IMPORTANT — many documents have 50-150+ serials per line):
  - Write serial_number as a SINGLE JSON string value, never an array.
  - Format: open the string with a single quote, then list every serial number
    separated by ", " (comma + space), then close with a single quote.
    Example: "serial_number": "SF7YD70NYWV, SKWH4P34XWL, SKTWVCGJ7H9"
  - Do NOT add a closing quote, line break, or any character after the LAST
    serial number other than the single closing quote for the JSON string.
  - Do NOT repeat, restate, or re-emit any serial number or partial serial
    number after the string has been closed.
  - Write the full list in one continuous pass, in the exact order they
    appear in the source document. Do not stop partway and restart.
  - If the list is very long, stay mechanical and consistent — do not vary
    formatting mid-list.
- MULTI-SEGMENT INPUT:
  - The document may be provided as MULTIPLE image segments in top-to-bottom
    reading order. Treat all segments as ONE continuous document.
  - Segment edges overlap slightly: if a table row appears at the bottom of
    one segment and again at the top of the next, extract it ONCE only.
  - Each page may repeat the supplier header — extract header fields once.
  - The totals/VAT block usually appears only on the LAST page/segment;
    header fields (supplier, tax_id, document_date) usually only on the FIRST.
  - line_number must remain a single continuous sequence across all segments.

Schema:
{schema}
""".format(
    schema=json.dumps(STRICT_SCHEMA, ensure_ascii=False, indent=2)
)


#    - If barcode doesn't exist, use item code as a fallback if available.


# ──────────────────────────────────────────────────────────────────────────────
# Response Schema (grammar-constrained decoding)
# ──────────────────────────────────────────────────────────────────────────────
# This forces Gemini's decoder to only emit tokens that keep the output on a
# path toward valid JSON matching this shape (keys, brackets, commas, closing
# quotes are structurally guaranteed). It does NOT stop the model from looping
# on *content* inside a string value, which is why we still keep the
# stutter-strip + json_repair fallbacks below as a safety net.
# ──────────────────────────────────────────────────────────────────────────────

def _build_response_schema() -> types.Schema:
    line_item_schema = types.Schema(
        type=types.Type.OBJECT,
        properties={
            "line_number":        types.Schema(type=types.Type.STRING),
            "item_name":          types.Schema(type=types.Type.STRING),
            "barcode":            types.Schema(type=types.Type.STRING),
            "quantity":           types.Schema(type=types.Type.STRING),
            "unit_price":         types.Schema(type=types.Type.STRING),
            "discount_percent":   types.Schema(type=types.Type.STRING),
            "amount":             types.Schema(type=types.Type.STRING),
            "bonus_item":         types.Schema(type=types.Type.STRING),
            "line_note":          types.Schema(type=types.Type.STRING),
            "return_reason":      types.Schema(type=types.Type.STRING),
            "line_reference":     types.Schema(type=types.Type.STRING),
            "batch_series":       types.Schema(type=types.Type.STRING),
            "expiry_date":        types.Schema(type=types.Type.STRING),
            "production_date":    types.Schema(type=types.Type.STRING),
            "manufacturer_code":  types.Schema(type=types.Type.STRING),
            "serial_number":      types.Schema(type=types.Type.STRING),  # stays a plain string
        },
    )
    return types.Schema(
        type=types.Type.OBJECT,
        properties={
            "supplier_account":     types.Schema(type=types.Type.STRING),
            "supplier_name":        types.Schema(type=types.Type.STRING),
            "tax_id":               types.Schema(type=types.Type.STRING),
            "receiver_tax_id":      types.Schema(type=types.Type.STRING),
            "receiver_name":        types.Schema(type=types.Type.STRING),
            "receiver_address":     types.Schema(type=types.Type.STRING),
            "supplier_document":    types.Schema(type=types.Type.STRING),
            "document_date":        types.Schema(type=types.Type.STRING),
            "due_date":             types.Schema(type=types.Type.STRING),
            "notes":                types.Schema(type=types.Type.STRING),
            "warehouse":            types.Schema(type=types.Type.STRING),
            "warehouse_name":       types.Schema(type=types.Type.STRING),
            "purchase_order":       types.Schema(type=types.Type.STRING),
            "total":                types.Schema(type=types.Type.STRING),
            "discount_percent":     types.Schema(type=types.Type.STRING),
            "discount":             types.Schema(type=types.Type.STRING),
            "total_before_vat":     types.Schema(type=types.Type.STRING),
            "vat_percent":          types.Schema(type=types.Type.STRING),
            "vat_amount":           types.Schema(type=types.Type.STRING),
            "total_including_vat":  types.Schema(type=types.Type.STRING),
            "line_items":           types.Schema(type=types.Type.ARRAY, items=line_item_schema),
        },
    )


RESPONSE_SCHEMA = _build_response_schema()


# ──────────────────────────────────────────────────────────────────────────────
# Utility Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _detect_mime_from_name(file_name: str) -> str:
    m = mimetypes.guess_type(file_name or "")[0]
    if m:
        return m
    name = (file_name or "").lower()
    if name.endswith(".pdf"):
        return "application/pdf"
    if name.endswith((".jpg", ".jpeg")):
        return "image/jpeg"
    if name.endswith(".png"):
        return "image/png"
    return "application/octet-stream"


def _normalize_date(val: str) -> Optional[str]:
    """Return ISO 8601 'YYYY-MM-DDThh:mm:ss' or None if invalid/empty."""
    if not val:
        return None
    val = val.strip()
    m = re.match(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", val)
    if m:
        d, mth, y = m.groups()
        y = y if len(y) == 4 else f"20{y.zfill(2)}"
        try:
            dt = datetime(int(y), int(mth), int(d))
            return dt.strftime("%Y-%m-%dT00:00:00")
        except ValueError:
            return None
    if re.match(r"\d{4}-\d{2}-\d{2}$", val):
        return val + "T00:00:00"
    return None


def _parse_iso(val: Optional[str]) -> Optional[datetime]:
    """Best-effort ISO-8601 string -> datetime, else None."""
    if not val:
        return None
    try:
        return datetime.fromisoformat(val)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# JSON Repair Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _strip_trailing_stutter(text: str) -> str:
    """
    Fixes the specific Gemini repetition-degeneration artifact where a long
    comma-separated string value ends correctly, but the model then emits a
    few stray partial-repeat fragments before finally terminating the
    response, e.g.:

        ...SJ4CWH9V3V"
        VWH9V3V"
        VHW9V3V"

    Each stray line is a suffix of the token that precedes it and is not
    valid JSON on its own. We detect a closing quote followed by one or more
    short "junk" lines that look like partial-word repeats (no JSON
    structural characters: no ':', '{', '[', and no legitimate new key),
    and drop them.
    """
    lines = text.split("\n")
    cleaned = []
    i = 0
    while i < len(lines):
        line = lines[i]
        cleaned.append(line)
        stripped = line.rstrip()
        if stripped.endswith('"') and not stripped.endswith('\\"'):
            j = i + 1
            while j < len(lines):
                junk = lines[j].strip()
                if re.fullmatch(r'[A-Za-z0-9]{1,20}"?,?', junk) and junk:
                    j += 1  # skip this junk line
                else:
                    break
            i = j
            continue
        i += 1
    return "\n".join(cleaned)


def _recover_json(text: str, request_id: str, attempt: int):
    """
    Multi-layer recovery for malformed JSON coming back from Gemini.
    Layer 1: targeted stutter-strip (cheap, precise for the known artifact).
    Layer 2: json_repair (generic fallback for anything else, e.g. truncation).
    Raises the original-style RuntimeError if both fail.
    """
    # ── Layer 1: targeted stutter-strip ────────────────────────────────────────
    try:
        destuttered = _strip_trailing_stutter(text)
        result = json.loads(destuttered)
        logging.warning(f"[Vertex] Recovered JSON via stutter-strip (attempt {attempt})")
        save_raw_ai_response(request_id, destuttered, attempt, "destuttered")
        return result
    except Exception:
        pass

    # ── Layer 2: generic json_repair fallback ──────────────────────────────────
    try:
        repaired = repair_json(text)
        result = json.loads(repaired)
        logging.warning(f"[Vertex] Recovered JSON via json_repair (attempt {attempt})")
        save_raw_ai_response(request_id, repaired, attempt, "repaired")
        return result
    except Exception as repair_err:
        logging.error(f"[Vertex] json_repair failed too: {repair_err}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Comax XML Helpers
# ──────────────────────────────────────────────────────────────────────────────

COMAX_NS_SOAP = "http://schemas.xmlsoap.org/soap/envelope/"
COMAX_NS_BODY = "http://tempuri.org/"


def _to_number_or_none(v: Any) -> Optional[str]:
    """Return numeric string if valid, otherwise None."""
    try:
        if v in (None, "", " ", "null", "None"):
            return None
        v = str(v).replace(",", "").strip()
        if not v:
            return None
        float(v)
        return v
    except Exception:
        return None


def _normalize_bool_byte(val: Any) -> str:
    if str(val).lower() in {"true", "1", "yes"}:
        return "1"
    return "0"


def _add_text(child_of, tag, value):
    el = ET.SubElement(child_of, tag)
    el.text = "" if value is None else str(value)
    return el


def _add_text_if_present(child_of, tag, value):
    if value is not None:
        el = ET.SubElement(child_of, tag)
        el.text = str(value)
        return el
    return None


def _build_comax_xml(call_id: str, data: dict) -> str:
    """Build SOAP XML envelope for UpdateDocumentKABUTA."""
    ns_soap = COMAX_NS_SOAP
    ns_body = COMAX_NS_BODY

    ET.register_namespace("", COMAX_NS_BODY)
    env  = ET.Element(ET.QName(ns_soap, "Envelope"))
    body = ET.SubElement(env, ET.QName(ns_soap, "Body"))
    root = ET.SubElement(body, ET.QName(ns_body, "UpdateDocumentKABUTA"))

    request_el = ET.SubElement(root, "request")
    _add_text(request_el, "RequestId", call_id)

    # ── String fields (include when non-empty) ────────────────────────────────
    string_fields = {
        "supplier_document": "SupplierDocument",
        "supplier_name":     "SupplierName",
        "notes":             "Notes",
        "warehouse_name":    "WarehouseName",
        "purchase_order":    "PurchaseOrder",
    }
    for src_key, tag in string_fields.items():
        val = data.get(src_key, "")
        if val not in (None, "", []):
            _add_text(request_el, tag, val)

    # ── Numeric fields (omit if null/empty) ───────────────────────────────────
    numeric_fields = {
        "supplier_account":    "SupplierAccount",
        "tax_id":              "TaxId",
        "warehouse":           "Warehouse",
        "total":               "Total",
        "discount_percent":    "DiscountPercent",
        "discount":            "Discount",
        "total_before_vat":    "TotalBeforeVAT",
        "vat_percent":         "VATPercent",
        "vat_amount":          "VATAmount",
        "total_including_vat": "TotalIncludingVAT",
    }
    for src_key, tag in numeric_fields.items():
        val = data.get(src_key, "")
        if tag == "Warehouse":
            val = str(val).strip()
            if val and val.isdigit():
                _add_text_if_present(request_el, tag, val)
        else:
            numeric_val = _to_number_or_none(val)
            if numeric_val is not None:
                _add_text_if_present(request_el, tag, numeric_val)

    # ── Date fields ───────────────────────────────────────────────────────────
    date_fields = {
        "document_date": "DocumentDate",
        "due_date":      "DueDate",
    }
    for src_key, tag in date_fields.items():
        normalized_date = _normalize_date(data.get(src_key, ""))
        if normalized_date:
            _add_text(request_el, tag, normalized_date)

    # ── Line items ────────────────────────────────────────────────────────────
    lines_el   = ET.SubElement(request_el, "Lines")
    line_items = data.get("line_items", []) or []
    for li in line_items:
        line_el = ET.SubElement(lines_el, "KabutaDocumentLine")
        _add_text(line_el, "LineNumber", li.get("line_number", ""))

        item_name = li.get("item_name", "")
        if item_name:
            _add_text(line_el, "ItemName", item_name)

        line_numeric_fields = {
            "barcode":          "Barcode",
            "quantity":         "Quantity",
            "unit_price":       "UnitPrice",
            "discount_percent": "DiscountPercent",
            "amount":           "Amount",
        }
        for src_key, tag in line_numeric_fields.items():
            numeric_val = _to_number_or_none(li.get(src_key, ""))
            if numeric_val is not None:
                _add_text_if_present(line_el, tag, numeric_val)

        bonus = li.get("bonus_item", "")
        if bonus:
            _add_text(line_el, "BonusItem", _normalize_bool_byte(bonus))

        optional_fields = {
            "line_note":         "LineNote",
            "return_reason":     "ReturnReason",
            "line_reference":    "LineReference",
            "batch_series":      "BatchSeries",
            "expiry_date":       "ExpiryDate",
            "production_date":   "ProductionDate",
            "manufacturer_code": "ManufacturerCode",
            "serial_number":     "SerialNumber",
        }
        for src_key, tag in optional_fields.items():
            val = li.get(src_key)
            if val not in (None, "", []):
                _add_text(line_el, tag, val)

    # ── Login credentials ─────────────────────────────────────────────────────
    _add_text(root, "LoginID",       COMAX_LOGIN_ID)
    _add_text(root, "LoginPassword", COMAX_LOGIN_PASSWORD)

    xml_bytes = ET.tostring(env, encoding="utf-8", method="xml")
    return xml_bytes.decode("utf-8")


def _parse_comax_response(xml_text: str) -> dict:
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        return {"success": False, "error": f"Invalid XML response: {e}", "raw": xml_text[:2000]}

    def _find_first_by_localname(root_el, local):
        for el in root_el.iter():
            ln = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if ln == local:
                return el
        return None

    is_success_el = _find_first_by_localname(root, "IsSuccess")
    error_desc_el = _find_first_by_localname(root, "ErrorDescription")

    is_success = (
        is_success_el is not None
        and is_success_el.text
        and is_success_el.text.strip().lower() == "true"
    )
    error_desc = (
        error_desc_el.text.strip()
        if (error_desc_el is not None and error_desc_el.text)
        else ("Update completed successfully" if is_success else "")
    )
    return {
        "success": is_success,
        "error":   "" if is_success else error_desc,
        "raw":     xml_text[:2000],
    }


# ──────────────────────────────────────────────────────────────────────────────
# Vertex AI Initialisation  (google-genai SDK, FALLBACK first, PRIMARY second)
# ──────────────────────────────────────────────────────────────────────────────

# Module-level cache: holds the initialised genai.Client instance
_genai_client_cache: Optional[genai.Client] = None


def _download_sa_key(blob_url: str, project_id: str) -> str:
    """Download SA JSON from Azure Blob to /tmp and return the path."""
    resp = requests.get(blob_url, timeout=60)
    resp.raise_for_status()
    key_path = f"/tmp/{project_id}-key.json"
    with open(key_path, "wb") as f:
        f.write(resp.content)
    return key_path


def _get_genai_client() -> genai.Client:
    global _genai_client_cache
    if _genai_client_cache is not None:
        logging.info("[Vertex] ✓ Using cached genai client (skipping init)")
        return _genai_client_cache

    t0 = time.time()
    logging.info("[Vertex] Cache miss — downloading SA keys and initialising...")

    # PRIMARY first — fallback project doesn't have Gemini 3.x access
    candidates = [
        (PRIMARY_CREDS_BLOB,  PRIMARY_PROJECT_ID,  "primary"),
        (FALLBACK_CREDS_BLOB, FALLBACK_PROJECT_ID, "fallback"),
    ]
    last_err = None

    for blob_url, pid, label in candidates:
        try:
            key_path = _download_sa_key(blob_url, pid)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

            client = genai.Client(
                vertexai=True,
                project=pid,
                location="global",          # ← must be "global" for Gemini 3.x
            )

            logging.info(
                f"[Vertex] Initialised with {label} project '{pid}' "
                f"in {time.time()-t0:.2f}s"
            )
            _genai_client_cache = client
            return client

        except Exception as e:
            logging.warning(f"[Vertex] Init failed for {label} project '{pid}': {e}")
            last_err = e

    raise RuntimeError(
        f"Failed to initialise Vertex AI with both credentials. "
        f"Last error: {last_err}"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Vertex AI Analysis  (google-genai SDK)
# ──────────────────────────────────────────────────────────────────────────────

# def analyze_with_vertex_ai_strict(file_content: bytes, file_name: str, request_id: str) -> dict:
#     t_fn_start = time.time()
#     logging.info("[Vertex] analyze_with_vertex_ai_strict() started")

#     t0     = time.time()
#     client = _get_genai_client()
#     logging.info(f"[Vertex] _get_genai_client() took {time.time()-t0:.2f}s")

#     mime_type = _detect_mime_from_name(file_name)
#     logging.info(f"[Vertex] mime_type={mime_type}, file_size={len(file_content)/1024:.1f}KB")

#     # ── Build the file parts. Multi-page images are split into per-page
#     #    segments and sent as multiple inline parts in one call; PDFs are
#     #    left untouched (Gemini reads their pages natively). Sending all
#     #    segments together lets the model treat them as one document, so
#     #    headers/totals that appear on only one page are still resolved and
#     #    a single JSON object is returned. ──────────────────────────────────
#     if mime_type.startswith("image/"):
#         segments = _split_tall_image(file_content)
#         if len(segments) > 1:
#             logging.info(f"[Vertex] Image split into {len(segments)} segment(s)")
#             file_parts = [
#                 types.Part(inline_data=types.Blob(mime_type="image/png", data=seg))
#                 for seg in segments
#             ]
#         else:
#             file_parts = [
#                 types.Part(inline_data=types.Blob(mime_type=mime_type, data=file_content))
#             ]
#     else:
#         file_parts = [
#             types.Part(inline_data=types.Blob(mime_type=mime_type, data=file_content))
#         ]

#     # Build the content parts: text prompt + one-or-more inline file segments
#     contents = [
#         types.Content(
#             role="user",
#             parts=[types.Part(text=EXTRACTION_PROMPT), *file_parts],
#         )
#     ]

#     max_retries = 3
#     last_exc    = None

#     for attempt in range(max_retries):
#         # Slightly raise temperature on retries only — breaks exact-repetition
#         # loops without touching the deterministic first attempt. The
#         # response_schema constraint below is what actually prevents most
#         # structural breakage regardless of temperature.
#         cfg = types.GenerateContentConfig(
#             response_mime_type="application/json",
#             response_schema=RESPONSE_SCHEMA,   # ← grammar-constrained decoding
#             temperature=0.0 if attempt == 0 else 0.5,
#             max_output_tokens=65535,
#             thinking_config=types.ThinkingConfig(
#                 thinking_level="LOW",   # LOW / MEDIUM / HIGH
#             ),
#         )

#         try:
#             logging.info(
#                 f"[Vertex] Calling generate_content() "
#                 f"(attempt {attempt+1}/{max_retries}, model={MODEL_NAME})..."
#             )
#             t0   = time.time()
#             resp = client.models.generate_content(
#                 model=MODEL_NAME,
#                 contents=contents,
#                 config=cfg,
#             )
#             dur = time.time() - t0
#             logging.info(f"[Vertex] generate_content() returned in {dur:.2f}s")

#             # Extract text from response
#             if not resp.candidates:
#                 logging.error(f"[Vertex] Empty candidates: {resp}")
#                 raise RuntimeError("No candidates returned by Vertex.")

#             # With thinking enabled the first part may be the thought; find text part
#             text = None
#             for part in resp.candidates[0].content.parts:
#                 if hasattr(part, "text") and part.text:
#                     text = part.text.strip()
#                     break

#             if not text:
#                 logging.error(f"[Vertex] No text part found in response: {resp}")
#                 raise RuntimeError("No text content in Vertex response.")

#             logging.info(f"[Vertex] Response length={len(text)} chars")

#             t0 = time.time()
#             try:
#                 result = json.loads(text)
#                 logging.info(f"[Vertex] JSON parse took {time.time()-t0:.3f}s")
#                 logging.info(f"[Vertex] Total analyze() time: {time.time()-t_fn_start:.2f}s")
#                 save_raw_ai_response(request_id, text, attempt + 1, "success")
#                 return result
#             except json.JSONDecodeError as e:
#                 logging.error("--- VERTEX JSON DECODE ERROR ---")
#                 logging.error(f"Error: {e}  |  Line {e.lineno}, Col {e.colno}")
#                 logging.error(f"TAIL: ...{text[-500:]}" if len(text) > 500 else f"FULL: {text}")
#                 logging.error("--- END ERROR LOG ---")
#                 save_raw_ai_response(request_id, text, attempt + 1, "decode_error")

#                 # ── Recovery: stutter-strip, then json_repair ──────────────────
#                 try:
#                     result = _recover_json(text, request_id, attempt + 1)
#                     logging.info(f"[Vertex] Total analyze() time: {time.time()-t_fn_start:.2f}s")
#                     return result
#                 except Exception:
#                     raise RuntimeError(f"AI returned malformed JSON at {e.lineno}:{e.colno}")

#         except Exception as exc:
#             last_exc = exc
#             if "429" in str(exc) and attempt < max_retries - 1:
#                 wait = 10 * (2 ** attempt)
#                 logging.warning(
#                     f"[Vertex] 429 rate-limited (attempt {attempt+1}/{max_retries}), "
#                     f"retrying in {wait}s…"
#                 )
#                 time.sleep(wait)
#             elif "malformed JSON" in str(exc) and attempt < max_retries - 1:
#                 logging.warning(
#                     f"[Vertex] Malformed JSON on attempt {attempt+1}/{max_retries}, "
#                     f"retrying with higher temperature…"
#                 )
#                 continue
#             else:
#                 raise

#     raise last_exc

def analyze_with_vertex_ai_strict(file_content: bytes, file_name: str, request_id: str) -> dict:
    t_fn_start = time.time()
    logging.info("[Vertex] analyze_with_vertex_ai_strict() started")

    t0     = time.time()
    client = _get_genai_client()
    logging.info(f"[Vertex] _get_genai_client() took {time.time()-t0:.2f}s")

    mime_type = _detect_mime_from_name(file_name)
    logging.info(f"[Vertex] mime_type={mime_type}, file_size={len(file_content)/1024:.1f}KB")

    # ── Build the file parts. Multi-page images are split into per-page
    #    segments and sent as multiple inline parts in one call; PDFs are
    #    left untouched (Gemini reads their pages natively). ────────────────
    if mime_type.startswith("image/"):
        segments = _split_tall_image(file_content)
        if len(segments) > 1:
            logging.info(f"[Vertex] Image split into {len(segments)} segment(s)")
            file_parts = [
                types.Part(inline_data=types.Blob(mime_type="image/png", data=seg))
                for seg in segments
            ]
        else:
            file_parts = [
                types.Part(inline_data=types.Blob(mime_type=mime_type, data=file_content))
            ]
    else:
        file_parts = [
            types.Part(inline_data=types.Blob(mime_type=mime_type, data=file_content))
        ]

    contents = [
        types.Content(
            role="user",
            parts=[types.Part(text=EXTRACTION_PROMPT), *file_parts],
        )
    ]

    max_retries = 3
    last_exc    = None

    # Thinking level per attempt. Dense multi-page invoices can exhaust
    # max_output_tokens on thinking alone (thinking tokens count against the
    # output budget), leaving candidate.content.parts == None. Drop to LOW on
    # retry so the budget goes to the actual JSON.
    thinking_levels = ["LOW", "LOW", "MINIMAL"]

    for attempt in range(max_retries):
        cfg = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=RESPONSE_SCHEMA,   # grammar-constrained decoding
            temperature=0.0 if attempt == 0 else 0.5,
            max_output_tokens=65535,
            thinking_config=types.ThinkingConfig(
                thinking_level=thinking_levels[attempt],
            ),
        )

        try:
            logging.info(
                f"[Vertex] Calling generate_content() "
                f"(attempt {attempt+1}/{max_retries}, model={MODEL_NAME}, "
                f"thinking={thinking_levels[attempt]}, temp={cfg.temperature})..."
            )
            t0   = time.time()
            resp = client.models.generate_content(
                model=MODEL_NAME,
                contents=contents,
                config=cfg,
            )
            dur = time.time() - t0
            logging.info(f"[Vertex] generate_content() returned in {dur:.2f}s")

            # ── Diagnostics: always log finish_reason + token usage ────────────
            usage = getattr(resp, "usage_metadata", None)
            logging.info(
                "[Vertex] usage: prompt=%s thoughts=%s output=%s total=%s",
                getattr(usage, "prompt_token_count", "?"),
                getattr(usage, "thoughts_token_count", "?"),
                getattr(usage, "candidates_token_count", "?"),
                getattr(usage, "total_token_count", "?"),
            )

            if not resp.candidates:
                pf = getattr(resp, "prompt_feedback", None)
                logging.error(f"[Vertex] Empty candidates. prompt_feedback={pf}")
                save_raw_ai_response(request_id, repr(resp), attempt + 1, "empty")
                raise RuntimeError(f"EMPTY_RESPONSE: no candidates (prompt_feedback={pf})")

            cand   = resp.candidates[0]
            finish = getattr(cand, "finish_reason", None)
            logging.info(f"[Vertex] finish_reason={finish}")

            # ── content or parts can be None (MAX_TOKENS / SAFETY / RECITATION).
            #    Guard before iterating — this is what raised
            #    "'NoneType' object is not iterable". ──────────────────────────
            parts = (getattr(cand.content, "parts", None) if cand.content else None) or []
            if not parts:
                logging.error(
                    f"[Vertex] No content parts! finish_reason={finish}, usage={usage}"
                )
                save_raw_ai_response(request_id, repr(resp), attempt + 1, "empty")
                raise RuntimeError(f"EMPTY_RESPONSE: no content parts (finish_reason={finish})")

            # With thinking enabled, skip thought parts and take the first real text part.
            text = None
            for part in parts:
                if getattr(part, "thought", False):
                    continue
                if getattr(part, "text", None):
                    text = part.text.strip()
                    break

            if not text:
                logging.error(f"[Vertex] No text part found. finish_reason={finish}")
                save_raw_ai_response(request_id, repr(resp), attempt + 1, "empty")
                raise RuntimeError(f"EMPTY_RESPONSE: no text content (finish_reason={finish})")

            if str(finish).endswith("MAX_TOKENS"):
                logging.warning(
                    "[Vertex] finish_reason=MAX_TOKENS — output likely truncated, "
                    "json_repair may be needed"
                )

            logging.info(f"[Vertex] Response length={len(text)} chars")

            t0 = time.time()
            try:
                result = json.loads(text)
                logging.info(f"[Vertex] JSON parse took {time.time()-t0:.3f}s")
                logging.info(f"[Vertex] Total analyze() time: {time.time()-t_fn_start:.2f}s")
                save_raw_ai_response(request_id, text, attempt + 1, "success")
                return result
            except json.JSONDecodeError as e:
                logging.error("--- VERTEX JSON DECODE ERROR ---")
                logging.error(f"Error: {e}  |  Line {e.lineno}, Col {e.colno}")
                logging.error(f"finish_reason={finish}")
                logging.error(f"TAIL: ...{text[-500:]}" if len(text) > 500 else f"FULL: {text}")
                logging.error("--- END ERROR LOG ---")
                save_raw_ai_response(request_id, text, attempt + 1, "decode_error")

                # ── Recovery: stutter-strip, then json_repair ──────────────────
                try:
                    result = _recover_json(text, request_id, attempt + 1)
                    logging.info(f"[Vertex] Total analyze() time: {time.time()-t_fn_start:.2f}s")
                    return result
                except Exception:
                    raise RuntimeError(f"MALFORMED_JSON: at {e.lineno}:{e.colno}")

        except Exception as exc:
            last_exc = exc
            msg      = str(exc)
            is_last  = attempt >= max_retries - 1

            if "429" in msg and not is_last:
                wait = 10 * (2 ** attempt)
                logging.warning(
                    f"[Vertex] 429 rate-limited (attempt {attempt+1}/{max_retries}), "
                    f"retrying in {wait}s…"
                )
                time.sleep(wait)
                continue

            if "EMPTY_RESPONSE" in msg and not is_last:
                logging.warning(
                    f"[Vertex] Empty response on attempt {attempt+1}/{max_retries} "
                    f"({msg}) — retrying with lower thinking budget…"
                )
                continue

            if "MALFORMED_JSON" in msg and not is_last:
                logging.warning(
                    f"[Vertex] Malformed JSON on attempt {attempt+1}/{max_retries}, "
                    f"retrying with higher temperature…"
                )
                continue

            logging.error(f"[Vertex] Giving up after attempt {attempt+1}: {exc}", exc_info=True)
            raise

    raise last_exc

# ──────────────────────────────────────────────────────────────────────────────
# Blob Storage Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get_blob_service() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)


def _ensure_container(blob_service: BlobServiceClient, container_name: str) -> None:
    try:
        cc = blob_service.get_container_client(container_name)
        if not cc.exists():
            cc.create_container()
            logging.info(f"[Blob] Created container '{container_name}'")
    except Exception as e:
        logging.error(f"[Blob] Could not ensure container '{container_name}': {e}", exc_info=True)
        raise


def save_original_document(request_id: str, file_name: str, file_bytes: bytes) -> Optional[str]:
    """
    Save the raw uploaded file to blob storage.
    Always attempts to save regardless of downstream errors.
    Returns the blob path on success, None on failure.
    """
    try:
        svc       = _get_blob_service()
        _ensure_container(svc, BLOB_CONTAINER_NAME)
        blob_path = f"original/{request_id}/{file_name}"
        svc.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_path).upload_blob(
            file_bytes, overwrite=True
        )
        logging.info(f"[Blob] Saved original document → {BLOB_CONTAINER_NAME}/{blob_path}")
        return blob_path
    except Exception as e:
        logging.error(f"[Blob] Failed to save original document for {request_id}: {e}", exc_info=True)
        return None


def save_results_to_blob(request_id: str, results: Dict[str, Any]) -> None:
    """Save processing results JSON to blob storage."""
    try:
        svc = _get_blob_service()
        _ensure_container(svc, RESULTS_CONTAINER_NAME)
        blob_name = f"{request_id}.json"
        svc.get_blob_client(container=RESULTS_CONTAINER_NAME, blob=blob_name).upload_blob(
            json.dumps(results, ensure_ascii=False), overwrite=True
        )
        logging.info(f"[Blob] Saved results → {RESULTS_CONTAINER_NAME}/{blob_name}")
    except Exception as e:
        logging.error(f"[Blob] Failed to save results for {request_id}: {e}", exc_info=True)


def save_comax_xml(request_id: str, soap_xml: str) -> Optional[str]:
    """Save outgoing Comax SOAP XML to blob storage."""
    try:
        svc = _get_blob_service()
        _ensure_container(svc, RESULTS_CONTAINER_NAME)
        blob_path = f"{request_id}.xml"
        svc.get_blob_client(container=RESULTS_CONTAINER_NAME, blob=blob_path).upload_blob(
            soap_xml, overwrite=True
        )
        logging.info(f"[Blob] Saved Comax XML → {RESULTS_CONTAINER_NAME}/{blob_path}")
        return blob_path
    except Exception as e:
        logging.error(f"[Blob] Failed to save Comax XML for {request_id}: {e}", exc_info=True)
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Execution Log Helpers (Azure Table Storage)
# ──────────────────────────────────────────────────────────────────────────────

_table_service_cache: Optional[TableServiceClient] = None


def _get_table_service() -> TableServiceClient:
    global _table_service_cache
    if _table_service_cache is None:
        _table_service_cache = TableServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
    return _table_service_cache


def _get_execution_log_client():
    service = _get_table_service()
    try:
        service.create_table(EXECUTION_LOG_TABLE_NAME)
    except ResourceExistsError:
        pass
    except Exception as e:
        logging.warning(f"[ExecutionLog] create_table check failed (may already exist): {e}")
    return service.get_table_client(EXECUTION_LOG_TABLE_NAME)


def _log_partition_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def create_execution_log(request_id: str, file_name: str, received_at: datetime) -> None:
    """Create the initial ExecutionLog row the instant a document is received."""
    entity = {
        "PartitionKey":  _log_partition_key(received_at),
        "RowKey":        request_id,
        "FileName":      file_name,
        "Status":        "received",
        "ReceivedAt":    received_at.isoformat(),
    }
    try:
        client = _get_execution_log_client()
        client.create_entity(entity)
        logging.info(f"[ExecutionLog] Created entry for {request_id}")
    except Exception as e:
        logging.error(f"[ExecutionLog] Failed to create entry for {request_id}: {e}", exc_info=True)


def update_execution_log(request_id: str, received_at: datetime, updates: Dict[str, Any]) -> None:
    """
    Merge-update fields into the existing ExecutionLog row.
    received_at is required to reconstruct the PartitionKey — pass through the
    same value used in create_execution_log() (it's carried in the queue message).
    """
    entity = {
        "PartitionKey": _log_partition_key(received_at),
        "RowKey":       request_id,
        **updates,
    }
    try:
        client = _get_execution_log_client()
        client.upsert_entity(entity, mode=UpdateMode.MERGE)
        logging.info(f"[ExecutionLog] Updated entry for {request_id}: {list(updates.keys())}")
    except Exception as e:
        logging.error(f"[ExecutionLog] Failed to update entry for {request_id}: {e}", exc_info=True)


# ──────────────────────────────────────────────────────────────────────────────
# Comax SOAP Send
# ──────────────────────────────────────────────────────────────────────────────

def send_to_comax(call_id: str, data: dict) -> dict:
    soap_xml = _build_comax_xml(call_id, data)

    # Always persist the XML before sending
    save_comax_xml(call_id, soap_xml)

    url = f"{COMAX_SOAP_URL}?op=UpdateDocumentKABUTA"
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction":   "http://tempuri.org/UpdateDocumentKABUTA",
    }

    logging.info("---------- COMAX OUTGOING XML ----------")
    logging.info(soap_xml)
    logging.info("---------- END XML ----------")

    try:
        resp = requests.post(url, data=soap_xml.encode("utf-8"), headers=headers, timeout=60)
        resp.raise_for_status()
    except requests.HTTPError as e:
        return {
            "success": False,
            "error":   f"HTTP {resp.status_code}: {e}",
            "raw":     resp.text[:2000] if "resp" in dir() else "",
        }
    except Exception as e:
        return {"success": False, "error": f"Request failed: {e}"}

    return _parse_comax_response(resp.text)


# ──────────────────────────────────────────────────────────────────────────────
# Queue Helper
# ──────────────────────────────────────────────────────────────────────────────

def enqueue_processing(payload: dict) -> None:
    queue = QueueClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING, PROCESSING_QUEUE_NAME
    )
    queue.message_encode_policy = BinaryBase64EncodePolicy()
    msg_bytes = json.dumps(payload).encode("utf-8")
    queue.send_message(queue.message_encode_policy.encode(content=msg_bytes))
    logging.info(f"[Queue] Enqueued message for request_id={payload.get('request_id')}")


# ──────────────────────────────────────────────────────────────────────────────
# HTTP Trigger: analyze_document
# ──────────────────────────────────────────────────────────────────────────────

@app.route(route="analyze_document", methods=["POST"])
def analyze_document(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("[analyze_document] Received document analysis request")

    # ── 1. Read inputs ─────────────────────────────────────────────────────────
    base64_data = req.params.get("base64_file")
    file_name   = req.params.get("file_name")

    if not base64_data:
        try:
            body = req.get_json()
        except ValueError:
            body = None
        if body:
            base64_data = body.get("base64_file")
            file_name   = body.get("file_name", file_name)

    if not base64_data:
        return func.HttpResponse(
            json.dumps({"error": "Missing required parameter 'base64_file'."}),
            status_code=400,
            mimetype="application/json",
        )

    file_name = file_name or "document"

    # ── 2. Generate request ID + mark the official "received" timestamp ────────
    request_id  = uuid.uuid4().hex
    received_at = datetime.utcnow()
    logging.info(f"[analyze_document] request_id={request_id}, file={file_name}")

    # ── 2b. Create the ExecutionLog row immediately ─────────────────────────────
    create_execution_log(request_id, file_name, received_at)

    # ── 3. Decode Base64 ───────────────────────────────────────────────────────
    try:
        if base64_data.startswith("data:"):
            base64_data = base64_data.split(",", 1)[1]
        file_content = base64.b64decode(base64_data)
    except Exception as e:
        logging.error(f"[analyze_document] Invalid base64 data: {e}")
        update_execution_log(request_id, received_at, {
            "Status":       "failed",
            "ErrorMessage": f"Invalid base64 data: {e}",
        })
        return func.HttpResponse(
            json.dumps({"error": "Invalid file data supplied."}),
            status_code=400,
            mimetype="application/json",
        )

    # ── 4. ALWAYS save original to blob (before any further processing) ────────
    blob_path = save_original_document(request_id, file_name, file_content)
    if blob_path is None:
        logging.error(
            f"[analyze_document] Original file blob upload failed for {request_id}. "
            "Continuing to enqueue anyway."
        )
    else:
        logging.info(f"[analyze_document] Original file confirmed at: {blob_path}")

    # ── 5. Enqueue processing job (carry received_at so the log row can be found) ─
    queued_at = datetime.utcnow()
    enqueue_processing({
        "request_id":  request_id,
        "file_name":   file_name,
        "received_at": received_at.isoformat(),
    })

    update_execution_log(request_id, received_at, {
        "Status":    "queued",
        "BlobSaved": blob_path is not None,
        "QueuedAt":  queued_at.isoformat(),
    })

    # ── 6. Return immediately ──────────────────────────────────────────────────
    return func.HttpResponse(
        json.dumps(
            {
                "request_id": request_id,
                "status":     "queued",
                "blob_saved": blob_path is not None,
                "message":    "Document queued for processing",
            }
        ),
        status_code=202,
        mimetype="application/json",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Queue Trigger: process_document_queue
# ──────────────────────────────────────────────────────────────────────────────

@app.queue_trigger(
    arg_name="msg",
    queue_name="processing-queue",
    connection="AzureWebJobsStorage",
)
def process_document_queue(msg: func.QueueMessage) -> None:
    """
    Queue-triggered processor.
    Message body must contain:
      - request_id  : str
      - file_name   : str
      - received_at : str (ISO 8601) — when analyze_document() first accepted the file
    """
    t_total = time.time()
    logging.info("[process_document_queue] ═══ START ═══")

    request_id  = None
    file_name   = "unknown"
    received_at = None

    try:
        # ── 1. Parse queue payload ─────────────────────────────────────────────
        body        = json.loads(msg.get_body().decode("utf-8"))
        request_id  = body.get("request_id")
        file_name   = body.get("file_name", "document")
        received_at = _parse_iso(body.get("received_at")) or datetime.utcnow()

        if not request_id or not file_name:
            logging.error("[process_document_queue] Missing request_id or file_name in message")
            return

        logging.info(f"[process_document_queue] request_id={request_id}, file={file_name}")

        processing_started_at = datetime.utcnow()
        update_execution_log(request_id, received_at, {
            "Status":               "processing",
            "ProcessingStartedAt":  processing_started_at.isoformat(),
        })

        # ── 2. Retrieve original file from blob ────────────────────────────────
        svc       = _get_blob_service()
        blob_path = f"original/{request_id}/{file_name}"
        bc        = svc.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_path)

        if not bc.exists():
            logging.error(
                f"[process_document_queue] Original file not found in blob for {request_id} "
                f"at path {blob_path}"
            )
            finished_at = datetime.utcnow()
            save_results_to_blob(
                request_id,
                {
                    "request_id":   request_id,
                    "file_name":    file_name,
                    "status":       "failed",
                    "error":        "Original file not found in blob storage",
                    "processed_at": finished_at.isoformat(),
                },
            )
            update_execution_log(request_id, received_at, {
                "Status":                    "failed",
                "ErrorMessage":              "Original file not found in blob storage",
                "TotalDurationSeconds":      (finished_at - received_at).total_seconds(),
                "ProcessingDurationSeconds": (finished_at - processing_started_at).total_seconds(),
            })
            return

        file_content = bc.download_blob().readall()
        file_size_mb = len(file_content) / (1024 * 1024)
        logging.info(f"[process_document_queue] Loaded file ({file_size_mb:.2f} MB)")

        # ── 3. Vertex AI analysis ──────────────────────────────────────────────
        t0 = time.time()
        logging.info(f"[process_document_queue] Starting Vertex AI analysis for {request_id}")
        data = analyze_with_vertex_ai_strict(file_content, file_name, request_id)
        vertex_duration = time.time() - t0
        logging.info(f"[TIMER] Vertex AI total: {vertex_duration:.2f}s")

        update_execution_log(request_id, received_at, {
            "VertexAIDurationSeconds": vertex_duration,
            "Receiver":        (data.get("receiver_name") or "").strip(),
            "ReceiverTaxId":   (data.get("receiver_tax_id") or "").strip(),
            "ReceiverAddress": (data.get("receiver_address") or "").strip(),
        })

        try:
            t0   = time.time()
            data = normalize_extracted_data(data)
            logging.info(f"[TIMER] Normalize data: {time.time()-t0:.3f}s")
        except Exception as norm_err:
            logging.error(f"[process_document_queue] Normalization failed: {norm_err}")

        # ── 4. Send to Comax ───────────────────────────────────────────────────
        t0           = time.time()
        comax_result = send_to_comax(request_id, data)
        comax_duration = time.time() - t0
        logging.info(
            f"[TIMER] Comax SOAP call: {comax_duration:.2f}s | "
            f"success={comax_result.get('success')}"
        )

        accepted_at    = datetime.utcnow()
        comax_success  = bool(comax_result.get("success"))
        final_status   = "completed" if comax_success else "failed"

        # ── 5. Save final results ──────────────────────────────────────────────
        t0 = time.time()
        save_results_to_blob(
            request_id,
            {
                "request_id":   request_id,
                "file_name":    file_name,
                "status":       final_status,
                "data":         data,
                "comax_result": comax_result,
                "processed_at": accepted_at.isoformat(),
            },
        )
        logging.info(f"[TIMER] Save results blob: {time.time()-t0:.3f}s")

        # ── 6. Finalise ExecutionLog with full timing breakdown ─────────────────
        update_execution_log(request_id, received_at, {
            "Status":                    final_status,
            "ComaxDurationSeconds":      comax_duration,
            "ComaxSuccess":              comax_success,
            "ErrorMessage":              "" if comax_success else str(comax_result.get("error", "")),
            "AcceptedAt":                accepted_at.isoformat() if comax_success else "",
            "ProcessingDurationSeconds": (accepted_at - processing_started_at).total_seconds(),
            "TotalDurationSeconds":      (accepted_at - received_at).total_seconds(),
        })

        logging.info(f"[TIMER] ═══ TOTAL: {time.time()-t_total:.2f}s ═══")

    except Exception as e:
        logging.error(
            f"[process_document_queue] Unhandled error for request_id={request_id}: {e}",
            exc_info=True,
        )
        if request_id:
            finished_at = datetime.utcnow()
            try:
                save_results_to_blob(
                    request_id,
                    {
                        "request_id":   request_id,
                        "file_name":    file_name,
                        "status":       "failed",
                        "error":        str(e),
                        "processed_at": finished_at.isoformat(),
                    },
                )
            except Exception as save_err:
                logging.error(
                    f"[process_document_queue] Failed to save error result: {save_err}"
                )
            try:
                update_fields = {
                    "Status":       "failed",
                    "ErrorMessage": str(e),
                }
                if received_at:
                    update_fields["TotalDurationSeconds"] = (finished_at - received_at).total_seconds()
                update_execution_log(request_id, received_at or finished_at, update_fields)
            except Exception as log_err:
                logging.error(
                    f"[process_document_queue] Failed to update ExecutionLog on error: {log_err}"
                )


# ──────────────────────────────────────────────────────────────────────────────
# Data Normalisation
# ──────────────────────────────────────────────────────────────────────────────

def normalize_extracted_data(data: dict) -> dict:
    line_items = data.get("line_items", [])
    tax_id = str(data.get("tax_id", "")).strip()

    # ── Decide ONCE for the whole document: is this column barcodes or serials?
    # Rule 1: Known serial-only supplier → always move all to serial
    # Rule 2: ANY alphanumeric value anywhere in the barcode column → whole column is serials
    force_serial = (
        tax_id in SERIAL_ONLY_SUPPLIERS
        or any(
            any(c.isalpha() for c in str(item.get("barcode", "")))
            for item in line_items
        )
    )

    if force_serial:
        logging.info(
            f"[Normalize] Barcode column classified as SERIAL for entire document "
            f"(supplier={tax_id}, force_serial={force_serial})"
        )
        for item in line_items:
            barcode = str(item.get("barcode", "")).strip()
            serial  = str(item.get("serial_number", "")).strip()
            if barcode and not serial:
                item["serial_number"] = barcode
                item["barcode"] = ""
            elif barcode and serial:
                # Both filled — barcode column was misclassified, discard barcode
                logging.warning(
                    f"[Normalize] Line {item.get('line_number')}: both barcode='{barcode}' "
                    f"and serial='{serial}' present — discarding barcode"
                )
                item["barcode"] = ""

    return data


def save_raw_ai_response(request_id: str, raw_text: str, attempt: int, status: str) -> Optional[str]:
    """
    Persist the raw (unparsed) Vertex AI response to the 'ai-raw' container,
    a sibling of document-uploads / processing-results.
    status: 'success' | 'decode_error' | 'destuttered' | 'repaired' | 'empty'
    """
    try:
        svc = _get_blob_service()
        _ensure_container(svc, AI_RAW_REPONSE)
        blob_path = f"{request_id}_attempt{attempt}_{status}.txt"
        svc.get_blob_client(container=AI_RAW_REPONSE, blob=blob_path).upload_blob(
            raw_text, overwrite=True
        )
        logging.info(f"[Blob] Saved raw AI response → {AI_RAW_REPONSE}/{blob_path}")
        return blob_path
    except Exception as e:
        logging.error(f"[Blob] Failed to save raw AI response for {request_id}: {e}", exc_info=True)
        return None