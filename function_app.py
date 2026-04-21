# import os
# import re
# import json
# import base64
# import logging
# import mimetypes
# from datetime import datetime
# import time
# import uuid
# import requests
# import xml.etree.ElementTree as ET
# from azure.storage.queue import (
#     QueueClient,
#     BinaryBase64EncodePolicy,
#     BinaryBase64DecodePolicy,
# )
# from typing import Optional, Dict, Any
# import azure.functions as func
# from azure.storage.blob import BlobServiceClient
# import vertexai
# from vertexai.generative_models import GenerativeModel, Part, GenerationConfig

# SERIAL_ONLY_SUPPLIERS = {
#     "515057206",  # הראל ייבוא ושיווק - Apple/Nokia importer
# }

# # ──────────────────────────────────────────────────────────────────────────────
# # Azure Function
# # ──────────────────────────────────────────────────────────────────────────────
# app = func.FunctionApp()

# # ──────────────────────────────────────────────────────────────────────────────
# # Configuration
# # ──────────────────────────────────────────────────────────────────────────────

# # ── Vertex AI credentials (FALLBACK is tried FIRST) ──────────────────────────
# FALLBACK_CREDS_BLOB = os.getenv(
#     "FALLBACK_CREDS_BLOB",
#     "https://kabutatestfunction.blob.core.windows.net/credentials/kabuta-backend-backup-2134236604a3.json",
# )
# PRIMARY_CREDS_BLOB = os.getenv(
#     "PRIMARY_CREDS_BLOB",
#     "https://kabutatestfunction.blob.core.windows.net/credentials/kabuta-f2a1b-8facf2e97ddd.json",
# )

# FALLBACK_PROJECT_ID = os.getenv("FALLBACK_PROJECT_ID", "kabuta-backend-backup")
# PRIMARY_PROJECT_ID  = os.getenv("PRIMARY_PROJECT_ID",  "kabuta-f2a1b")

# # GEMINI 2 CONFIG
# VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")
# # MODEL_NAME      = os.getenv("VERTEX_MODEL_NAME", "gemini-2.5-flash")

# # GEMINI 3 MODEL
# MODEL_NAME = os.getenv("VERTEX_MODEL_NAME", "gemini-3.1-pro-preview")


# # ── Comax API ─────────────────────────────────────────────────────────────────
# COMAX_SOAP_URL = os.getenv(
#     "COMAX_SOAP_URL",
#     "http://ws.comax.co.il/WS_WRK/Work_Comax_WS/OCRDocument_Service.asmx",
# )
# COMAX_LOGIN_ID       = os.getenv("COMAX_LOGIN_ID", "")
# COMAX_LOGIN_PASSWORD = os.getenv("COMAX_LOGIN_PASSWORD", "")

# # ── Azure Storage ─────────────────────────────────────────────────────────────
# AZURE_STORAGE_CONNECTION_STRING = (
#     os.getenv("AzureWebJobsStorage")
# )
# BLOB_CONTAINER_NAME    = "document-uploads"
# RESULTS_CONTAINER_NAME = "processing-results"
# PROCESSING_QUEUE_NAME  = "processing-queue"

# # ── Processor endpoint (kept for reference) ───────────────────────────────────
# PROCESSOR_ENDPOINT = os.getenv(
#     "PROCESSOR_ENDPOINT",
#     "https://comax-genenral-backend.azurewebsites.net/api/process_document_queue",
# )

# # ──────────────────────────────────────────────────────────────────────────────
# # Schema and Prompt
# # ──────────────────────────────────────────────────────────────────────────────
# STRICT_SCHEMA = {
#     "supplier_account": "",
#     "supplier_name": "",
#     "tax_id": "",
#     "supplier_document": "",
#     "document_date": "",
#     "due_date": "",
#     "notes": "",
#     "warehouse": "",
#     "warehouse_name": "",
#     "purchase_order": "",
#     "total": "",
#     "discount_percent": "",
#     "discount": "",
#     "total_before_vat": "",
#     "vat_percent": "",
#     "vat_amount": "",
#     "total_including_vat": "",
#     "line_items": [
#         {
#             "line_number": "",
#             "item_name": "",
#             "barcode": "",
#             "quantity": "",
#             "unit_price": "",
#             "discount_percent": "",
#             "amount": "",
#             "bonus_item": "",
#             "line_note": "",
#             "return_reason": "",
#             "line_reference": "",
#             "batch_series": "",
#             "expiry_date": "",
#             "production_date": "",
#             "manufacturer_code": "",
#             "serial_number": "",
#         }
#     ],
# }

# EXTRACTION_PROMPT = """
# You are an information extraction engine for invoices/receipts.
# Return ONLY valid JSON in this EXACT structure and keys.
# Do not add extra keys, comments or text.
# If a field does not exist, leave it as an empty string ("") or empty list ([]).

# IMPORTANT: For all numeric fields (amounts, prices, percentages), extract the raw numeric value WITHOUT any formatting:
# - Remove commas, currency symbols, and thousands separators
# - Use decimal point (.) for decimals
# - Examples: "4,992.52" → "4992.52", "$1,234.56" → "1234.56", "15%" → "15"
# - "warehouse" field MUST return a numeric warehouse code (integer). NEVER return warehouse names or Hebrew text. If no warehouse code exists, return an empty string.
# - Always output dates in ISO 8601 full format: YYYY-MM-DD (four-digit year, two-digit month/day)
#   Example: 2022-06-06
# - supplier_document is the invoice id or delivery note id. Extract as a string.
# - BARCODE VS SERIAL_NUMBER:
#    - If the code in the product line is purely numeric and fits a standard format, map it to "barcode".
#    - CRITICAL: If the code contains LETTERS (e.g., "SH3RHP...") the entire column should be mapped it to "serial_number" instead of "barcode".
# - If barcode doesn't exist, use item code as a fallback if available.
# - line_number should be sequential, if you see skips, it's probably item codes or SKUs, not line numbers.
# - tax_id - ONLY REFER TO THE SENDER of the invoice, not the reciever of the invoice.
#   It's usually next to עוסק מורשה ,ח.פ, ח"פ, ע"מ, but NEVER return the receiver's tax_id.

# Schema:
# {schema}
# """.format(
#     schema=json.dumps(STRICT_SCHEMA, ensure_ascii=False, indent=2)
# )


# # ──────────────────────────────────────────────────────────────────────────────
# # Utility Helpers
# # ──────────────────────────────────────────────────────────────────────────────

# _vertex_model_cache: Optional[GenerativeModel] = None

# def _get_vertex_model() -> GenerativeModel:
#     global _vertex_model_cache
#     if _vertex_model_cache is not None:
#         logging.info("[Vertex] ✓ Using cached model (skipping init)")
#         return _vertex_model_cache

#     t0 = time.time()
#     logging.info("[Vertex] Cache miss — downloading SA keys and initialising...")
#     _init_vertex()
#     logging.info(f"[Vertex] _init_vertex() took {time.time()-t0:.2f}s")

#     _vertex_model_cache = GenerativeModel(MODEL_NAME)
#     logging.info(f"[Vertex] Model ready. Total init time: {time.time()-t0:.2f}s")
#     return _vertex_model_cache


# def _detect_mime_from_name(file_name: str) -> str:
#     m = mimetypes.guess_type(file_name or "")[0]
#     if m:
#         return m
#     name = (file_name or "").lower()
#     if name.endswith(".pdf"):
#         return "application/pdf"
#     if name.endswith((".jpg", ".jpeg")):
#         return "image/jpeg"
#     if name.endswith(".png"):
#         return "image/png"
#     return "application/octet-stream"


# def _normalize_date(val: str) -> Optional[str]:
#     """Return ISO 8601 'YYYY-MM-DDThh:mm:ss' or None if invalid/empty."""
#     if not val:
#         return None
#     val = val.strip()
#     m = re.match(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", val)
#     if m:
#         d, mth, y = m.groups()
#         y = y if len(y) == 4 else f"20{y.zfill(2)}"
#         try:
#             dt = datetime(int(y), int(mth), int(d))
#             return dt.strftime("%Y-%m-%dT00:00:00")
#         except ValueError:
#             return None
#     if re.match(r"\d{4}-\d{2}-\d{2}$", val):
#         return val + "T00:00:00"
#     return None


# # ──────────────────────────────────────────────────────────────────────────────
# # Comax XML Helpers
# # ──────────────────────────────────────────────────────────────────────────────

# COMAX_NS_SOAP = "http://schemas.xmlsoap.org/soap/envelope/"
# COMAX_NS_BODY = "http://tempuri.org/"


# def _to_number_or_none(v: Any) -> Optional[str]:
#     """Return numeric string if valid, otherwise None."""
#     try:
#         if v in (None, "", " ", "null", "None"):
#             return None
#         v = str(v).replace(",", "").strip()
#         if not v:
#             return None
#         float(v)
#         return v
#     except Exception:
#         return None


# def _normalize_bool_byte(val: Any) -> str:
#     if str(val).lower() in {"true", "1", "yes"}:
#         return "1"
#     return "0"


# def _add_text(child_of, tag, value):
#     el = ET.SubElement(child_of, tag)
#     el.text = "" if value is None else str(value)
#     return el


# def _add_text_if_present(child_of, tag, value):
#     if value is not None:
#         el = ET.SubElement(child_of, tag)
#         el.text = str(value)
#         return el
#     return None


# def _build_comax_xml(call_id: str, data: dict) -> str:
#     """Build SOAP XML envelope for UpdateDocumentKABUTA."""
#     ns_soap = COMAX_NS_SOAP
#     ns_body = COMAX_NS_BODY

#     ET.register_namespace("", COMAX_NS_BODY)
#     env  = ET.Element(ET.QName(ns_soap, "Envelope"))
#     body = ET.SubElement(env, ET.QName(ns_soap, "Body"))
#     root = ET.SubElement(body, ET.QName(ns_body, "UpdateDocumentKABUTA"))

#     request_el = ET.SubElement(root, "request")
#     _add_text(request_el, "RequestId", call_id)

#     # ── String fields (include when non-empty) ────────────────────────────────
#     string_fields = {
#         "supplier_document": "SupplierDocument",
#         "supplier_name":     "SupplierName",
#         "notes":             "Notes",
#         "warehouse_name":    "WarehouseName",
#         "purchase_order":    "PurchaseOrder",
#     }
#     for src_key, tag in string_fields.items():
#         val = data.get(src_key, "")
#         if val not in (None, "", []):
#             _add_text(request_el, tag, val)

#     # ── Numeric fields (omit if null/empty) ───────────────────────────────────
#     numeric_fields = {
#         "supplier_account":   "SupplierAccount",
#         "tax_id":             "TaxId",
#         "warehouse":          "Warehouse",
#         "total":              "Total",
#         "discount_percent":   "DiscountPercent",
#         "discount":           "Discount",
#         "total_before_vat":   "TotalBeforeVAT",
#         "vat_percent":        "VATPercent",
#         "vat_amount":         "VATAmount",
#         "total_including_vat": "TotalIncludingVAT",
#     }
#     for src_key, tag in numeric_fields.items():
#         val = data.get(src_key, "")
#         if tag == "Warehouse":
#             val = str(val).strip()
#             if val and val.isdigit():
#                 _add_text_if_present(request_el, tag, val)
#         else:
#             numeric_val = _to_number_or_none(val)
#             if numeric_val is not None:
#                 _add_text_if_present(request_el, tag, numeric_val)

#     # ── Date fields ───────────────────────────────────────────────────────────
#     date_fields = {
#         "document_date": "DocumentDate",
#         "due_date":      "DueDate",
#     }
#     for src_key, tag in date_fields.items():
#         normalized_date = _normalize_date(data.get(src_key, ""))
#         if normalized_date:
#             _add_text(request_el, tag, normalized_date)

#     # ── Line items ────────────────────────────────────────────────────────────
#     lines_el   = ET.SubElement(request_el, "Lines")
#     line_items = data.get("line_items", []) or []
#     for li in line_items:
#         line_el = ET.SubElement(lines_el, "KabutaDocumentLine")
#         _add_text(line_el, "LineNumber", li.get("line_number", ""))

#         item_name = li.get("item_name", "")
#         if item_name:
#             _add_text(line_el, "ItemName", item_name)

#         line_numeric_fields = {
#             "barcode":        "Barcode",
#             "quantity":       "Quantity",
#             "unit_price":     "UnitPrice",
#             "discount_percent": "DiscountPercent",
#             "amount":         "Amount",
#         }
#         for src_key, tag in line_numeric_fields.items():
#             numeric_val = _to_number_or_none(li.get(src_key, ""))
#             if numeric_val is not None:
#                 _add_text_if_present(line_el, tag, numeric_val)

#         bonus = li.get("bonus_item", "")
#         if bonus:
#             _add_text(line_el, "BonusItem", _normalize_bool_byte(bonus))

#         optional_fields = {
#             "line_note":         "LineNote",
#             "return_reason":     "ReturnReason",
#             "line_reference":    "LineReference",
#             "batch_series":      "BatchSeries",
#             "expiry_date":       "ExpiryDate",
#             "production_date":   "ProductionDate",
#             "manufacturer_code": "ManufacturerCode",
#             "serial_number":     "SerialNumber",
#         }
#         for src_key, tag in optional_fields.items():
#             val = li.get(src_key)
#             if val not in (None, "", []):
#                 _add_text(line_el, tag, val)

#     # ── Login credentials ─────────────────────────────────────────────────────
#     _add_text(root, "LoginID",       COMAX_LOGIN_ID)
#     _add_text(root, "LoginPassword", COMAX_LOGIN_PASSWORD)

#     xml_bytes = ET.tostring(env, encoding="utf-8", method="xml")
#     return xml_bytes.decode("utf-8")


# def _parse_comax_response(xml_text: str) -> dict:
#     try:
#         root = ET.fromstring(xml_text)
#     except Exception as e:
#         return {"success": False, "error": f"Invalid XML response: {e}", "raw": xml_text[:2000]}

#     def _find_first_by_localname(root_el, local):
#         for el in root_el.iter():
#             ln = el.tag.split("}")[-1] if "}" in el.tag else el.tag
#             if ln == local:
#                 return el
#         return None

#     is_success_el = _find_first_by_localname(root, "IsSuccess")
#     error_desc_el = _find_first_by_localname(root, "ErrorDescription")

#     is_success = (
#         is_success_el is not None
#         and is_success_el.text
#         and is_success_el.text.strip().lower() == "true"
#     )
#     error_desc = (
#         error_desc_el.text.strip()
#         if (error_desc_el is not None and error_desc_el.text)
#         else ("Update completed successfully" if is_success else "")
#     )
#     return {
#         "success": is_success,
#         "error":   "" if is_success else error_desc,
#         "raw":     xml_text[:2000],
#     }


# # ──────────────────────────────────────────────────────────────────────────────
# # Vertex AI Initialisation  (FALLBACK first, PRIMARY second)
# # ──────────────────────────────────────────────────────────────────────────────

# def _download_sa_key(blob_url: str, project_id: str) -> str:
#     """Download SA JSON from Azure Blob to /tmp and return the path."""
#     resp = requests.get(blob_url, timeout=60)
#     resp.raise_for_status()
#     key_path = f"/tmp/{project_id}-key.json"
#     with open(key_path, "wb") as f:
#         f.write(resp.content)
#     return key_path


# def _init_vertex() -> str:
#     """
#     Try to initialise Vertex AI.
#     Order: FALLBACK project (kabuta-backend-backup) → PRIMARY project (kabuta-f2a1b).
#     Returns the project_id that succeeded.
#     Raises RuntimeError if both fail.
#     """
#     candidates = [
#         (FALLBACK_CREDS_BLOB, FALLBACK_PROJECT_ID, "fallback"),
#         (PRIMARY_CREDS_BLOB,  PRIMARY_PROJECT_ID,  "primary"),
#     ]
#     last_err = None
#     for blob_url, pid, label in candidates:
#         try:
#             key_path = _download_sa_key(blob_url, pid)
#             os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
#             vertexai.init(project=pid, location=VERTEX_LOCATION)
#             # Lightweight probe – instantiate the model object only
#             _ = GenerativeModel(MODEL_NAME)
#             logging.info(f"[Vertex] Initialised with {label} project '{pid}'")
#             return pid
#         except Exception as e:
#             logging.warning(f"[Vertex] Init failed for {label} project '{pid}': {e}")
#             last_err = e

#     raise RuntimeError(
#         f"Failed to initialise Vertex AI with both fallback and primary credentials. "
#         f"Last error: {last_err}"
#     )


# # ──────────────────────────────────────────────────────────────────────────────
# # Vertex AI Analysis
# # ──────────────────────────────────────────────────────────────────────────────

# def analyze_with_vertex_ai_strict(file_content: bytes, file_name: str) -> dict:
#     # ⚠️ BUG FIX: removed the extra _init_vertex() call that was here before
#     t_fn_start = time.time()
#     logging.info(f"[Vertex] analyze_with_vertex_ai_strict() started")

#     t0 = time.time()
#     model = _get_vertex_model()
#     logging.info(f"[Vertex] _get_vertex_model() took {time.time()-t0:.2f}s")

#     mime_type = _detect_mime_from_name(file_name)
#     logging.info(f"[Vertex] mime_type={mime_type}, file_size={len(file_content)/1024:.1f}KB")

#     parts = [
#         EXTRACTION_PROMPT,
#         Part.from_data(mime_type=mime_type, data=file_content),
#     ]
#     cfg = GenerationConfig(
#         response_mime_type="application/json",
#         temperature=0.0,
#         max_output_tokens=65535,
#     )

#     max_retries = 3
#     last_exc    = None
#     for attempt in range(max_retries):
#         try:
#             logging.info(f"[Vertex] Calling generate_content() (attempt {attempt+1}/{max_retries})...")
#             t0   = time.time()
#             resp = model.generate_content(parts, generation_config=cfg, stream=False)
#             dur  = time.time() - t0
#             logging.info(f"[Vertex] generate_content() returned in {dur:.2f}s")

#             if not getattr(resp, "candidates", None) or not resp.candidates[0].content.parts:
#                 logging.error(f"[Vertex] Empty candidates: {resp}")
#                 raise RuntimeError("No candidates returned by Vertex.")

#             text = resp.candidates[0].content.parts[0].text.strip()
#             logging.info(f"[Vertex] Response length={len(text)} chars")

#             t0 = time.time()
#             try:
#                 result = json.loads(text)
#                 logging.info(f"[Vertex] JSON parse took {time.time()-t0:.3f}s")
#                 logging.info(f"[Vertex] Total analyze() time: {time.time()-t_fn_start:.2f}s")
#                 return result
#             except json.JSONDecodeError as e:
#                 logging.error("--- VERTEX JSON DECODE ERROR ---")
#                 logging.error(f"Error: {e}  |  Line {e.lineno}, Col {e.colno}")
#                 logging.error(f"TAIL: ...{text[-500:]}" if len(text) > 500 else f"FULL: {text}")
#                 logging.error("--- END ERROR LOG ---")
#                 raise RuntimeError(f"AI returned malformed JSON at {e.lineno}:{e.colno}")

#         except Exception as exc:
#             last_exc = exc
#             if "429" in str(exc) and attempt < max_retries - 1:
#                 wait = 10 * (2 ** attempt)
#                 logging.warning(
#                     f"[Vertex] 429 rate-limited (attempt {attempt+1}/{max_retries}), "
#                     f"retrying in {wait}s…"
#                 )
#                 time.sleep(wait)
#             else:
#                 raise

#     raise last_exc


# # ──────────────────────────────────────────────────────────────────────────────
# # Blob Storage Helpers
# # ──────────────────────────────────────────────────────────────────────────────

# def _get_blob_service() -> BlobServiceClient:
#     return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)


# def _ensure_container(blob_service: BlobServiceClient, container_name: str) -> None:
#     try:
#         cc = blob_service.get_container_client(container_name)
#         if not cc.exists():
#             cc.create_container()
#     except Exception as e:
#         logging.warning(f"[Blob] Could not ensure container '{container_name}': {e}")


# def save_original_document(request_id: str, file_name: str, file_bytes: bytes) -> Optional[str]:
#     """
#     Save the raw uploaded file to blob storage.
#     Always attempts to save regardless of downstream errors.
#     Returns the blob path on success, None on failure.
#     """
#     try:
#         svc       = _get_blob_service()
#         _ensure_container(svc, BLOB_CONTAINER_NAME)
#         blob_path = f"original/{request_id}/{file_name}"
#         svc.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_path).upload_blob(
#             file_bytes, overwrite=True
#         )
#         logging.info(f"[Blob] Saved original document → {BLOB_CONTAINER_NAME}/{blob_path}")
#         return blob_path
#     except Exception as e:
#         logging.error(f"[Blob] Failed to save original document for {request_id}: {e}", exc_info=True)
#         return None


# def save_results_to_blob(request_id: str, results: Dict[str, Any]) -> None:
#     """Save processing results JSON to blob storage."""
#     try:
#         svc = _get_blob_service()
#         _ensure_container(svc, RESULTS_CONTAINER_NAME)
#         blob_name = f"{request_id}.json"
#         svc.get_blob_client(container=RESULTS_CONTAINER_NAME, blob=blob_name).upload_blob(
#             json.dumps(results, ensure_ascii=False), overwrite=True
#         )
#         logging.info(f"[Blob] Saved results → {RESULTS_CONTAINER_NAME}/{blob_name}")
#     except Exception as e:
#         logging.error(f"[Blob] Failed to save results for {request_id}: {e}", exc_info=True)


# def save_comax_xml(request_id: str, soap_xml: str) -> Optional[str]:
#     """Save outgoing Comax SOAP XML to blob storage."""
#     try:
#         svc = _get_blob_service()
#         _ensure_container(svc, RESULTS_CONTAINER_NAME)
#         blob_path = f"{request_id}.xml"
#         svc.get_blob_client(container=RESULTS_CONTAINER_NAME, blob=blob_path).upload_blob(
#             soap_xml, overwrite=True
#         )
#         logging.info(f"[Blob] Saved Comax XML → {RESULTS_CONTAINER_NAME}/{blob_path}")
#         return blob_path
#     except Exception as e:
#         logging.error(f"[Blob] Failed to save Comax XML for {request_id}: {e}", exc_info=True)
#         return None


# # ──────────────────────────────────────────────────────────────────────────────
# # Comax SOAP Send
# # ──────────────────────────────────────────────────────────────────────────────

# def send_to_comax(call_id: str, data: dict) -> dict:
#     soap_xml = _build_comax_xml(call_id, data)

#     # Always persist the XML before sending
#     save_comax_xml(call_id, soap_xml)

#     url = f"{COMAX_SOAP_URL}?op=UpdateDocumentKABUTA"
#     headers = {
#         "Content-Type": "text/xml; charset=utf-8",
#         "SOAPAction":   "http://tempuri.org/UpdateDocumentKABUTA",
#     }

#     logging.info("---------- COMAX OUTGOING XML ----------")
#     logging.info(soap_xml)
#     logging.info("---------- END XML ----------")

#     try:
#         resp = requests.post(url, data=soap_xml.encode("utf-8"), headers=headers, timeout=60)
#         resp.raise_for_status()
#     except requests.HTTPError as e:
#         return {
#             "success": False,
#             "error":   f"HTTP {resp.status_code}: {e}",
#             "raw":     resp.text[:2000] if "resp" in dir() else "",
#         }
#     except Exception as e:
#         return {"success": False, "error": f"Request failed: {e}"}

#     return _parse_comax_response(resp.text)


# # ──────────────────────────────────────────────────────────────────────────────
# # Queue Helper
# # ──────────────────────────────────────────────────────────────────────────────

# def enqueue_processing(payload: dict) -> None:
#     queue = QueueClient.from_connection_string(
#         AZURE_STORAGE_CONNECTION_STRING, PROCESSING_QUEUE_NAME
#     )
#     queue.message_encode_policy = BinaryBase64EncodePolicy()
#     msg_bytes = json.dumps(payload).encode("utf-8")
#     queue.send_message(queue.message_encode_policy.encode(content=msg_bytes))
#     logging.info(f"[Queue] Enqueued message for request_id={payload.get('request_id')}")


# # ──────────────────────────────────────────────────────────────────────────────
# # HTTP Trigger: analyze_document
# # ──────────────────────────────────────────────────────────────────────────────

# @app.route(route="analyze_document", methods=["POST"])
# def analyze_document(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info("[analyze_document] Received document analysis request")

#     # ── 1. Read inputs ─────────────────────────────────────────────────────────
#     base64_data = req.params.get("base64_file")
#     file_name   = req.params.get("file_name")

#     if not base64_data:
#         try:
#             body = req.get_json()
#         except ValueError:
#             body = None
#         if body:
#             base64_data = body.get("base64_file")
#             file_name   = body.get("file_name", file_name)

#     if not base64_data:
#         return func.HttpResponse(
#             json.dumps({"error": "Missing required parameter 'base64_file'."}),
#             status_code=400,
#             mimetype="application/json",
#         )

#     file_name = file_name or "document"

#     # ── 2. Generate request ID ─────────────────────────────────────────────────
#     request_id = uuid.uuid4().hex
#     logging.info(f"[analyze_document] request_id={request_id}, file={file_name}")

#     # ── 3. Decode Base64 ───────────────────────────────────────────────────────
#     try:
#         if base64_data.startswith("data:"):
#             base64_data = base64_data.split(",", 1)[1]
#         file_content = base64.b64decode(base64_data)
#     except Exception as e:
#         logging.error(f"[analyze_document] Invalid base64 data: {e}")
#         return func.HttpResponse(
#             json.dumps({"error": "Invalid file data supplied."}),
#             status_code=400,
#             mimetype="application/json",
#         )

#     # ── 4. ALWAYS save original to blob (before any further processing) ────────
#     blob_path = save_original_document(request_id, file_name, file_content)
#     if blob_path is None:
#         # Log the failure but do NOT abort – we still want to return 202 and enqueue.
#         # The queue processor will also attempt to re-read from blob; if the upload
#         # failed it will log an appropriate error at that stage.
#         logging.error(
#             f"[analyze_document] Original file blob upload failed for {request_id}. "
#             "Continuing to enqueue anyway."
#         )
#     else:
#         logging.info(f"[analyze_document] Original file confirmed at: {blob_path}")

#     # ── 5. Enqueue processing job ──────────────────────────────────────────────
#     enqueue_processing({"request_id": request_id, "file_name": file_name})

#     # ── 6. Return immediately ──────────────────────────────────────────────────
#     return func.HttpResponse(
#         json.dumps(
#             {
#                 "request_id": request_id,
#                 "status":     "queued",
#                 "blob_saved": blob_path is not None,
#                 "message":    "Document queued for processing",
#             }
#         ),
#         status_code=202,
#         mimetype="application/json",
#     )


# # ──────────────────────────────────────────────────────────────────────────────
# # Queue Trigger: process_document_queue
# # ──────────────────────────────────────────────────────────────────────────────

# @app.queue_trigger(
#     arg_name="msg",
#     queue_name="processing-queue",
#     connection="AzureWebJobsStorage",
# )
# def process_document_queue(msg: func.QueueMessage) -> None:
#     """
#     Queue-triggered processor.
#     Message body must contain:
#       - request_id : str
#       - file_name  : str
#     """
#     t_total = time.time()
#     logging.info("[process_document_queue] ═══ START ═══")

#     request_id = None
#     file_name  = "unknown"

#     try:
#         # ── 1. Parse queue payload ─────────────────────────────────────────────
#         body       = json.loads(msg.get_body().decode("utf-8"))
#         request_id = body.get("request_id")
#         file_name  = body.get("file_name", "document")

#         if not request_id or not file_name:
#             logging.error("[process_document_queue] Missing request_id or file_name in message")
#             return

#         logging.info(f"[process_document_queue] request_id={request_id}, file={file_name}")

#         # ── 2. Retrieve original file from blob ────────────────────────────────
#         svc       = _get_blob_service()
#         blob_path = f"original/{request_id}/{file_name}"
#         bc        = svc.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_path)

#         if not bc.exists():
#             logging.error(
#                 f"[process_document_queue] Original file not found in blob for {request_id} "
#                 f"at path {blob_path}"
#             )
#             # Save a failed result so callers can detect the issue
#             save_results_to_blob(
#                 request_id,
#                 {
#                     "request_id":    request_id,
#                     "file_name":     file_name,
#                     "status":        "failed",
#                     "error":         "Original file not found in blob storage",
#                     "processed_at":  datetime.utcnow().isoformat(),
#                 },
#             )
#             return

#         file_content  = bc.download_blob().readall()
#         file_size_mb  = len(file_content) / (1024 * 1024)
#         logging.info(f"[process_document_queue] Loaded file ({file_size_mb:.2f} MB)")

#         # ── 3. Vertex AI analysis ──────────────────────────────────────────────
#         t0   = time.time()
#         logging.info(f"[process_document_queue] Starting Vertex AI analysis for {request_id}")
#         data = analyze_with_vertex_ai_strict(file_content, file_name)
#         logging.info(f"[TIMER] Vertex AI total: {time.time()-t0:.2f}s")

#         try:
#             t0   = time.time()
#             data = normalize_extracted_data(data)
#             logging.info(f"[TIMER] Normalize data: {time.time()-t0:.3f}s")

#         except Exception as norm_err:
#             logging.error(f"[process_document_queue] Normalization failed: {norm_err}")

#         # ── 4. Send to Comax ───────────────────────────────────────────────────
#         t0           = time.time()
#         comax_result = send_to_comax(request_id, data)
#         logging.info(f"[TIMER] Comax SOAP call: {time.time()-t0:.2f}s | success={comax_result.get('success')}")

#         # ── 5. Save final results ──────────────────────────────────────────────
#         t0 = time.time()
#         save_results_to_blob(
#             request_id,
#             {
#                 "request_id":   request_id,
#                 "file_name":    file_name,
#                 "status":       "completed" if comax_result.get("success") else "failed",
#                 "data":         data,
#                 "comax_result": comax_result,
#                 "processed_at": datetime.utcnow().isoformat(),
#             },
#         )
#         logging.info(f"[TIMER] Save results blob: {time.time()-t0:.3f}s")
#         logging.info(f"[TIMER] ═══ TOTAL: {time.time()-t_total:.2f}s ═══")

#     except Exception as e:
#         logging.error(
#             f"[process_document_queue] Unhandled error for request_id={request_id}: {e}",
#             exc_info=True,
#         )
#         if request_id:
#             try:
#                 save_results_to_blob(
#                     request_id,
#                     {
#                         "request_id":   request_id,
#                         "file_name":    file_name,
#                         "status":       "failed",
#                         "error":        str(e),
#                         "processed_at": datetime.utcnow().isoformat(),
#                     },
#                 )
#             except Exception as save_err:
#                 logging.error(
#                     f"[process_document_queue] Failed to save error result: {save_err}"
#                 )

# SERIAL_ONLY_SUPPLIERS = {
#     "515057206",  # הראל ייבוא ושיווק - Apple/Nokia importer
# }

# def normalize_extracted_data(data: dict) -> dict:
#     line_items = data.get("line_items", [])
#     tax_id = str(data.get("tax_id", "")).strip()

#     # ── Decide ONCE for the whole document: is this column barcodes or serials?
#     # Rule 1: Known serial-only supplier → always move all to serial
#     # Rule 2: ANY alphanumeric value anywhere in the barcode column → whole column is serials
#     force_serial = (
#         tax_id in SERIAL_ONLY_SUPPLIERS
#         or any(
#             any(c.isalpha() for c in str(item.get("barcode", "")))
#             for item in line_items
#         )
#     )

#     if force_serial:
#         logging.info(
#             f"[Normalize] Barcode column classified as SERIAL for entire document "
#             f"(supplier={tax_id}, force_serial={force_serial})"
#         )
#         for item in line_items:
#             barcode = str(item.get("barcode", "")).strip()
#             serial  = str(item.get("serial_number", "")).strip()
#             if barcode and not serial:
#                 item["serial_number"] = barcode
#                 item["barcode"] = ""
#             elif barcode and serial:
#                 # Both filled — barcode column was misclassified, discard barcode
#                 logging.warning(
#                     f"[Normalize] Line {item.get('line_number')}: both barcode='{barcode}' "
#                     f"and serial='{serial}' present — discarding barcode"
#                 )
#                 item["barcode"] = ""

#     return data



import os
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
from typing import Optional, Dict, Any
import azure.functions as func
from azure.storage.blob import BlobServiceClient

# ── NEW: google-genai SDK (replaces vertexai SDK) ─────────────────────────────
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
MODEL_NAME      = os.getenv("VERTEX_MODEL_NAME", "gemini-3.1-pro-preview")

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

# ── Processor endpoint (kept for reference) ───────────────────────────────────
PROCESSOR_ENDPOINT = os.getenv(
    "PROCESSOR_ENDPOINT",
    "https://comax-genenral-backend.azurewebsites.net/api/process_document_queue",
)

# ──────────────────────────────────────────────────────────────────────────────
# Schema and Prompt
# ──────────────────────────────────────────────────────────────────────────────
STRICT_SCHEMA = {
    "supplier_account": "",
    "supplier_name": "",
    "tax_id": "",
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
- Always output dates in ISO 8601 full format: YYYY-MM-DD (four-digit year, two-digit month/day)
  Example: 2022-06-06
- supplier_document is the invoice id or delivery note id. Extract as a string.
- BARCODE VS SERIAL_NUMBER:
   - If the code in the product line is purely numeric and fits a standard format, map it to "barcode".
   - CRITICAL: If the code contains LETTERS (e.g., "SH3RHP...") the entire column should be mapped it to "serial_number" instead of "barcode".
- If barcode doesn't exist, use item code as a fallback if available.
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

Schema:
{schema}
""".format(
    schema=json.dumps(STRICT_SCHEMA, ensure_ascii=False, indent=2)
)


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

def analyze_with_vertex_ai_strict(file_content: bytes, file_name: str) -> dict:
    t_fn_start = time.time()
    logging.info("[Vertex] analyze_with_vertex_ai_strict() started")

    t0     = time.time()
    client = _get_genai_client()
    logging.info(f"[Vertex] _get_genai_client() took {time.time()-t0:.2f}s")

    mime_type = _detect_mime_from_name(file_name)
    logging.info(f"[Vertex] mime_type={mime_type}, file_size={len(file_content)/1024:.1f}KB")

    # Build the content parts: text prompt + inline file bytes
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part(text=EXTRACTION_PROMPT),
                types.Part(
                    inline_data=types.Blob(
                        mime_type=mime_type,
                        data=file_content,
                    )
                ),
            ],
        )
    ]

    # Generation config — thinking_level is now supported
    cfg = types.GenerateContentConfig(
        response_mime_type="application/json",
        temperature=0.0,
        max_output_tokens=65535,
        thinking_config=types.ThinkingConfig(
            thinking_level="LOW",   # LOW / MEDIUM / HIGH
        ),
    )

    max_retries = 3
    last_exc    = None

    for attempt in range(max_retries):
        try:
            logging.info(
                f"[Vertex] Calling generate_content() "
                f"(attempt {attempt+1}/{max_retries}, model={MODEL_NAME})..."
            )
            t0   = time.time()
            resp = client.models.generate_content(
                model=MODEL_NAME,
                contents=contents,
                config=cfg,
            )
            dur = time.time() - t0
            logging.info(f"[Vertex] generate_content() returned in {dur:.2f}s")

            # Extract text from response
            if not resp.candidates:
                logging.error(f"[Vertex] Empty candidates: {resp}")
                raise RuntimeError("No candidates returned by Vertex.")

            # With thinking enabled the first part may be the thought; find text part
            text = None
            for part in resp.candidates[0].content.parts:
                if hasattr(part, "text") and part.text:
                    text = part.text.strip()
                    break

            if not text:
                logging.error(f"[Vertex] No text part found in response: {resp}")
                raise RuntimeError("No text content in Vertex response.")

            logging.info(f"[Vertex] Response length={len(text)} chars")

            t0 = time.time()
            try:
                result = json.loads(text)
                logging.info(f"[Vertex] JSON parse took {time.time()-t0:.3f}s")
                logging.info(f"[Vertex] Total analyze() time: {time.time()-t_fn_start:.2f}s")
                return result
            except json.JSONDecodeError as e:
                logging.error("--- VERTEX JSON DECODE ERROR ---")
                logging.error(f"Error: {e}  |  Line {e.lineno}, Col {e.colno}")
                logging.error(f"TAIL: ...{text[-500:]}" if len(text) > 500 else f"FULL: {text}")
                logging.error("--- END ERROR LOG ---")
                raise RuntimeError(f"AI returned malformed JSON at {e.lineno}:{e.colno}")

        except Exception as exc:
            last_exc = exc
            if "429" in str(exc) and attempt < max_retries - 1:
                wait = 10 * (2 ** attempt)
                logging.warning(
                    f"[Vertex] 429 rate-limited (attempt {attempt+1}/{max_retries}), "
                    f"retrying in {wait}s…"
                )
                time.sleep(wait)
            else:
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
    except Exception as e:
        logging.warning(f"[Blob] Could not ensure container '{container_name}': {e}")


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

    # ── 2. Generate request ID ─────────────────────────────────────────────────
    request_id = uuid.uuid4().hex
    logging.info(f"[analyze_document] request_id={request_id}, file={file_name}")

    # ── 3. Decode Base64 ───────────────────────────────────────────────────────
    try:
        if base64_data.startswith("data:"):
            base64_data = base64_data.split(",", 1)[1]
        file_content = base64.b64decode(base64_data)
    except Exception as e:
        logging.error(f"[analyze_document] Invalid base64 data: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Invalid file data supplied."}),
            status_code=400,
            mimetype="application/json",
        )

    # ── 4. ALWAYS save original to blob (before any further processing) ────────
    blob_path = save_original_document(request_id, file_name, file_content)
    if blob_path is None:
        # Log the failure but do NOT abort – we still want to return 202 and enqueue.
        # The queue processor will also attempt to re-read from blob; if the upload
        # failed it will log an appropriate error at that stage.
        logging.error(
            f"[analyze_document] Original file blob upload failed for {request_id}. "
            "Continuing to enqueue anyway."
        )
    else:
        logging.info(f"[analyze_document] Original file confirmed at: {blob_path}")

    # ── 5. Enqueue processing job ──────────────────────────────────────────────
    enqueue_processing({"request_id": request_id, "file_name": file_name})

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
      - request_id : str
      - file_name  : str
    """
    t_total = time.time()
    logging.info("[process_document_queue] ═══ START ═══")

    request_id = None
    file_name  = "unknown"

    try:
        # ── 1. Parse queue payload ─────────────────────────────────────────────
        body       = json.loads(msg.get_body().decode("utf-8"))
        request_id = body.get("request_id")
        file_name  = body.get("file_name", "document")

        if not request_id or not file_name:
            logging.error("[process_document_queue] Missing request_id or file_name in message")
            return

        logging.info(f"[process_document_queue] request_id={request_id}, file={file_name}")

        # ── 2. Retrieve original file from blob ────────────────────────────────
        svc       = _get_blob_service()
        blob_path = f"original/{request_id}/{file_name}"
        bc        = svc.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_path)

        if not bc.exists():
            logging.error(
                f"[process_document_queue] Original file not found in blob for {request_id} "
                f"at path {blob_path}"
            )
            save_results_to_blob(
                request_id,
                {
                    "request_id":   request_id,
                    "file_name":    file_name,
                    "status":       "failed",
                    "error":        "Original file not found in blob storage",
                    "processed_at": datetime.utcnow().isoformat(),
                },
            )
            return

        file_content = bc.download_blob().readall()
        file_size_mb = len(file_content) / (1024 * 1024)
        logging.info(f"[process_document_queue] Loaded file ({file_size_mb:.2f} MB)")

        # ── 3. Vertex AI analysis ──────────────────────────────────────────────
        t0 = time.time()
        logging.info(f"[process_document_queue] Starting Vertex AI analysis for {request_id}")
        data = analyze_with_vertex_ai_strict(file_content, file_name)
        logging.info(f"[TIMER] Vertex AI total: {time.time()-t0:.2f}s")

        try:
            t0   = time.time()
            data = normalize_extracted_data(data)
            logging.info(f"[TIMER] Normalize data: {time.time()-t0:.3f}s")
        except Exception as norm_err:
            logging.error(f"[process_document_queue] Normalization failed: {norm_err}")

        # ── 4. Send to Comax ───────────────────────────────────────────────────
        t0           = time.time()
        comax_result = send_to_comax(request_id, data)
        logging.info(
            f"[TIMER] Comax SOAP call: {time.time()-t0:.2f}s | "
            f"success={comax_result.get('success')}"
        )

        # ── 5. Save final results ──────────────────────────────────────────────
        t0 = time.time()
        save_results_to_blob(
            request_id,
            {
                "request_id":   request_id,
                "file_name":    file_name,
                "status":       "completed" if comax_result.get("success") else "failed",
                "data":         data,
                "comax_result": comax_result,
                "processed_at": datetime.utcnow().isoformat(),
            },
        )
        logging.info(f"[TIMER] Save results blob: {time.time()-t0:.3f}s")
        logging.info(f"[TIMER] ═══ TOTAL: {time.time()-t_total:.2f}s ═══")

    except Exception as e:
        logging.error(
            f"[process_document_queue] Unhandled error for request_id={request_id}: {e}",
            exc_info=True,
        )
        if request_id:
            try:
                save_results_to_blob(
                    request_id,
                    {
                        "request_id":   request_id,
                        "file_name":    file_name,
                        "status":       "failed",
                        "error":        str(e),
                        "processed_at": datetime.utcnow().isoformat(),
                    },
                )
            except Exception as save_err:
                logging.error(
                    f"[process_document_queue] Failed to save error result: {save_err}"
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