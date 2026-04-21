import base64
import vertexai
from vertexai.generative_models import GenerativeModel, Part, GenerationConfig
import azure.functions as func
import logging
import json
import requests
import os
import mimetypes
from datetime import datetime
import re

# Azure Function
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Configuration
FALLBACK_CREDS_BLOB = (
    "https://kabutatestfunction.blob.core.windows.net/credentials/"
    "kabuta-backend-backup-2134236604a3.json"
)
PRIMARY_CREDS_BLOB = (
    "https://kabutatestfunction.blob.core.windows.net/credentials/"
    "kabuta-f2a1b-8facf2e97ddd.json"
)

@app.route(route="analyze_document", methods=["POST"])
def analyze_document(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to analyze base64 encoded documents and extract specified fields using Vertex AI.
    """
    logging.info("Processing document analysis request...")
    
    try:
        # Parse JSON request
        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse("Invalid JSON payload.", status_code=400)
        
        # Extract base64 data
        base64_data = req_body.get("base64_file")
        file_name = req_body.get("file_name", "document")
        
        if not base64_data:
            return func.HttpResponse("No base64_file provided in JSON.", status_code=400)
        
        logging.info(f"Processing file: {file_name}")
        
        # Decode base64 data
        try:
            # Handle data URL format (data:mime/type;base64,...)
            if base64_data.startswith('data:'):
                base64_data = base64_data.split(',', 1)[1]
            
            file_content = base64.b64decode(base64_data)
        except Exception as e:
            logging.error(f"Error decoding base64 data: {e}")
            return func.HttpResponse("Invalid base64 data.", status_code=400)
        
        # Analyze with Vertex AI
        logging.info("Analyzing document with Vertex AI...")
        analysis_results = analyze_with_vertex_ai(file_content, file_name)
        
        # Return results
        return func.HttpResponse(
            json.dumps({
                "message": "Document analyzed successfully.",
                "data": analysis_results,
                "file_name": file_name
            }, ensure_ascii=False, indent=2),
            status_code=200,
            mimetype="application/json; charset=utf-8"
        )
        
    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json; charset=utf-8"
        )


def analyze_with_vertex_ai(file_content: bytes, file_name: str) -> dict:
    """
    Analyze document using Google Vertex AI with fallback projects.
    Uses Hebrew prompts but returns mapped English field names.
    """
    projects = [
        ("Primary Project", PRIMARY_CREDS_BLOB, "kabuta-f2a1b"),
        ("Fallback Project", FALLBACK_CREDS_BLOB, "kabuta-backend-backup")
    ]
    
    # Template for extraction in Hebrew
    template = """
    אנא חלץ את השדות הבאים מהמסמך בפורמט JSON:
    {
        "מספר_מזהה_לפניה_בקבוטה": "",
        "חשבון_ספק": "",
        "שם_ספק": "",
        "חפ": "",
        "תעודת_ספק": "",
        "לתאריך": "",
        "תאריך_פרעון": "",
        "הערות": "",
        "מחסן": "",
        "שם_מחסן": "",
        "הזמנת_רכש": "",
        "סהכ": "",
        "אחוז_הנחה": "",
        "הנחה": "",
        "סהכ_לפני_מעמ": "",
        "אחוז_מעמ": "",
        "מעמ": "",
        "סהכ_כולל_מעמ": "",
        "שורות": [
            {
                "שורה": "",
                "פריט": "",
                "ברקוד": "",
                "כמות": "",
                "מחיר_יח": "",
                "אחוז_הנחה": "",
                "סכום": "",
                "פריט_בונוס": false,
                "הערה_לשורה": "",
                "סיבת_החזר": "",
                "אסמכתה_לשורה": "",
                "סדרה": "",
                "תאריך_תוקף_סדרה": "",
                "תאריך_ייצור_סדרה": "",
                "קוד_יצרן": "",
                "מספר_סריאלי": ""
            }
        ]
    }
    
    הוראות:
    1. חלץ את כל השדות הרלוונטיים מהמסמך
    2. עבור שדות תאריך, החזר בפורמט dd/mm/yyyy
    3. עבור מספרים, החזר את הערך המספרי בלבד
    4. אם שדה לא קיים במסמך, השאר אותו ריק
    5. עבור שורות, חלץ את כל השורות הקיימות במסמך
    6. החזר JSON תקני בלבד
    """
    
    for name, creds_blob, project_id in projects:
        try:
            logging.info(f"Attempting analysis with {name} ({project_id})")
            
            # Download and set credentials
            key_resp = requests.get(creds_blob)
            key_resp.raise_for_status()
            key_path = f"/tmp/{project_id}-key.json"
            with open(key_path, "wb") as key_file:
                key_file.write(key_resp.content)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
            
            # Initialize Vertex AI
            vertexai.init(project=project_id, location="us-central1")
            model = GenerativeModel("gemini-2.0-flash-001")
            
            # Prepare document and prompt
            mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
            document_part = Part.from_data(mime_type=mime_type, data=file_content)
            prompt_part = Part.from_text(template)
            
            # Generation configuration
            config = GenerationConfig(
                max_output_tokens=8192,
                temperature=0,
                top_p=0.95
            )
            
            logging.info("Sending document to Vertex AI for analysis.")
            response = model.generate_content(
                [document_part, prompt_part],
                generation_config=config,
                stream=False
            )
            
            if not hasattr(response, "candidates") or not response.candidates:
                logging.error(f"[{name}] No valid candidates returned")
                continue
            
            candidate_content = response.candidates[0].content.parts[0].text
            logging.info(f"Raw response from {name}: {candidate_content[:500]}...")
            
            # Clean up JSON content
            if candidate_content.startswith("```json"):
                candidate_content = candidate_content.split("```json", 1)[-1].rsplit("```", 1)[0].strip()
            elif candidate_content.startswith("```"):
                candidate_content = candidate_content.split("```", 1)[-1].rsplit("```", 1)[0].strip()
            
            # Parse JSON response
            try:
                parsed_result = json.loads(candidate_content)
                logging.info(f"Successfully parsed response from {name}")
                return parsed_result
            except json.JSONDecodeError as json_err:
                logging.error(f"JSON parsing failed for {name}: {json_err}")
                continue
                
        except Exception as e:
            logging.error(f"{name} project failed: {e}")
            continue
    
    raise RuntimeError("All Vertex AI projects failed. Unable to analyze document.")


def validate_extracted_data(data: dict) -> dict:
    """
    Validate and clean extracted data.
    """
    # Basic validation and cleaning
    validated_data = {}
    
    for key, value in data.items():
        if key == "שורות" and isinstance(value, list):
            # Validate line items
            validated_lines = []
            for line in value:
                if isinstance(line, dict):
                    validated_line = {}
                    for line_key, line_value in line.items():
                        validated_line[line_key] = str(line_value) if line_value is not None else ""
                    validated_lines.append(validated_line)
            validated_data[key] = validated_lines
        else:
            validated_data[key] = str(value) if value is not None else ""
    
    return validated_data


# Alternative endpoint for English field names
@app.route(route="analyze_document_en", methods=["POST"])
def analyze_document_en(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to analyze base64 encoded documents with English field names.
    """
    logging.info("Processing document analysis request (English)...")
    
    try:
        # Parse JSON request
        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse("Invalid JSON payload.", status_code=400)
        
        # Extract base64 data
        base64_data = req_body.get("base64_file")
        file_name = req_body.get("file_name", "document")
        
        if not base64_data:
            return func.HttpResponse("No base64_file provided in JSON.", status_code=400)
        
        # Decode base64 data
        try:
            if base64_data.startswith('data:'):
                base64_data = base64_data.split(',', 1)[1]
            file_content = base64.b64decode(base64_data)
        except Exception as e:
            logging.error(f"Error decoding base64 data: {e}")
            return func.HttpResponse("Invalid base64 data.", status_code=400)
        
        # Analyze with Vertex AI (English version)
        analysis_results = analyze_with_vertex_ai_english(file_content, file_name)
        
        return func.HttpResponse(
            json.dumps({
                "message": "Document analyzed successfully.",
                "data": analysis_results,
                "file_name": file_name
            }, ensure_ascii=False, indent=2),
            status_code=200,
            mimetype="application/json; charset=utf-8"
        )
        
    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json; charset=utf-8"
        )


def analyze_with_vertex_ai_english(file_content: bytes, file_name: str) -> dict:
    """
    Analyze document using Google Vertex AI with English field names.
    """
    projects = [
        ("Primary Project", PRIMARY_CREDS_BLOB, "kabuta-f2a1b"),
        ("Fallback Project", FALLBACK_CREDS_BLOB, "kabuta-backend-backup")
    ]
    
    template = """
    Please extract the following fields from the document in JSON format:
    {
        "reference_number": "",
        "supplier_account": "",
        "supplier_name": "",
        "tax_id": "",
        "supplier_certificate": "",
        "document_date": "",
        "due_date": "",
        "remarks": "",
        "warehouse_code": "",
        "warehouse_name": "",
        "purchase_order": "",
        "total": "",
        "discount_percentage": "",
        "discount_amount": "",
        "total_before_vat": "",
        "vat_percentage": "",
        "vat_amount": "",
        "total_including_vat": "",
        "line_items": [
            {
                "line_number": "",
                "item_code": "",
                "barcode": "",
                "quantity": "",
                "unit_price": "",
                "discount_percentage": "",
                "amount": "",
                "is_bonus_item": false,
                "line_remarks": "",
                "return_reason": "",
                "line_reference": "",
                "batch_number": "",
                "batch_expiry_date": "",
                "batch_production_date": "",
                "manufacturer_code": "",
                "serial_number": ""
            }
        ]
    }
    
    Instructions:
    1. Extract all relevant fields from the document
    2. For date fields, return in dd/mm/yyyy format
    3. For numeric fields, return the numeric value only
    4. If a field doesn't exist in the document, leave it empty
    5. For line items, extract all lines present in the document
    6. Return valid JSON only
    """
    
    for name, creds_blob, project_id in projects:
        try:
            logging.info(f"Attempting analysis with {name} ({project_id})")
            
            # Download and set credentials
            key_resp = requests.get(creds_blob)
            key_resp.raise_for_status()
            key_path = f"/tmp/{project_id}-key.json"
            with open(key_path, "wb") as key_file:
                key_file.write(key_resp.content)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
            
            # Initialize Vertex AI
            vertexai.init(project=project_id, location="us-central1")
            model = GenerativeModel("gemini-2.0-flash-001")
            
            # Prepare document and prompt
            mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
            document_part = Part.from_data(mime_type=mime_type, data=file_content)
            prompt_part = Part.from_text(template)
            
            config = GenerationConfig(
                max_output_tokens=8192,
                temperature=0,
                top_p=0.95
            )
            
            response = model.generate_content(
                [document_part, prompt_part],
                generation_config=config,
                stream=False
            )
            
            if not hasattr(response, "candidates") or not response.candidates:
                continue
            
            candidate_content = response.candidates[0].content.parts[0].text
            
            # Clean up JSON content
            if candidate_content.startswith("```json"):
                candidate_content = candidate_content.split("```json", 1)[-1].rsplit("```", 1)[0].strip()
            elif candidate_content.startswith("```"):
                candidate_content = candidate_content.split("```", 1)[-1].rsplit("```", 1)[0].strip()
            
            try:
                parsed_result = json.loads(candidate_content)
                logging.info(f"Successfully parsed response from {name}")
                return parsed_result
            except json.JSONDecodeError:
                continue
                
        except Exception as e:
            logging.error(f"{name} project failed: {e}")
            continue
    
    raise RuntimeError("All Vertex AI projects failed. Unable to analyze document.")