from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import requests
from datetime import datetime, timezone
import paramiko

app = Flask(__name__)

with open("config.json") as f:
    conn_params = json.load(f)

ODS_BASE_URL = "http://178.128.226.23:8001/api/v1"
ODS_ORDERS_URL = f"{ODS_BASE_URL}/orders"
ODS_HEADERS = {
    "X-API-Key": "kMJIoWBGA_A5xNOLH86NRc2yha_4N8n5u-r_zAmB6BZvDssj",
    "Content-Type": "application/json"
}

ssh = paramiko.SSHClient();
try:
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
    ssh.connect("68.183.203.17", 22, "sec1", "t&thmGV26cffZ@XoBrt0");
    stfp = ssh.open_sftp();
    try:
        stfp.get("/primary/CFP-Cambridge.csv", "C:/Users/Kirby/Downloads/Delivery Exec/CFP-Cambridge.csv");
    except FileNotFoundError as err:
        print("File not found on server");
    
    stfp.close();
finally:
    ssh.close();



ALLOWED_CITIES = {"Waterloo", "Kitchener", "Cambridge"}
SERVICE_NAME = "delivery-exec"
SERVICE_VERSION = "1.0.0"


def get_connection():
    return psycopg2.connect(
        cursor_factory=RealDictCursor,
        **conn_params
    )


def fetch_secret():
    query = "SELECT secret FROM dtsecrets WHERE teamname = 'Delivery Execution'"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def error_response(status_code: int, code: str, message: str, details=None):
    payload = {
        "error": {
            "code": code,
            "message": message
        }
    }
    if details is not None:
        payload["error"]["details"] = details
    return jsonify(payload), status_code


def validate_order_payload(data):
    if not isinstance(data, dict):
        return "INVALID_PAYLOAD", "Request body must be a JSON object", None

    required_top = ["warehouseOrderNumber", "destination", "specialRequirements"]
    for field in required_top:
        if field not in data:
            return "MISSING_FIELD", f"Missing required field: {field}", {"field": field}

    warehouse_order_number = data.get("warehouseOrderNumber")
    destination = data.get("destination")
    special_requirements = data.get("specialRequirements")
    requested_at = data.get("requestedAtUtc")

    if not isinstance(warehouse_order_number, str) or not warehouse_order_number.strip():
        return (
            "INVALID_PAYLOAD",
            "warehouseOrderNumber must be a non-empty string",
            {"field": "warehouseOrderNumber"}
        )

    if not isinstance(destination, dict):
        return "INVALID_PAYLOAD", "destination must be an object", {"field": "destination"}

    required_destination = ["addressLine1", "city", "province", "postalCode"]
    for field in required_destination:
        if field not in destination:
            return "MISSING_FIELD", f"Missing required field: destination.{field}", {
                "field": f"destination.{field}"
            }

    if not isinstance(destination.get("addressLine1"), str) or not destination["addressLine1"].strip():
        return "INVALID_PAYLOAD", "destination.addressLine1 must be a non-empty string", {
            "field": "destination.addressLine1"
        }

    address_line_2 = destination.get("addressLine2")
    if address_line_2 is not None and not isinstance(address_line_2, str):
        return "INVALID_PAYLOAD", "destination.addressLine2 must be a string", {
            "field": "destination.addressLine2"
        }

    city = destination.get("city")
    if not isinstance(city, str) or city.strip() not in ALLOWED_CITIES:
        return "INVALID_CITY", "City must be one of Waterloo, Kitchener, Cambridge", {
            "field": "destination.city",
            "allowed": sorted(ALLOWED_CITIES)
        }

    province = destination.get("province")
    if not isinstance(province, str) or not province.strip():
        return "INVALID_PAYLOAD", "destination.province must be a non-empty string", {
            "field": "destination.province"
        }

    postal_code = destination.get("postalCode")
    if not isinstance(postal_code, str) or not postal_code.strip():
        return "INVALID_PAYLOAD", "destination.postalCode must be a non-empty string", {
            "field": "destination.postalCode"
        }

    if not isinstance(special_requirements, dict):
        return "INVALID_PAYLOAD", "specialRequirements must be an object", {
            "field": "specialRequirements"
        }

    for field in ["refrigeration", "dropOff"]:
        if field not in special_requirements:
            return "MISSING_FIELD", f"Missing required field: specialRequirements.{field}", {
                "field": f"specialRequirements.{field}"
            }

        if not isinstance(special_requirements[field], bool):
            return "INVALID_PAYLOAD", f"specialRequirements.{field} must be boolean", {
                "field": f"specialRequirements.{field}"
            }

    if requested_at is not None and (not isinstance(requested_at, str) or not requested_at.strip()):
        return "INVALID_PAYLOAD", "requestedAtUtc must be a non-empty string if provided", {
            "field": "requestedAtUtc"
        }

    return None, None, None


def normalize_order_for_ods(data):
    destination = data["destination"]
    normalized = {
        "warehouseOrderNumber": data["warehouseOrderNumber"].strip(),
        "destination": {
            "addressLine1": destination["addressLine1"].strip(),
            "city": destination["city"].strip(),
            "province": destination["province"].strip(),
            "postalCode": destination["postalCode"].strip()
        },
        "specialRequirements": {
            "refrigeration": data["specialRequirements"]["refrigeration"],
            "dropOff": data["specialRequirements"]["dropOff"]
        },
        "requestedAtUtc": data.get("requestedAtUtc", utc_now_iso())
    }

    if "addressLine2" in destination and destination["addressLine2"] is not None:
        normalized["destination"]["addressLine2"] = destination["addressLine2"].strip()

    return normalized


@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "service": SERVICE_NAME,
        "message": "Delivery Execution service is running"
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": SERVICE_NAME,
        "timeUtc": utc_now_iso()
    })


@app.route("/version", methods=["GET"])
def version():
    return jsonify({
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION
    })


@app.route("/secret", methods=["GET"])
def get_secret():
    try:
        rows = fetch_secret()
        if not rows:
            return jsonify({"secret": None})
        return jsonify({"secret": rows[0]["secret"]})
    except Exception as ex:
        return error_response(
            500,
            "DATABASE_ERROR",
            "Failed to fetch secret from database",
            {"details": str(ex)}
        )


@app.route("/order", methods=["POST"])
def create_order():
    data = request.get_json(silent=True)
    code, message, details = validate_order_payload(data)
    if code is not None:
        return error_response(400, code, message, details)

    ods_payload = normalize_order_for_ods(data)

    try:
        response = requests.post(
            ODS_ORDERS_URL,
            headers=ODS_HEADERS,
            json=ods_payload,
            timeout=10
        )
    except requests.RequestException as ex:
        return error_response(
            502,
            "ODS_UNAVAILABLE",
            "Failed to connect to ODS",
            {"details": str(ex)}
        )

    try:
        response_data = response.json()
    except ValueError:
        response_data = {
            "rawResponse": response.text
        }

    if response.status_code == 202:
        return jsonify({
            "message": "Order accepted by Delivery Execution and forwarded to ODS",
            "odsResponse": response_data
        }), 202

    if response.status_code in (400, 401, 403, 404, 409, 429):
        return jsonify(response_data), response.status_code

    return error_response(
        502,
        "ODS_ERROR",
        "ODS returned an unexpected response",
        {
            "statusCode": response.status_code,
            "response": response_data
        }
    )


@app.route("/order", methods=["GET"])
def list_orders():
    params = {}

    status = request.args.get("status")
    city = request.args.get("city")
    warehouse_order_number = request.args.get("warehouseOrderNumber")
    page = request.args.get("page")
    page_size = request.args.get("pageSize")

    if status:
        params["status"] = status
    if city:
        params["city"] = city
    if warehouse_order_number:
        params["warehouseOrderNumber"] = warehouse_order_number
    if page:
        params["page"] = page
    if page_size:
        params["pageSize"] = page_size

    try:
        response = requests.get(
            ODS_ORDERS_URL,
            headers=ODS_HEADERS,
            params=params,
            timeout=10
        )
    except requests.RequestException as ex:
        return error_response(
            502,
            "ODS_UNAVAILABLE",
            "Failed to connect to ODS",
            {"details": str(ex)}
        )

    try:
        return jsonify(response.json()), response.status_code
    except ValueError:
        return error_response(
            502,
            "ODS_ERROR",
            "ODS returned a non-JSON response",
            {
                "statusCode": response.status_code,
                "response": response.text
            }
        )


@app.route("/order/<order_id>", methods=["GET"])
def get_order(order_id):
    try:
        response = requests.get(
            f"{ODS_ORDERS_URL}/{order_id}",
            headers=ODS_HEADERS,
            timeout=10
        )
    except requests.RequestException as ex:
        return error_response(
            502,
            "ODS_UNAVAILABLE",
            "Failed to connect to ODS",
            {"details": str(ex)}
        )

    try:
        return jsonify(response.json()), response.status_code
    except ValueError:
        return error_response(
            502,
            "ODS_ERROR",
            "ODS returned a non-JSON response",
            {
                "statusCode": response.status_code,
                "response": response.text
            }
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)