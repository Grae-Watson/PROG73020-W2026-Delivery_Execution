from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import requests
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional, Tuple
import threading
import time
import os
from pathlib import Path
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

# Customer & Subscriptions callback URL.
# Prefer setting this as an environment variable in real deployments.
CS_URL = os.getenv("CUSTOMER_SUBS_URL", "").strip()

# CFP / SSH settings.
# These default to your current values but can be overridden with environment variables.
CFP_SSH_HOST = os.getenv("CFP_SSH_HOST", "68.183.203.17")
CFP_SSH_PORT = int(os.getenv("CFP_SSH_PORT", "22"))
CFP_SSH_USER = os.getenv("CFP_SSH_USER", "sec1")
CFP_SSH_PASSWORD = os.getenv("CFP_SSH_PASSWORD", "t&thmGV26cffZ@XoBrt0")
CFP_REMOTE_PATH = os.getenv("CFP_REMOTE_PATH", "/primary/CFP-Cambridge.csv")
CFP_LOCAL_PATH = os.getenv("CFP_LOCAL_PATH", str(Path.cwd() / "CFP-Cambridge.csv"))

ALLOWED_CITIES = {"Waterloo", "Kitchener", "Cambridge"}
SERVICE_NAME = "delivery-exec"
SERVICE_VERSION = "1.1.0"

# Stores the most recent startup CFP sync result so you can inspect it if needed.
CFP_SYNC_STATUS = {
    "attempted": False,
    "success": False,
    "message": "CFP sync not attempted yet",
    "localPath": CFP_LOCAL_PATH
}


@dataclass
class CSpayload:
    client_id: str
    produce: str
    meat: str
    dairy: str
    order_id: Optional[str] = None
    warehouse_order_number: Optional[str] = None


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


def fetch_aggs():
    query = "SELECT * FROM delexec"
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


def sync_cfp_file_at_startup():
    """
    Downloads the CFP file at startup, but never crashes the app if it fails.
    """
    CFP_SYNC_STATUS["attempted"] = True

    ssh = paramiko.SSHClient()
    sftp = None

    try:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            CFP_SSH_HOST,
            CFP_SSH_PORT,
            CFP_SSH_USER,
            CFP_SSH_PASSWORD,
            timeout=10
        )

        sftp = ssh.open_sftp()

        local_path_obj = Path(CFP_LOCAL_PATH)
        local_path_obj.parent.mkdir(parents=True, exist_ok=True)

        sftp.get(CFP_REMOTE_PATH, str(local_path_obj))

        CFP_SYNC_STATUS["success"] = True
        CFP_SYNC_STATUS["message"] = "CFP file downloaded successfully"
    except FileNotFoundError:
        CFP_SYNC_STATUS["success"] = False
        CFP_SYNC_STATUS["message"] = "CFP file not found on remote server"
    except Exception as ex:
        CFP_SYNC_STATUS["success"] = False
        CFP_SYNC_STATUS["message"] = f"CFP sync failed: {str(ex)}"
    finally:
        try:
            if sftp is not None:
                sftp.close()
        except Exception:
            pass

        try:
            ssh.close()
        except Exception:
            pass


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


def validate_order_aggregates(data) -> Tuple[Optional[str], Optional[str], Optional[dict], Optional[CSpayload]]:
    """
    Validates the /order/aggregates payload.
    Requires aggregate data plus either order_id or warehouseOrderNumber so ODS can be monitored.
    """
    if not isinstance(data, dict):
        return "INVALID_PAYLOAD", "Request body must be a JSON object", None, None

    required_fields = ["client_id", "produce", "meat", "dairy"]
    for field in required_fields:
        value = data.get(field)
        if not isinstance(value, str) or not value.strip():
            return (
                "INVALID_PAYLOAD",
                f"{field} must be a non-empty string",
                {"field": field},
                None
            )

    order_id = data.get("order_id")
    warehouse_order_number = data.get("warehouseOrderNumber")

    if (not isinstance(order_id, str) or not order_id.strip()) and (
        not isinstance(warehouse_order_number, str) or not warehouse_order_number.strip()
    ):
        return (
            "MISSING_FIELD",
            "Either order_id or warehouseOrderNumber is required",
            {"field": "order_id|warehouseOrderNumber"},
            None
        )

    payload = CSpayload(
        client_id=data["client_id"].strip(),
        produce=data["produce"].strip(),
        meat=data["meat"].strip(),
        dairy=data["dairy"].strip(),
        order_id=order_id.strip() if isinstance(order_id, str) and order_id.strip() else None,
        warehouse_order_number=warehouse_order_number.strip() if isinstance(warehouse_order_number, str) and warehouse_order_number.strip() else None
    )

    return None, None, None, payload


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


def resolve_order_id_from_warehouse_order_number(warehouse_order_number: str) -> Optional[str]:
    """
    If aggregates are registered using warehouseOrderNumber instead of order_id,
    resolve the ODS order ID first.
    """
    try:
        response = requests.get(
            ODS_ORDERS_URL,
            headers=ODS_HEADERS,
            params={"warehouseOrderNumber": warehouse_order_number, "pageSize": 1},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        if items:
            return items[0].get("orderId")
    except Exception:
        return None

    return None


def send_aggregates_to_customers_subs(payload: CSpayload, final_status: str):
    """
    Sends a completion/update payload to Customer & Subscriptions.
    Returns a tuple: (success: bool, details: dict)
    """
    if not CS_URL:
        return False, {
            "message": "CUSTOMER_SUBS_URL is not configured",
            "sent": False
        }

    body = {
        "client_id": payload.client_id,
        "produce": payload.produce,
        "meat": payload.meat,
        "dairy": payload.dairy,
        "order_id": payload.order_id,
        "warehouseOrderNumber": payload.warehouse_order_number,
        "delivery_status": final_status,
        "sentAtUtc": utc_now_iso()
    }

    try:
        response = requests.post(
            CS_URL,
            json=body,
            timeout=10
        )

        response_body = None
        try:
            response_body = response.json()
        except ValueError:
            response_body = {"rawResponse": response.text}

        if 200 <= response.status_code < 300:
            return True, {
                "message": "Aggregate update sent successfully",
                "sent": True,
                "statusCode": response.status_code,
                "response": response_body
            }

        return False, {
            "message": "Customer & Subscriptions returned a non-success response",
            "sent": False,
            "statusCode": response.status_code,
            "response": response_body
        }
    except requests.RequestException as ex:
        return False, {
            "message": "Failed to send aggregate update to Customer & Subscriptions",
            "sent": False,
            "details": str(ex)
        }


def monitor_ODS(payload: CSpayload, poll_seconds: int = 5, max_checks: int = 120):
    """
    Monitors ODS until the order reaches a final state.
    Final states handled:
    - delivered: notify Customer & Subscriptions
    - rejected: stop without sending
    Notes:
    - ODS status values are lowercase.
    """
    order_id = payload.order_id

    if not order_id and payload.warehouse_order_number:
        order_id = resolve_order_id_from_warehouse_order_number(payload.warehouse_order_number)
        if order_id:
            payload.order_id = order_id

    if not order_id:
        return {
            "success": False,
            "message": "Unable to resolve order ID for aggregate monitoring"
        }

    for _ in range(max_checks):
        try:
            response = requests.get(
                f"{ODS_ORDERS_URL}/{order_id}",
                headers=ODS_HEADERS,
                timeout=10
            )

            if response.status_code != 200:
                time.sleep(poll_seconds)
                continue

            order_data = response.json()
            status = str(order_data.get("status", "")).strip().lower()

            if status == "delivered":
                sent, notify_result = send_aggregates_to_customers_subs(payload, final_status=status)
                return {
                    "success": sent,
                    "message": "Order delivered; notification attempt completed",
                    "orderId": order_id,
                    "status": status,
                    "notification": notify_result
                }

            if status == "rejected":
                return {
                    "success": False,
                    "message": "Order was rejected; no aggregate notification sent",
                    "orderId": order_id,
                    "status": status
                }

            # queued / out_for_delivery / delivery_failed -> keep polling
            time.sleep(poll_seconds)

        except requests.RequestException as ex:
            time.sleep(poll_seconds)

    return {
        "success": False,
        "message": "Timed out while monitoring ODS order status",
        "orderId": order_id
    }


def monitor_ODS_async(payload: CSpayload):
    """
    Background wrapper so /order/aggregates can return immediately.
    """
    try:
        monitor_ODS(payload)
    except Exception:
        # Swallow background-thread exceptions so they don't crash the app.
        pass


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


@app.route("/cfp/status", methods=["GET"])
def cfp_status():
    return jsonify(CFP_SYNC_STATUS), 200


@app.route("/order/aggregates", methods=["POST"])
def aggregates():
    """
    Registers aggregate/customer-subscription data and begins monitoring ODS.
    Expected payload example:
    {
      "client_id": "C-1001",
      "produce": "weekly produce box",
      "meat": "beef",
      "dairy": "milk",
      "order_id": "ODS-ORDER-ID"
    }

    Or use warehouseOrderNumber instead of order_id:
    {
      "client_id": "C-1001",
      "produce": "weekly produce box",
      "meat": "beef",
      "dairy": "milk",
      "warehouseOrderNumber": "WH-100045"
    }
    """
    data = request.get_json(silent=True)
    code, message, details, payload = validate_order_aggregates(data)
    if code is not None:
        return error_response(400, code, message, details)

    worker = threading.Thread(target=monitor_ODS_async, args=(payload,), daemon=True)
    worker.start()

    return jsonify({
        "message": "Aggregate monitoring started",
        "client_id": payload.client_id,
        "order_id": payload.order_id,
        "warehouseOrderNumber": payload.warehouse_order_number
    }), 202


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


# Safe startup CFP sync.
sync_cfp_file_at_startup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)