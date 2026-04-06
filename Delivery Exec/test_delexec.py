import importlib
import sys
from unittest.mock import Mock
import pytest


@pytest.fixture
def app_module(monkeypatch):
    """
    Import delexec.py safely by mocking the SSH/SFTP side effects that happen at import time.
    """

    class FakeSFTP:
        def get(self, remote_path, local_path):
            return None

        def close(self):
            return None

    class FakeSSHClient:
        def set_missing_host_key_policy(self, policy):
            return None

        def connect(self, host, port, username, password):
            return None

        def open_sftp(self):
            return FakeSFTP()

        def close(self):
            return None

    class FakeAutoAddPolicy:
        pass

    fake_paramiko = Mock()
    fake_paramiko.SSHClient.return_value = FakeSSHClient()
    fake_paramiko.AutoAddPolicy = FakeAutoAddPolicy

    monkeypatch.setitem(sys.modules, "paramiko", fake_paramiko)

    if "delexec" in sys.modules:
        del sys.modules["delexec"]

    module = importlib.import_module("delexec")
    module.app.config["TESTING"] = True
    return module


@pytest.fixture
def client(app_module):
    return app_module.app.test_client()


def valid_order():
    return {
        "warehouseOrderNumber": "WH-100045",
        "destination": {
            "addressLine1": "123 King St W",
            "addressLine2": "Unit 5",
            "city": "Waterloo",
            "province": "ON",
            "postalCode": "N2L3G1",
        },
        "specialRequirements": {
            "refrigeration": True,
            "dropOff": False,
        },
        "requestedAtUtc": "2026-04-02T15:00:00Z",
    }


def test_validate_order_payload_accepts_valid_payload(app_module):
    code, message, details = app_module.validate_order_payload(valid_order())
    assert code is None
    assert message is None
    assert details is None


def test_validate_order_payload_rejects_non_json_object(app_module):
    code, message, details = app_module.validate_order_payload("not-a-dict")
    assert code == "INVALID_PAYLOAD"
    assert "JSON object" in message
    assert details is None


def test_validate_order_payload_missing_top_level_field(app_module):
    payload = valid_order()
    del payload["destination"]

    code, message, details = app_module.validate_order_payload(payload)

    assert code == "MISSING_FIELD"
    assert message == "Missing required field: destination"
    assert details == {"field": "destination"}


def test_validate_order_payload_rejects_invalid_city(app_module):
    payload = valid_order()
    payload["destination"]["city"] = "Hamilton"

    code, message, details = app_module.validate_order_payload(payload)

    assert code == "INVALID_CITY"
    assert "City must be one of Waterloo, Kitchener, Cambridge" in message
    assert details["field"] == "destination.city"
    assert "Waterloo" in details["allowed"]


def test_validate_order_payload_rejects_non_boolean_dropoff(app_module):
    payload = valid_order()
    payload["specialRequirements"]["dropOff"] = "false"

    code, message, details = app_module.validate_order_payload(payload)

    assert code == "INVALID_PAYLOAD"
    assert message == "specialRequirements.dropOff must be boolean"
    assert details == {"field": "specialRequirements.dropOff"}


def test_normalize_order_for_ods_trims_fields(app_module):
    payload = {
        "warehouseOrderNumber": "  WH-200001  ",
        "destination": {
            "addressLine1": "  456 Queen St  ",
            "addressLine2": "  Apt 9  ",
            "city": "  Waterloo  ",
            "province": "  ON  ",
            "postalCode": "  N2L3G1  ",
        },
        "specialRequirements": {
            "refrigeration": True,
            "dropOff": False,
        },
        "requestedAtUtc": "2026-04-02T15:00:00Z",
    }

    normalized = app_module.normalize_order_for_ods(payload)

    assert normalized["warehouseOrderNumber"] == "WH-200001"
    assert normalized["destination"]["addressLine1"] == "456 Queen St"
    assert normalized["destination"]["addressLine2"] == "Apt 9"
    assert normalized["destination"]["city"] == "Waterloo"
    assert normalized["destination"]["province"] == "ON"
    assert normalized["destination"]["postalCode"] == "N2L3G1"


def test_health_route_returns_ok(client):
    response = client.get("/health")
    data = response.get_json()

    assert response.status_code == 200
    assert data["status"] == "ok"
    assert data["service"] == "delivery-exec"
    assert "timeUtc" in data


def test_version_route_returns_service_version(client):
    response = client.get("/version")
    data = response.get_json()

    assert response.status_code == 200
    assert data == {
        "service": "delivery-exec",
        "version": "1.0.0",
    }


def test_create_order_returns_202_when_ods_accepts(client, app_module, monkeypatch):
    mock_response = Mock()
    mock_response.status_code = 202
    mock_response.json.return_value = {
        "orderId": "ODS-123",
        "warehouseOrderNumber": "WH-100045",
        "status": "queued",
        "message": "Order accepted",
        "createdAtUtc": "2026-04-02T15:00:01Z",
    }

    mock_post = Mock(return_value=mock_response)
    monkeypatch.setattr(app_module.requests, "post", mock_post)

    response = client.post("/order", json=valid_order())
    data = response.get_json()

    assert response.status_code == 202
    assert data["message"] == "Order accepted by Delivery Execution and forwarded to ODS"
    assert data["odsResponse"]["orderId"] == "ODS-123"
    mock_post.assert_called_once()


def test_create_order_returns_400_for_invalid_input(client):
    bad_payload = valid_order()
    bad_payload["destination"]["city"] = "Hamilton"

    response = client.post("/order", json=bad_payload)
    data = response.get_json()

    assert response.status_code == 400
    assert data["error"]["code"] == "INVALID_CITY"


def test_get_order_returns_502_when_ods_is_unavailable(client, app_module, monkeypatch):
    import requests

    def raise_request_exception(*args, **kwargs):
        raise requests.RequestException("ODS is down")

    monkeypatch.setattr(app_module.requests, "get", raise_request_exception)

    response = client.get("/order/ODS-123")
    data = response.get_json()

    assert response.status_code == 502
    assert data["error"]["code"] == "ODS_UNAVAILABLE"
    assert "Failed to connect to ODS" in data["error"]["message"]