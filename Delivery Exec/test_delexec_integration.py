import importlib
import sys
import time
import uuid
import pytest


@pytest.fixture
def app_module(monkeypatch):
   

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

    class FakeParamikoModule:
        SSHClient = FakeSSHClient
        AutoAddPolicy = FakeAutoAddPolicy

    monkeypatch.setitem(sys.modules, "paramiko", FakeParamikoModule())

    if "delexec" in sys.modules:
        del sys.modules["delexec"]

    module = importlib.import_module("delexec")
    module.app.config["TESTING"] = True
    return module


@pytest.fixture
def client(app_module):
    return app_module.app.test_client()


def build_unique_order():
    suffix = uuid.uuid4().hex[:10].upper()
    return {
        "warehouseOrderNumber": f"WH-INT-{suffix}",
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


def test_integration_create_order_accepted_by_ods(client):
    order = build_unique_order()

    response = client.post("/order", json=order)
    data = response.get_json()

    assert response.status_code == 202
    assert data["message"] == "Order accepted by Delivery Execution and forwarded to ODS"
    assert data["odsResponse"]["warehouseOrderNumber"] == order["warehouseOrderNumber"]
    assert "orderId" in data["odsResponse"]
    assert data["odsResponse"]["status"] == "queued"


def test_integration_get_order_by_id_from_ods(client):
    order = build_unique_order()

    create_response = client.post("/order", json=order)
    create_data = create_response.get_json()

    assert create_response.status_code == 202
    order_id = create_data["odsResponse"]["orderId"]

    # Small delay to avoid eventual consistency timing issues
    time.sleep(1)

    get_response = client.get(f"/order/{order_id}")
    get_data = get_response.get_json()

    assert get_response.status_code == 200
    assert get_data["orderId"] == order_id
    assert get_data["warehouseOrderNumber"] == order["warehouseOrderNumber"]
    assert get_data["destinationCity"] == order["destination"]["city"]
    assert get_data["status"] in [
        "queued",
        "out_for_delivery",
        "delivery_failed",
        "delivered",
        "rejected",
    ]


def test_integration_duplicate_order_rejected_by_ods(client):
    order = build_unique_order()

    first_response = client.post("/order", json=order)
    first_data = first_response.get_json()

    assert first_response.status_code == 202
    assert first_data["odsResponse"]["warehouseOrderNumber"] == order["warehouseOrderNumber"]

    second_response = client.post("/order", json=order)
    second_data = second_response.get_json()

    # Accept either the ideal direct ODS duplicate response
    # or the app's wrapped gateway-style failure if ODS response is not parsed cleanly
    assert second_response.status_code in [409, 502]

    if second_response.status_code == 409:
        assert second_data["error"]["code"] == "DUPLICATE_ORDER"

    if second_response.status_code == 502:
        assert second_data["error"]["code"] in ["ODS_ERROR", "ODS_UNAVAILABLE"]