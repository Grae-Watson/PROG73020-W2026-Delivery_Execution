from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

# TEMPORARY IN-MEMORY DELIVERY DATA
# This mock data simulates assignments coming from
# Order Orchestration or other teams.
# Later this can be replaced with DB / ODS integration.

deliveries = [
    {
        "order_id": "ORD1001",
        "customer_name": "Faisal",
        "address": "123 King St W, Kitchener",
        "items": ["Milk", "Eggs", "Bread"],
        "status": "Assigned",
        "eta": "25 mins",
        "driver": "Driver A"
    },
    {
        "order_id": "ORD1002",
        "customer_name": "Ahmed",
        "address": "45 University Ave, Waterloo",
        "items": ["Apples", "Carrots"],
        "status": "Out for Delivery",
        "eta": "15 mins",
        "driver": "Driver B"
    }
]

# Existing routes

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/login')
def login():
    return render_template('login.html')


@app.route('/providers')
def providers():
    return render_template('providers.html')


@app.route('/info')
def info():
    return render_template('info.html')


# DELIVERY EXECUTION UI ROUTES 
# These pages are my DT Delivery Execution contribution.
# /delivery           -> dashboard of all assignments
# /delivery/<id>      -> details of one delivery

@app.route('/delivery')
def delivery_dashboard():
    return render_template('delivery_dashboard.html', deliveries=deliveries)


@app.route('/delivery/<order_id>')
def delivery_details(order_id):
    delivery = next((d for d in deliveries if d["order_id"] == order_id), None)

    if not delivery:
        return render_template('info.html'), 404

    return render_template('delivery_details.html', delivery=delivery)


# EXISTING RESTOCK INTEGRATION ENDPOINT
# Product Team -> Supply Team

@app.route('/api/v1/restock_request', methods=["GET", "POST"])
def restock_request():
    if request.method == "GET":
        return jsonify({
            "status": "success",
            "data": {"message": "Restock endpoint available"},
            "error": None
        }), 200

    if request.headers.get("X-API-Key") != "bestTeam":
        return jsonify({
            "status": "error",
            "data": None,
            "error": {
                "code": "UNAUTHORIZED",
                "message": "Invalid API key"
            }
        }), 401

    data = request.get_json()

    print(f"vendorId: {data.get('vendorId')}")
    print(f"manifest: {data.get('manifest')}")

    return jsonify({
        "status": "success",
        "data": {"message": "Restock request received"},
        "error": None
    }), 200


# DELIVERY EXECUTION API ENDPOINTS 
# These endpoints are created so other teams can
# communicate with Delivery Execution.

# Get all assigned deliveries
@app.route('/api/deliveries', methods=['GET'])
def get_deliveries():
    return jsonify({
        "status": "success",
        "data": deliveries,
        "error": None
    }), 200


# Get one delivery by order id
@app.route('/api/deliveries/<order_id>', methods=['GET'])
def get_delivery(order_id):
    delivery = next((d for d in deliveries if d["order_id"] == order_id), None)

    if not delivery:
        return jsonify({
            "status": "error",
            "data": None,
            "error": {"code": "NOT_FOUND", "message": "Delivery not found"}
        }), 404

    return jsonify({
        "status": "success",
        "data": delivery,
        "error": None
    }), 200


# Receive assignment from other teams
@app.route('/api/delivery/assignments', methods=['POST'])
def create_delivery_assignment():
    data = request.get_json()

    new_delivery = {
        "order_id": data.get("order_id"),
        "customer_name": data.get("customer_name"),
        "address": data.get("address"),
        "items": data.get("items", []),
        "status": data.get("status", "Assigned"),
        "eta": data.get("eta", "TBD"),
        "driver": data.get("driver", "Unassigned")
    }

    deliveries.append(new_delivery)

    return jsonify({
        "status": "success",
        "data": {
            "message": "Delivery assignment received",
            "delivery": new_delivery
        },
        "error": None
    }), 201


# Update delivery progress status
@app.route('/api/delivery/status', methods=['POST'])
def update_delivery_status():
    data = request.get_json()

    order_id = data.get("order_id")
    new_status = data.get("status")

    delivery = next((d for d in deliveries if d["order_id"] == order_id), None)

    if not delivery:
        return jsonify({
            "status": "error",
            "data": None,
            "error": {"code": "NOT_FOUND", "message": "Delivery not found"}
        }), 404

    delivery["status"] = new_status

    return jsonify({
        "status": "success",
        "data": {
            "message": "Delivery status updated",
            "delivery": delivery
        },
        "error": None
    }), 200


# Final completion endpoint to confirm delivery drop-off
@app.route('/api/delivery/complete', methods=['POST'])
def complete_delivery():
    data = request.get_json()
    order_id = data.get("order_id")

    delivery = next((d for d in deliveries if d["order_id"] == order_id), None)

    if not delivery:
        return jsonify({
            "status": "error",
            "data": None,
            "error": {"code": "NOT_FOUND", "message": "Delivery not found"}
        }), 404

    delivery["status"] = "Delivered"

    return jsonify({
        "status": "success",
        "data": {
            "message": "Delivery completed successfully",
            "delivery": delivery
        },
        "error": None
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7500, debug=True)
