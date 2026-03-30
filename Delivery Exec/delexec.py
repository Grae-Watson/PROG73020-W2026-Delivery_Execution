from flask import Flask, render_template, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import requests

app = Flask(__name__)


with open("config.json") as f:
    conn_params = json.load(f)

url = "http://178.128.226.23:8001/api/v1/orders/"
apikey = {
    "X-API-Key": "kMJIoWBGA_A5xNOLH86NRc2yha_4N8n5u-r_zAmB6BZvDssj"
}

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

@app.route("/")
def index():
    return "Hi :)";

@app.route("/order", methods=["GET"])
def show_orders():
    response = requests.get(url,headers=apikey);
    return response.json();


#eg id: b7f66ae4-da29-40f9-87dc-cca980420389
@app.route("/order/<id>", methods=["GET"])
def show_orders_with_id(id):
    response = requests.get(url + id,headers=apikey);
    return response.json();

@app.route("/secret", methods=["GET"])
def get_secret():
    rows = fetch_secret()
    if not rows:
        return jsonify({"secret": None})
    return jsonify({"secret": rows[0]["secret"]})
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

