import pymongo
from google.cloud import bigquery
from datetime import datetime
import paho.mqtt.client as mqtt
import os
import json
from google.protobuf.timestamp_pb2 import Timestamp

# ==============================
# Google Cloud Configuration
# ==============================
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-account-key.json'

# ==============================
# MongoDB Configuration (optional)
# ==============================
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["weathermonitoring"]
collection = db["iot"]

# ==============================
# BigQuery Configuration
# ==============================
bigquery_client = bigquery.Client()
dataset_id = 'weather_monitoring'
table_id = 'sensor_readings'

table_ref = bigquery_client.dataset(dataset_id).table(table_id)
table = bigquery_client.get_table(table_ref)

# ==============================
# MQTT Configuration (MATCH ARDUINO)
# ==============================
mqtt_broker_address = "34.134.162.101"
mqtt_port = 8883
mqtt_topic = "iot/weathermonitoring"

# ==============================
# Payload Parser
# ==============================
def parse_mqtt_payload(payload):
    """
    Temperature: XX.XX °C, Humidity: YY.YY %, Rain: No Rain / Rain Detected
    """
    try:
        parts = payload.split(", ")

        temperature = float(parts[0].split(": ")[1].replace(" °C", ""))
        humidity = float(parts[1].split(": ")[1].replace(" %", ""))
        rain_text = parts[2].split(": ")[1]

        # Convert rain status to INTEGER
        rain = 0 if rain_text == "No Rain" else 1

        return temperature, humidity, rain
    except Exception as e:
        print("Payload parsing error:", e)
        return None, None, None

# ==============================
# MQTT Callbacks
# ==============================
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("Connected to MQTT broker")
        client.subscribe(mqtt_topic)
    else:
        print(f"MQTT connection failed: {reason_code}")

def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    print("Received:", payload)

    temperature, humidity, rain = parse_mqtt_payload(payload)
    if temperature is None:
        return

    row = {
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": temperature,
        "humidity": humidity,
        "rain": rain
    }

    errors = bigquery_client.insert_rows_json(table, [row])
    if errors:
        print("BigQuery insert errors:", errors)
    else:
        print("Data inserted into BigQuery")

# ==============================
# MQTT Client Setup
# ==============================
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

client.connect(mqtt_broker_address, mqtt_port, 60)

print("Listening for sensor data...")
client.loop_forever()