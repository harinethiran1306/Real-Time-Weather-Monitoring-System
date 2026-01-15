#include <PubSubClient.h>
#include <WiFi.h>
#include "DHT.h"
#define DHTTYPE DHT11

// WiFi and MQTT configuration
const char* WIFI_SSID = "Hari’s iPhone";
const char* WIFI_PASSWORD = "1234567890";
const char* MQTT_SERVER = "34.134.162.101";
const char* MQTT_TOPIC = "iot/weathermonitoring";
const int MQTT_PORT = 8883;

// Sensor pins
const int dht11Pin = 42;  // DHT11 sensor pin
const int rainPin = 4;    // Rain sensor pin

// Variables
char buffer[256];         // Buffer for MQTT messages
DHT dht(dht11Pin, DHTTYPE);
WiFiClient espClient;
PubSubClient client(espClient);

void setup_wifi() {
  Serial.print("Connecting to WiFi...");
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWiFi connected!");
  Serial.print("IP Address: ");
  Serial.println(WiFi.localIP());
}

void reconnect() {
  while (!client.connected()) {
    Serial.print("Attempting MQTT connection...");
    if (client.connect("ESP32Client")) {
      Serial.println("Connected to MQTT server!");
    } else {
      Serial.print("Failed to connect, rc=");
      Serial.print(client.state());
      Serial.println(" Retrying in 5 seconds...");
      delay(5000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  dht.begin();
  pinMode(rainPin, INPUT);  // Set rain sensor pin as input
  setup_wifi();
  client.setServer(MQTT_SERVER, MQTT_PORT);
}

void loop() {
  if (!client.connected()) {
    reconnect();
  }
  client.loop();

  // Read DHT11 sensor data
  float temperature = dht.readTemperature();
  float humidity = dht.readHumidity();
  if (isnan(temperature) || isnan(humidity)) {
    Serial.println("Failed to read from DHT sensor!");
    return;
  }

  // Read Rain sensor data
  int rainValue = digitalRead(rainPin);
  const char* rainStatus = (rainValue == HIGH) ? "No Rain" : "Rain Detected";

  // Prepare and send MQTT message
  sprintf(buffer, "Temperature: %.2f °C, Humidity: %.2f %%, Rain: %s", temperature, humidity, rainStatus);
  client.publish(MQTT_TOPIC, buffer);
  Serial.println(buffer);

  delay(5000);  // Wait 5 seconds before next reading
}
