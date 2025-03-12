from services.mqtt_handler import MQTTHandler

BROKER = "localhost"
PORT = 1883
TOPICS = ["device/+/+/request", "device/+/+/response"]

mqtt_handler = MQTTHandler(BROKER, PORT, TOPICS)
mqtt_handler.start()
