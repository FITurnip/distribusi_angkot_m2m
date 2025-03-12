import paho.mqtt.client as mqtt
from devices.device import Device
from devices.angkot import Angkot
from devices.halt_point import HaltPoint
import logging

# Configure logging
logging.basicConfig(filename='mqtt_handler.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

class MQTTHandler:
    def __init__(self, broker, port, topics):
        self.broker = broker
        self.port = port
        self.topics = topics
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
    
    def start(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_forever()
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT Broker!")
            for topic in self.topics:
                client.subscribe(topic)
                logging.info(f"Subscribed to {topic}")
        else:
            logging.info(f"Failed to connect, return code {rc}")
    
    def on_message(self, client, userdata, msg):
        print(f"Received message on {msg.topic}: {msg.payload.decode('utf-8')}")  # Debug print
        logging.info(f"Received message on {msg.topic}: {msg.payload.decode('utf-8')}")
        topic_parts = msg.topic.split("/")
        device_type = topic_parts[1]  # Extract device type
        device_id = topic_parts[2]  # Extract device ID
        category = topic_parts[3]  # Either 'request' or 'response'
        payload = msg.payload.decode("utf-8").split(",")  # Split payload using comma delimiter
        
        logging.info(f"[{category.upper()}] from {device_type}/{device_id}: {payload}")
        self.process_message(device_type, device_id, category, payload)
    
    def process_message(self, device_type, device_id, category, payload):
        print(f"Processing message: {device_type}, {device_id}, {category}, {payload}")  # Debug print
        logging.info(f"Processing message: {device_type}, {device_id}, {category}, {payload}")
        if category == "request":
            device = self.get_device_instance(device_type, device_id, payload)
            method_name = f"handle_{payload[0]}"  # Determine method dynamically
            if hasattr(device, method_name):
                response_payload = getattr(device, method_name)()
            else:
                response_payload = device.handle_request()
            
            response_topic = f"device/{device_type}/{device_id}/response"
            self.client.publish(response_topic, response_payload)
            logging.info(f"Sent response to {device_type}/{device_id}: {response_payload}")
    
    def get_device_instance(self, device_type, device_id, payload):
        if device_type == "HP":
            return HaltPoint(device_id, payload)
        elif device_type == "AK":
            return Angkot(device_id, payload)
        else:
            return Device(device_id, payload)
