from services.mongodb_service import MongoDBService

class Device:
    def __init__(self, device_id, payload):
        self.device_id = device_id
        self.payload = payload
        self.db_service = MongoDBService()
    
    def handle_request(self):
        return f"ACK,unknown,{self.device_id},error"
