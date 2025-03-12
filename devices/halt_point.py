from devices.device import Device

class HaltPoint(Device):
    def handle_nearest_angkot(self):
        rute_id = self.payload[0]

        # Get the halt's lat and long first
        halt_data = self.db_service.execute_raw_query(
            collection_name="halt",
            pipeline=[
                {"$match": {"id": self.device_id}},  # Find the halt point by ID
                {"$project": {"_id": 0, "lat": 1, "long": 1}}  # Get only lat and long
            ]
        )

        if not halt_data:
            return f"ACK,unknown,{self.device_id},halt_not_found"
        
        halt_location = halt_data[0]  # Extract lat and long from halt data
        halt_lat = halt_location["lat"]
        halt_long = halt_location["long"]

        # Get nearest angkot using $geoNear
        result = self.db_service.execute_raw_query(
            collection_name="posisi_angkot",
            query=[
                {
                    "$geoNear": {
                        "near": {"type": "Point", "coordinates": [halt_long, halt_lat]},  # Halt location
                        "distanceField": "distance",  # Store calculated distance
                        "spherical": True  # Use spherical calculation (Earth-like distance)
                    }
                },
                {"$lookup": {
                    "from": "angkot",
                    "localField": "angkot_id",
                    "foreignField": "id",
                    "as": "angkot_data"
                }},
                {"$unwind": "$angkot_data"},  # Convert angkot_data array into single objects
                {"$match": {"angkot_data.rute_id": rute_id}},  # Match angkot with requested route
                {"$lookup": {
                    "from": "halt",
                    "localField": "angkot_data.halt_ids",
                    "foreignField": "id",
                    "as": "halt_data"
                }},
                {"$match": {"halt_data.id": self.device_id}},  # Ensure angkot stops at this halt
                {"$sort": {"distance": 1}},  # Sort by nearest angkot
                {"$limit": 1},  # Get only the nearest angkot
                {"$project": {
                    "_id": 0,
                    "angkot_id": "$angkot_id",
                    "distance": 1,
                    "lat": 1,
                    "long": 1
                }}
            ]
        )

        if not result:
            return f"ACK,unknown,{self.device_id},no_angkot_found"

        nearest_angkot = result[0]  # Get nearest angkot
        distance = nearest_angkot["distance"]  # Distance in meters

        # Estimate travel time (assuming average speed of 40 km/h ≈ 11.1 m/s)
        duration = round(distance / 11.1)  # Time in seconds

        return f"ACK,{distance:.2f},{duration}"
    
    def monitor(self, rute_id, range_time):
        result = self.db_service.execute_raw_query(
            collection_name="request_angkot",
            query=[
                {"$match": {"halt_id": self.device_id}},
                {"$match": {"rute_id": rute_id}},
                {
                    "$match": {
                        "timestamp": {
                            "$gte": {
                                "$dateSubtract": {
                                    "startDate": "$$NOW",
                                    "unit": "minute",
                                    "amount": range_time
                                }
                            }
                        }
                    }
                }
            ]
        )
        return result
    
    def handle_request_angkot(self):
        action = self.payload[0]
        rute_id = self.payload[1]
        range_time = self.payload[2]

        if action == 'I': # Insert new document
            rute_id = self.payload[1]
            self.db_service.insert_document("request_angkot", {
                "halt_id": self.device_id,
                "rute_id": rute_id
            })

        result = self.monitor(rute_id=rute_id, range_time=range_time)
        request_counter = len(result)
        return f"ACK,{request_counter}"

    def handle_init_halt(self):
        route_ids = [1, 2, 3, 4, 5, ]
        return f"ACK,{','.join(map(str, route_ids))}"
