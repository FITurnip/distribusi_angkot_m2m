from devices.device import Device
from bson import ObjectId

class Angkot(Device):
    def get_request_angkot_counter(self, halt_id, rute_id, range_time):
        result = self.db_service.execute_raw_query(
            collection_name="request_angkot",
            query=[
                {"$match": {"halt_id": ObjectId(halt_id)}},
                {"$match": {"rute_id": ObjectId(rute_id)}},
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
        return len(result)

    def handle_nearest_halt(self):
        rute_id = self.payload[1]
        cur_lat = self.payload[2]
        cur_long = self.payload[3]
        range_time = self.payload[4]

        result = self.db_service.execute_raw_query(
            collection_name="halt",
            query=[
                {
                    "$geoNear": {
                        "near": {"type": "Point", "coordinates": [cur_lat, cur_long]},  # Halt location
                        "distanceField": "distance",  # Store calculated distance
                        "spherical": True  # Use spherical calculation (Earth-like distance)
                    }
                },
                {"$lookup": {
                    "from": "rute",
                    "localField": "id",
                    "foreignField": "halt_ids",
                    "as": "rute_data"
                }},
                {"$match": {"rute_data.id":ObjectId(rute_id)}},
                {"$sort": {"distance": 1}},  # Sort by nearest angkot
                {"$limit": 1},  # Get only the nearest angkot
            ]
        )

        nearest_halt = result[0]
        request_angkot_counter = self.get_request_angkot_counter(
            halt_id=nearest_halt["id"] , rute_id=rute_id, range_time=range_time
        )

        return f"ACK,{request_angkot_counter}"
    
    def handle_naik_turun_angkot(self):
        card_uid = self.payload[1]
        is_entry = int(self.payload[2])
        cur_lat = float(self.payload[3])
        cur_long = float(self.payload[4])

        result = self.db_service.execute_raw_query(
            collection_name="penumpang",
            query=[
                {"$match": {"card_uid":card_uid}},
            ]
        )

        total_result = len(result)
        if total_result == 0:
            return f"ACK,kartu_tidak_terdeteksi"
        elif total_result > 1 :
            return f"ACK,aktivitas_illegal"
        else:
            penumpang = result[0]
            print(penumpang)

            self.db_service.insert_document(collection_name="passenger_monitor", document= {
                "is_entry": is_entry,
                "angkot_id": ObjectId(self.device_id),
                "penumpang_id": ObjectId(penumpang["_id"]),
                "location": {
                    "type": "Point",
                    "coordinates": [cur_long, cur_lat]
                }
            })

            increamentPenumpang = 1 if is_entry else -1

            self.db_service.update_document(
                collection_name="angkot",
                query={"_id": ObjectId(self.device_id)},
                update_query={"$inc":{"jumlah_penumpang":increamentPenumpang}},
            )

            status = "OK"
            return f"ACK,{status},{is_entry}"

    def handle_mulai_operasi(self):
        self.db_service.update_document(
            collection_name="angkot",
            query={"_id": ObjectId(self.device_id)},
            update_query={"$set":{"jumlah_penumpang":0}},
            upsert_cond=True
        )

        status = "OK"
        return f"ACK,{status}"