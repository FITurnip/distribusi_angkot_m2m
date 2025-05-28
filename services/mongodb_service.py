from pymongo import MongoClient

class MongoDBService:
    def __init__(self, uri="mongodb://newadmin:newpassword123@localhost:27017/?authSource=admin", db_name="monitoring_distribusi_angkot"):
        try:
            self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)  # 5-second timeout
            self.client.admin.command('ping')  # Attempt to ping the server
            print("MongoDB connection successful.")
            self.db = self.client[db_name]
        except ConnectionFailure as e:
            print("MongoDB connection failed:", e)

    def insert_document(self, collection_name, document):
        collection = self.db[collection_name]
        return collection.insert_one(document).inserted_id

    def find_document(self, collection_name, query):
        collection = self.db[collection_name]
        return collection.find_one(query)

    def get_all_documents(self, collection_name):
        collection = self.db[collection_name]
        return list(collection.find())

    def update_document(self, collection_name, query, update_query, upsert_cond = False):
        collection = self.db[collection_name]
        return collection.update_one(query, update_query, upsert_cond ).modified_count

    def delete_document(self, collection_name, query):
        collection = self.db[collection_name]
        return collection.delete_one(query).deleted_count
    
    def inner_join_collections(self, from_collection, local_field, foreign_field, as_field, main_collection):
        collection = self.db[main_collection]
        pipeline = [
            {
                "$lookup": {
                    "from": from_collection,
                    "localField": local_field,
                    "foreignField": foreign_field,
                    "as": as_field
                }
            },
            {
                "$match": { as_field: { "$ne": [] } }  # Remove documents where the join result is empty
            }
        ]
        return list(collection.aggregate(pipeline))

    def execute_raw_query(self, collection_name, query):
        """
        Executes a raw MongoDB query (aggregation pipeline or find query).
        
        :param collection_name: The name of the collection to query.
        :param query: The raw MongoDB query (list for aggregation, dict for find).
        :return: The query result as a list.
        """
        collection = self.db[collection_name]

        print(f"exec {collection_name} {query}")
        
        if isinstance(query, list):  # Aggregation query
            return list(collection.aggregate(query))
        elif isinstance(query, dict):  # Simple find query
            return list(collection.find(query))
        else:
            raise ValueError("Query must be a dictionary (find) or a list (aggregate)")
