from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime

class AgentDataBase:
    def __init__(self, client='mongodb://localhost:27017/', db_name='EtebarTaban', collection_name='specificationsPerDay'):
        # initialize database
        self.client = MongoClient(client)
        self.database = self.client[db_name]
        self.collection = self.database[collection_name]
        
    def update(self, commissions, collection_name='commission'):
        for message_dict in commissions:
            existing_doc = self.database[collection_name].find_one({'AccountCode':message_dict['AccountCode']})
            if existing_doc:
                if datetime.strptime(existing_doc['Date'], "%Y-%m-%dT%H:%M:%S").date() > datetime.strptime(message_dict['Date'], "%Y-%m-%dT%H:%M:%S").date():
                    continue
                
                self.database[collection_name].update_one({"AccountCode":existing_doc['AccountCode']},{"$set":{"TotalCommission":existing_doc['TotalCommission'] + message_dict['TotalCommission']}} )
                self.database[collection_name].update_one({"AccountCode":existing_doc['AccountCode']},{"$set":{"Date": message_dict['Date']}} )
                
                # commission = {
                #     "TotalCommission": existing_doc['TotalCommission'] + message_dict['TotalCommission'],
                #     "AccountCode": existing_doc['AccountCode'],
                #     "Date": message_dict['Date']
                # }
                # self.database[collection_name].replace_one(existing_doc, commission)
                print("update the commission")
            else:
                self.database[collection_name].insert_one(message_dict)
                print("insert the first commission")
            
    # for insert document into database
    # if document exist replace into database
    def upsert(self, message_dict):
        existing_doc = self.collection.find_one({'AccountCode': message_dict['AccountCode'],
                                                 'Date': message_dict['Date']})
        # if we have disticnt data then replace
        if existing_doc:
            self.collection.replace_one({'AccountCode': existing_doc['AccountCode']}, message_dict)
            # print(existing_doc['AccountCode'])
            # time.sleep(2)
            print("replace the message")
        else:
            # print(message_dict['AccountCode'])
            # time.sleep(2)
            self.collection.insert_one(message_dict)
            print("insert the message")

    # delete document
    def delete(self, message_dict):
        self.collection.delete_one(message_dict)
        print("The message was deleted successfully")

    #delete collection
    def remove_collection(self, collection_name):
        self.database[collection_name].drop()

    # close database
    def close(self):
        if self.client:
            self.client.close()
            self.client = None
        print("The database was closed")

    # remove all of documents
    def clear_database(self):
        self.collection.delete_many({})
        print("clear the database")

    def check_connection(self):
        try:
            server_status = self.database.command("serverStatus")
            print("The connection is successful")
            return {"Status": "success", "Message": "Database connected successfully",
                    "Server_host": server_status["host"]}
        except PyMongoError as e:
            return {"status": "error", "Message": f"Database connection failed: {e}"}
        except Exception as e:
            return {"status": "error", "Message": f"Database error: {e}"}