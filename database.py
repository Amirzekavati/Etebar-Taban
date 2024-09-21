from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from datetime import datetime
import time

class AgentDataBase:
    def __init__(self, client='mongodb://localhost:27017/', db_name='EtebarTaban', collection_name='all Transactions'):
        # initialize database
        self.client = MongoClient(client)
        self.database = self.client[db_name]
        self.collection = self.database[collection_name]

    def find_person(self, accountCode, collection_name="total commissions person"):
        total = self.database[collection_name].find_one({'AccountCode': accountCode})
        if total:
            print(f"Total commissions in this period of time is: {total['TotalCommission']}")
        else:
            print(f"No commissions found for account: {accountCode}")

    def analysis_update(self, fromDate, toDate, collection_name='total commissions person'):
        documents = self.database["all Transactions"].find({"Date":{"$gte":fromDate,"$lte":toDate}})

        for document in documents:
            existing_doc = self.database[collection_name].find_one({
                'AccountCode': document['AccountCode']
            })   
            if existing_doc:
                self.database[collection_name].update_one(
                    {'AccountCode': existing_doc['AccountCode']},
                    {"$set":{'TotalCommission': existing_doc['TotalCommission'] + document['TotalCommission']}}
                )
                print("update the commission")
            else:
                new_doc = {
                    'TotalCommission': document['TotalCommission'],
                    'AccountCode': document['AccountCode'],
                    'Date': document['Date']
                }

                self.database[collection_name].insert_one(new_doc)
                print("insert the new commission")

    # for insert document into database
    # if document exist replace into database
    def upsert(self, message_dict ,collection_name):
        existing_doc = self.database[collection_name].find_one({
            'AccountCode': message_dict['AccountCode'],
            'Date': message_dict['Date']
            }
        )
        # if we have disticnt data then replace
        if existing_doc:
            self.database[collection_name].replace_one({'AccountCode': existing_doc['AccountCode']}, message_dict)
            print("replace the message")
        else:
            self.database[collection_name].insert_one(message_dict)
            print("insert the message")

    # delete document
    def delete(self, message_dict, collection_name):
        result = self.database[collection_name].delete_one(message_dict)
        if result.deleted_count > 0:
            print("The message was deleted successfully")
        else:
            print("No matching document found to delete")

    #delete collection
    def remove_collection(self, collection_name):
        self.database[collection_name].drop()
        print(f"Collection '{collection_name}' dropped successfully")

    # close database
    def close(self):
        if self.client:
            self.client.close()
            self.client = None
        print("The database was closed")

    # remove all of documents
    def clear_database(self, collection_name):
        result = self.database[collection_name].delete_many({})
        print(f"Cleared the database. {result.deleted_count} documents deleted.")

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