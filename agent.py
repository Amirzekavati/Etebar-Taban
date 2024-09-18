from database import AgentDataBase, UpdateOne
import requests
from datetime import datetime, timedelta
import time

class CommissionAgent:

    def __init__(self):
        # Create database
        self.db = AgentDataBase()
        
        # Set the header
        self.username = "Tahlil.taban"
        self.password = "Et@@2025MR"
        self.headers = {
            "UserName": self.username,
            "Password": self.password
        }
        self.base_url = "https://tbsapi.etbrokerage.ir/api/CustomerClub/GetCustomerTotalBrokerCommission"

    def fetch_data(self, from_date, to_date, page=1):
        url = f"{self.base_url}?fromDate={from_date}&toDate={to_date}&page={page}&pageSize=1000"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()  # Check for HTTP errors
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        
    def calculate_total_commission(self, record):
        return (
            float(record["OnlineSellCommission"]) +
            float(record["OnlineBuyCommission"]) +
            float(record["NonOnlineSellCommission"]) +
            float(record["NonOnlineBuyCommission"])
        )

    def process_data(self, data, bulk_operations):
        for record in data["Result"]:
            commission = {
                "OnlineSellCommission": float(record["OnlineSellCommission"]),
                "OnlineBuyCommission": float(record["OnlineBuyCommission"]),
                "NonOnlineSellCommission": float(record["NonOnlineSellCommission"]),
                "NonOnlineBuyCommission": float(record["NonOnlineBuyCommission"]),
                "TotalCommission": self.calculate_total_commission(record),
                "AccountCode": record["AccountCode"],
                "Date": record["Date"],
            }
            bulk_operations.append(UpdateOne(
                {"AccountCode": commission["AccountCode"], "Date": commission["Date"]},
                {"$set": commission},
                upsert= True
            ))
        
            

    def analysis_calculated_commissions(self, fromDate_str, toDate_str):

        fromDate = datetime.strptime(fromDate_str, "%Y-%m-%d")
        toDate = datetime.strptime(toDate_str, "%Y-%m-%d")
        self.db.analysis_update(fromDate, toDate)
        print("Analysis is successfully")
        time.sleep(2)
        
    def get_total_Commission_person(self, accountCode):
        self.db.find_person(accountCode)
        
    def remove_total_commissions_data(self):
        self.db.remove_collection("total commissions person")

    def update_allTransaction(self):
        last_date_obj = self.db.collection.find().sort("Date", -1).limit(1).next()
        last_date = last_date_obj['Date']
        bulk_operations = []
        # not self.db.check_database(current_date, "all Transactions")
        while not self.db.check_database(last_date, "all Transactions"):
            page = 1
            has_more_data = True
            bulk_operations = []

            while has_more_data:
                data = self.fetch_data(last_date.date(), last_date.date(), page)
                if not data["Result"]:
                    has_more_data = False
                else:
                    self.process_data(data, bulk_operations)
                    page += 1

            if bulk_operations:
                self.db.collection.bulk_write(bulk_operations) 
                print("bulk write")

            last_date += timedelta(days=1)

        print("All transactions collection is updated.")
        time.sleep(2)
            

if __name__ == "__main__":
    agent = CommissionAgent()
    # Uncomment the line below to fetch records
    # agent.get_records("2024-06-30", "2024-09-09")
    # agent.total_commission_customers("2024-04-18", "2024-09-14")
    agent.update_allTransaction()
    # agent.analysis_calculated_commissions("2024-04-18", "2024-09-14")
    # agent.get_total_Commission_person("31212-2002750")
    
    