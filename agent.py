from database import AgentDataBase
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
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()  # Ensure we notice bad responses
        return response.json()

    def process_data(self, data):
        commissions = []
        for record in data["Result"]:
            commission = {
                # "OnlineSellCommission": float(record["OnlineSellCommission"]),
                # "OnlineBuyCommission": float(record["OnlineBuyCommission"]),
                # "NonOnlineSellCommission": float(record["NonOnlineSellCommission"]),
                # "NonOnlineBuyCommission": float(record["NonOnlineBuyCommission"]),
                "TotalCommission": float(record["OnlineSellCommission"]) +
                                   float(record["OnlineBuyCommission"]) +
                                   float(record["NonOnlineSellCommission"]) +
                                   float(record["NonOnlineBuyCommission"]),
                "AccountCode": record["AccountCode"],
                "LastDateIdentifier": False
            }
            commissions.append(commission)
        return commissions

    def get_records(self, fromDate_str, toDate_str):
        fromDate = datetime.strptime(fromDate_str, "%Y-%m-%d")
        toDate = datetime.strptime(toDate_str, "%Y-%m-%d")

        while fromDate.date() <= toDate.date():
            page = 1
            has_more_data = True
            
            while has_more_data:
                data = self.fetch_data(fromDate.date(), fromDate.date(), page)
                if not data["Result"]:
                    has_more_data = False
                else:
                    commissions = self.process_data(data)
                    self.db.upsert(commissions)
                    page += 1

            fromDate += timedelta(days=1)
            # time.sleep(1)  # To handle potential rate limiting

    def total_commission_customers(self, fromDate_str, toDate_str):
        fromDate = datetime.strptime(fromDate_str, "%Y-%m-%d")
        toDate = datetime.strptime(toDate_str, "%Y-%m-%d")

        while fromDate.date() <= toDate.date():
            page = 1
            has_more_data = True

            while has_more_data:
                data = self.fetch_data(fromDate.date(), fromDate.date(), page)
                if not data["Result"]:
                    has_more_data = False
                else:
                    commissions = self.process_data(data)
                    self.db.update(commissions)
                    page += 1

            fromDate += timedelta(days=1)
            # time.sleep(1)  # To handle potential rate limiting

    # def total_commission_customer_from_to(self, fromDate_str, toDate_str):
        

if __name__ == "__main__":
    agent = CommissionAgent()
    # Uncomment the line below to fetch records
    # agent.get_records("2024-06-30", "2024-09-09")
    agent.total_commission_customers("2024-04-18", "2024-09-14")
