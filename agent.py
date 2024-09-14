from database import AgentDataBase
import requests
from datetime import datetime, timedelta
import time

class CommissionAgent:

    def __init__(self):
        # create database
        self.db = AgentDataBase()

    def get_records(self, fromDate_str, toDate_str, page=1):
        fromDate = datetime.strptime(fromDate_str, "%Y-%m-%d")
        toDate = datetime.strptime(toDate_str, "%Y-%m-%d")

        # set URL and specification
        url = f"https://tbsapi.etbrokerage.ir/api/CustomerClub/GetCustomerTotalBrokerCommission?fromDate={fromDate_str}&toDate={fromDate_str}&page={page}&pageSize=100"
        username = "Tahlil.taban"
        password = "Et@@2025MR"

        # set THE header
        headers = {
            "UserName": username,
            "Password": password
        }

        response = requests.get(url, headers=headers)
        data = response.json()
        while fromDate.date() <= toDate.date():
            while len(data["Result"]) != 0:
                for record in data["Result"]:
                    commission = {
                        "OnlineSellCommission": float(record["OnlineSellCommission"]),
                        "OnlineBuyCommission": float(record["OnlineBuyCommission"]),
                        "NonOnlineSellCommission": float(record["NonOnlineSellCommission"]),
                        "NonOnlineBuyCommission": float(record["NonOnlineBuyCommission"]),
                        "TotalCommission": float(record["OnlineSellCommission"])
                                           + float(record["OnlineBuyCommission"])
                                           + float(record["NonOnlineSellCommission"])
                                           + float(record["NonOnlineBuyCommission"]),
                        "AccountCode": record["AccountCode"],
                        "Date": record["Date"]
                    }
                    print(fromDate.date())
                    self.db.upsert(commission)

                page += 1
                url = f"https://tbsapi.etbrokerage.ir/api/CustomerClub/GetCustomerTotalBrokerCommission?fromDate={str(fromDate.date())}&toDate={str(fromDate.date())}&page={page}&pageSize=100"
                response = requests.get(url, headers=headers)
                data = response.json()
                
            page = 1
            fromDate += timedelta(days=1)
            url = f"https://tbsapi.etbrokerage.ir/api/CustomerClub/GetCustomerTotalBrokerCommission?fromDate={str(fromDate.date())}&toDate={str(fromDate.date())}&page={page}&pageSize=100"
            response = requests.get(url, headers=headers)
            data = response.json()
            print(fromDate.date())

    def total_commission_customer(fromDate_str, toDate_str, page=1):
        fromDate = datetime.strptime(fromDate_str, "%Y-%m-%d")
        toDate = datetime.strptime(toDate_str, "%Y-%m-%d")
        
        # set URL and specification
        url = f"https://tbsapi.etbrokerage.ir/api/CustomerClub/GetCustomerTotalBrokerCommission?fromDate={fromDate_str}&toDate={fromDate_str}&page={page}&pageSize=100"
        username = "Tahlil.taban"
        password = "Et@@2025MR"

        # set THE header
        headers = {
            "UserName": username,
            "Password": password
        }

        response = requests.get(url, headers=headers)
        data = response.json()
        
        while fromDate.date() <= toDate.date():
            while len(data["Result"]) != 0:
                for record in data["Result"]:
                    commission = {
                        "TotalCommission": float(record["OnlineSellCommission"])
                                           + float(record["OnlineBuyCommission"])
                                           + float(record["NonOnlineSellCommission"])
                                           + float(record["NonOnlineBuyCommission"]),
                        "AccountCode": record["AccountCode"],
                    }
                    
                    self.db.update(commission)

                page += 1
                url = f"https://tbsapi.etbrokerage.ir/api/CustomerClub/GetCustomerTotalBrokerCommission?fromDate={str(fromDate.date())}&toDate={str(fromDate.date())}&page={page}&pageSize=100"
                response = requests.get(url, headers=headers)
                data = response.json()
                
            page = 1
            fromDate += timedelta(days=1)
            url = f"https://tbsapi.etbrokerage.ir/api/CustomerClub/GetCustomerTotalBrokerCommission?fromDate={str(fromDate.date())}&toDate={str(fromDate.date())}&page={page}&pageSize=100"
            response = requests.get(url, headers=headers)
            data = response.json()
            print(fromDate.date())

        
        
        
        
if __name__ == "__main__":
    agent = CommissionAgent()
    # agent.get_records("2024-06-30", "2024-09-09")
    agent.total_commission_customer("2024-09-09", "2024-09-14")