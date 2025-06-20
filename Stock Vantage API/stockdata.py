import requests
import json

#API SETUP

api_key="7ZD17KJIBJ772FX0"
symbol = "AAPL" 
url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}"

response =requests.get(url)
stock_data = response.json()

with open("C:/Downloads/stock_data.json","w") as file:
    json.dump(stock_data,file)