import datetime
import json
import pandas as pd
from kafka import KafkaProducer
import time
import random

def crawl_price():
    btc_price = random.randint(100, 300)
    eth_price = random.randint(100, 300)
    bnb_price = random.randint(100, 300)
    sol_price = random.randint(100, 300)

    now = datetime.datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

    btc_info = {'Symbol':'BTC','Price':btc_price,'Updated_At':dt_string}
    eth_info = {'Symbol':'ETH','Price':eth_price,'Updated_At':dt_string}
    bnb_info = {'Symbol':'BNB','Price':bnb_price,'Updated_At':dt_string}
    sol_info = {'Symbol':'SOL','Price':sol_price,'Updated_At':dt_string}
    list_temp = [btc_info, eth_info, bnb_info, sol_info]

    return list_temp

def main():
    get_price = crawl_price()

    btc_df = pd.DataFrame(get_price).to_dict(orient='records')

    print(btc_df)

    producer = KafkaProducer( bootstrap_servers = ['localhost:9092'])
    print('------------------Writing data into Kafka Topic------------------')
    for i in btc_df:
        json_data = json.dumps(i).encode('utf-8')
        producer.send('json_data', json_data)
    print('------------------Finished------------------')

while True:
    main()
    time.sleep(5)
