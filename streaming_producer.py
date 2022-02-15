from alice_blue import *
from alice_blue import AliceBlue

from config import Credentials
import datetime
import time
from time import localtime, strftime
import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC_NAME_CONS = "TICK_DATA"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"
# import talib


UserName = Credentials.UserName.value
PassWord = Credentials.PassWord.value
SecretKey = Credentials.SecretKey.value
AppId = Credentials.AppId.value
f2 = 1997

SCRIPT_LIST = ["Nifty Bank"]

socket_opened = False

ohlc_dff = pd.DataFrame(
    columns=[
        "timestamp",
        "symbol",
        "day_high",
        "orb_low",
        "open",
        "close",
        "high",
        "low",
    ]
)


def event_handler_quote_update(message):
    ltp = message["ltp"]
    timestamp = datetime.datetime.fromtimestamp(message["exchange_time_stamp"])
    vol = message["volume"]
    instrumnet = message["instrument"].symbol
    exchange = message["instrument"].exchange
    high = message["high"]
    low = message["low"]

    currentTime = time.strftime("%Y-%m-%d %H:%M:%S", localtime())

    print(f"{instrumnet} {ltp} : {timestamp} : {vol}  ")
    # Creating tuple to store tick data
    all_data = (str(timestamp), instrumnet, high, low, ltp)
    # Create csv type from all_data to new_data
    new_data = str(all_data)[1:-1]
    # Create Kafka producer object
    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        value_serializer=lambda x: x.encode("utf-8"),
    )
    kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, new_data)


def open_callback():
    global socket_opened
    socket_opened = True
    print("Socket opened")


access_token = AliceBlue.login_and_get_access_token(
    username=UserName, password=PassWord, twoFA=f2, api_secret=SecretKey, app_id=AppId
)

alice = AliceBlue(
    username=UserName,
    password=PassWord,
    access_token=access_token,
    master_contracts_to_download=["NSE"],
)


alice.start_websocket(
    subscribe_callback=event_handler_quote_update,
    socket_open_callback=open_callback,
    run_in_background=True,
)

while socket_opened == False:
    pass
for script in SCRIPT_LIST:
    alice.subscribe(
        alice.get_instrument_by_symbol("NSE", script), LiveFeedType.MARKET_DATA
    )
