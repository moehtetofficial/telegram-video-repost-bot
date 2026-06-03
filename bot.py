import time
import pandas as pd
import numpy as np
import os
from binance.client import Client
from binance.enums import *

# === API Keys ===
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
client = Client(API_KEY, API_SECRET)
client.FUTURES_URL = 'https://fapi.binance.com/fapi'

# === Settings ===
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', '1000PEPEUSDT']
INTERVAL = '15m'
USD_PER_TRADE = 10
TRADE_COOLDOWN = 60 * 15  # 15 minutes
last_trade_times = {symbol: 0 for symbol in SYMBOLS}

# === Functions ===

def get_klines(symbol, interval, limit=100):
    data = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    df['close'] = df['close'].astype(float)
    return df

def bollinger_signal(df):
    df['MA20'] = df['close'].rolling(window=20).mean()
    df['STD'] = df['close'].rolling(window=20).std()
    df['Upper'] = df['MA20'] + (2 * df['STD'])
    df['Lower'] = df['MA20'] - (2 * df['STD'])

    price = df['close'].iloc[-1]
    upper = df['Upper'].iloc[-1]
    lower = df['Lower'].iloc[-1]

    if price <= lower:
        return 'BUY'
    elif price >= upper:
        return 'SELL'
    else:
        return 'HOLD'

def get_price(symbol):
    ticker = client.futures_symbol_ticker(symbol=symbol)
    return float(ticker['price'])

def place_market_order(symbol, side, quantity):
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity=quantity
        )
        print(f"{side} order placed for {symbol} (Qty: {quantity}) | Order ID: {order['orderId']}")
    except Exception as e:
        print(f"Error placing {side} order for {symbol}: {e}")

# === Main Loop ===
while True:
    for symbol in SYMBOLS:
        try:
            df = get_klines(symbol, INTERVAL)
            signal = bollinger_signal(df)
            print(f"[{symbol}] Signal: {signal}")

            current_time = time.time()

            if signal in ['BUY', 'SELL'] and (current_time - last_trade_times[symbol]) > TRADE_COOLDOWN:
                price = get_price(symbol)
                quantity = round(USD_PER_TRADE / price, 6)  # rounding to 6 decimals for precision

                side = SIDE_BUY if signal == 'BUY' else SIDE_SELL
                place_market_order(symbol, side, quantity)
                last_trade_times[symbol] = current_time

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    time.sleep(60)
