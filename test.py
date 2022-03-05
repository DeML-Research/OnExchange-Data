
# -- Load base packages
import pandas as pd
import numpy as np
import data as dt

# (pending) Get the list of exchanges with the symbol

# Get public trades from the list of exchanges
exchanges = ['binance']
symbol = 'BTC/USDT'

# Fetch realtime orderbook data until timer is out (60 secs is default)
orderbooks = dt.async_data(symbol=symbol, exchanges=exchanges, output_format='inplace', timestamp_format='timestamp',
                           data_type='orderbooks', file_route='files/orderbooks', stop_criteria=None,
                           elapsed_secs=10, verbose=2)

# Fetch realtime orderbook data until timer is out (60 secs is default)
publictrades = dt.async_data(symbol=symbol, exchanges=exchanges, output_format='inplace', timestamp_format='timestamp',
                           data_type='publictrades', file_route='files/publictrades', stop_criteria=None,
                           elapsed_secs=10, verbose=2)
