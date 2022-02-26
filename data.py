
# -- Load base packages
import pandas as pd
import numpy as np
import time
import json

# -- Cryptocurrency data and trading API
import ccxt

# -- Asyncronous data fetch
import asyncio
import ccxt.async_support as ccxt_async

# --------------------------------------------------------------------------- ASYNCRONOUS ORDERBOOK DATA -- # 
# --------------------------------------------------------------------------------------------------------- #

def order_book(symbol, exchanges, execution='async', stop=None, output_format='numpy', verbose=True):
    """
    Asyncronous OrderBook data fetcher. It will asyncronously catch innovations of transactions whenever they
    occur for every exchange is included in the list exchanges, and return the complete orederbook in a in a
    JSON format or DataFrame format with 'ask', 'ask_size', 'bid', 'bid_size'.

    Parameters
    ----------
    symbol: list
        with the names of instruments or markets to fetch the oredebook from.
    exchanges: list
        with the names of exchanges from where the data is intended to be fetched.
    
    execution: str
        'async': Asyncronous option to fetch several orderbooks in the same call. Depends on 
                 asyncio and ccxt.async_support
        'parallel': Run a parallel processing to deploy 1 instance for each symbol at each market. Depends
                 on multiprocessing (pending)
    stop: dict
        Criteria to stop the execution. Default behavior will be to stop after 1 minute of running.
        'min_count': int 
            Stops when all orderbooks have, at least, this number of registred timestamps.
        'target_timestamp': datetime
            Stops when its reached a specific timestamp.
        None: (default)
            Stop when 1 minute has elapsed
    
    output_format: str {'numpy', 'dataframe'} (default: 'numpy')
        Options for the output format, both is a dictionary with timestamp as key, values are:

        'numpy': numpy array with [0] Bid Volume, [1] Bid Price, [2] Ask Price, [3] Ask Volume
        'dataframe': pd.DataFrame with columns bid_volume, bid_price, ask_price, ask_volume
    
    verbose: bool
        To print in real time the fetched first ask and bid of every exchange.
    
    Returns
    -------
    r_data: dict
        A dictionary with the fetched data, with the following structure.
        r_data = {
            instrument: {
                exchange: {
                    
                    timestamp: {'ask': 1.4321, 'ask_size': 0.12,
                                'bid': 1.1234, 'bid_size': 0.21},
                    
                    timestamp: {'ask': 1.4321, 'ask_size': 0.12,
                                'bid': 1.1234, 'bid_size': 0.21}
            }
        }

    References
    ----------
    [1] https://github.com/ccxt/ccxt
    [2] https://docs.python.org/3/library/asyncio.html

    """
    
    # Store data for every exchange in the list
    r_data = {'bitfinex': {}, 'kraken': {}}

    # ----------------------------------------------------------------------------- ASYNCRONOUS REQUESTS -- # 
    async def async_client(exchange, symbol):

        # Await to be inside exchange limits of calls
        # await asyncio.sleep(exchange.rateLimit / 1000)

        # Initialize client inside the function, later will be closed, since this is runned asyncronuously
        # more than 1 client could be created and later closed.
        client = getattr(ccxt_async, exchange)({'enableRateLimit': True})
        await client.load_markets()

        # Check for symbol support on exchange
        if symbol not in client.symbols:
            raise Exception(exchange + ' does not support symbol ' + symbol)   

        # Initial time and counter
        time_1 = time.time()
        time_f = 0

        # Loop until stop criteria is reached
        while time_f <= 60:
            
            # Try and await for client response
            try:

                # Fetch, await and get datetime
                orderbook = await client.fetch_order_book(symbol)
                datetime = client.iso8601(client.milliseconds())

                # Verbosity
                if verbose:
                    print(datetime, client.id, symbol, orderbook['bids'][0], orderbook['asks'][0])

                # Unpack values
                ask_price, ask_size = np.array(list(zip(*orderbook['asks']))[0:2])
                bid_price, bid_size = np.array(list(zip(*orderbook['bids']))[0:2])
                spread = np.round(ask_price - bid_price, 4)
               
                # Final data format for the results
                r_data[client.id].update({datetime: pd.DataFrame({'ask_size': ask_size, 'ask': ask_price,
                                                                  'bid': bid_price, 'bid_size': bid_size,
                                                                  'spread': spread}) })
                # End time
                time_2 = time.time()
                time_f = round(time_2 - time_1, 4)

            # In case something bad happens with client
            except Exception as e:
                print(type(e).__name__, e.args, str(e))
                pass

        # Close client
        await client.close()

    # ------------------------------------------------------------------------------ MULTIPLE ORDERBOOKS -- #
    async def multi_orderbooks(exchanges, symbol):
        # A list of routines (and parameters) to run
        input_coroutines = [async_client(exchange, symbol) for exchange in exchanges]
        # wait for responses
        await asyncio.gather(*input_coroutines, return_exceptions=True)

    # Run event loop in async
    if execution=='async':
        asyncio.get_event_loop().run_until_complete(multi_orderbooks(exchanges, symbol))
    
    # Raise error in case of other value
    else:
        raise ValueError(execution, 'is not supported as a type of execution')

    # ----------------------------------------------------------------------------------- OUTPUT FORMAT -- #

    if output_format == 'numpy':
        print('this')

        return r_data
    elif output_format == 'dataframe':
        print('this')
        
        return r_data

# Small test
exchanges = ["bitfinex", "kraken"]
symbol = 'BTC/EUR'

# Massive download of OrderBook data
data = order_book(symbol=symbol, exchanges=exchanges, output='inplace', stop=None, verbose=True)
