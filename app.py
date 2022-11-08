# Importing libaries to request data from a API
from datetime import date, datetime
import requests
import os
import asyncio
from flask import Flask
from flask import  render_template
from datetime import datetime
from classes.cosmos_save import CosmosSave 
from classes.get_cripytos_data import GetCripytosData

from azure.cosmos.aio import CosmosClient as cosmos_client
from azure.cosmos import PartitionKey, exceptions

# Global variables
dict_time = {
                'start_time_1m':datetime.now(),
                'start_time_2h':datetime.now(),
                'start_time_1d':datetime.now(),
                'start_time_1w':datetime.now(),
                'time_now':datetime.now()
             }


usdt_pairs = [ 'BTCUSDT',  'ETHUSDT',  'XMRUSDT',  'XRPUSDT',   'BUSDUSDT',
               'DOGEUSDT', 'LTCUSDT',  'ETCUSDT',  'ZECUSDT',   'MANAUSDT' ]

# #usdt_pairs_to_trade = [ 'BTCUSDT',  'ETHUSDT',  'BNBUSDT',  'XRPUSDT',   'BUSDUSDT',
#                          'DOGEUSDT', 'ADAUSDT',  'SOLUSDT',  'MATICUSDT', 'DOTUSDT',
#                          'SHIBUSDT', 'TRXUSDT',  'AVAXUSDT', 'UNIUSDT',   'WBTCUSDT',
#                          'LTCUSDT',  'ATOMUSDT', 'LINKUSDT', 'ETCUSDT',   'ALGOUSDT',
#                          'FTTUSDT',  'XMRUSDT',  'XLMUSDT',  'NEARUSDT',  'BCHUSDT',
#                          'FILUSDT',  'QNTUSDT',  'VETUSDT',  'FLOWUSDT',  'CHZUSDT',
#                          'LUNCUSDT', 'APEUSDT',  'ICPUSDT',  'HBARUSDT',  'EGLDUSDT',
#                          'SANDUSDT', 'XTZUSDT',  'MANAUSDT', 'THETAUSDT', 'AAVEUSDT',
#                          'EOSUSDT',  'APTUSDT',  'AXSUSDT',  'MKRUSDT',   'ZECUSDT',
#                          'SNXUSDT',  'BTTCUSDT', 'IOTAUSDT', 'XECUSDT',   'FMTUSDT',]


# intervals = { '1m': 60, '2h': 7200, '1d': 24 * 3600, '1w': 7 * 24 * 3600 }
intervals = { '1m': 60 }#, '2h': 120 } #'1d': 60, '1w': 80 }
dict_cripyto = {}


def save_to_cosmos( to_save ):
    to_cosmos = []
    if len( to_save ) > 0:
        if len( to_save ) >= 10:
            for n in range( 10 ):
                to_cosmos.append( to_save.pop( 0 ) )
        else:
            to_cosmos.append( to_save.pop( 0 ) )
    else:
        print( 'No data to save!' )

    try:
        print( f'len to_save depois = {len( to_save)}' )
        CosmosSave.run_that( to_cosmos )
        return to_save
    except ConnectionError as error:
        print( f'Erro de conexao: {error}' )

def render_page( to_save ):
    return render_template( 
                            'countdown.html',
                           time = f'len to_save antes = {len( to_save )}'
                            )

def get_cripyto_data( dict_cripyto ):

    to_save = []
        
    while True:
        
        for api_interval in intervals.keys():
            interval = intervals[ api_interval ]
            if ( datetime.now() - dict_time[ 'start_time_' + api_interval ] ).seconds >= interval:
                dict_time[ 'start_time_' + api_interval ] = datetime.now()
                for pair in usdt_pairs:   
                    print( f'Getting data for {pair} evrey {interval} seconds' )
                                       
                    data = GetCripytosData( pair, api_interval )
                    cripytos_data = data.get_candles( data.pair, data.api_interval )
                    lista_to_cosmos, dict_cripyto = data.list_to_cosmos( cripytos_data, dict_cripyto )
                    to_save.extend( lista_to_cosmos )
                    print( f'len to_save antes = {len( to_save ) }' )
        
            else:
                to_save = save_to_cosmos( to_save )
                render_page( to_save )

# async def query():
#     async with cosmos_client( CosmosSave.endpoint, credential = CosmosSave.key ) as client:
#     # </create_cosmos_client>
#         try:
#             # create a database
#             database_obj = await CosmosSave.get_or_create_db( client, CosmosSave.database_name )
#             # create a container
#             container_obj = await CosmosSave.get_or_create_container( database_obj, CosmosSave.container_name)
           
#             query = "SELECT * FROM c WHERE c.pair = 'BTCUSDT' AND c.interval = '1m'"
#             response = await CosmosSave.query_items(container_obj, query)
#             print( type( response ) )
#             return response                
#         except exceptions.CosmosHttpResponseError as e:
#             print('\nrun_sample has caught an error. {0}'.format(e.message))


app = Flask( __name__ )

@app.route( '/' )
def app_run():
    get_cripyto_data( dict_cripyto )



            
if __name__ == '__main__':
    app.run()
    
                    
