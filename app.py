# Importing libaries to request data from a API
import time
import asyncio

from datetime import date, datetime
from flask import Flask
from flask import  render_template
from datetime import datetime
from classes.cosmos_save import CosmosSave 
from classes.get_cripytos_data import GetCripytosData

from azure.cosmos.aio import CosmosClient as cosmos_client
from azure.cosmos import PartitionKey, exceptions

# Global variables
# dict_time = {
#                 'start_time_1m':datetime.now(),
#                 'start_time_2h':datetime.now(),
#                 'start_time_1d':datetime.now(),
#                 'start_time_1w':datetime.now(),
#             }

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


times = { '1m': 60, '2h': 7200, '1d': 24 * 3600, '1w': 7 * 24 * 3600 }

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
        print( f'len to_save depois = { len( to_save)}' )
        CosmosSave.run_that( to_cosmos )
        return to_save
    except ConnectionError as error:
        print( f'Erro de conexao: {error}' )

def render_page( to_save ):
    return render_template( 
                            'countdown.html',
                           time = f'len to_save antes = {len( to_save )}'
                            )



def get_cripyto_data( usdt_pairs ):

    dict_cripyto = {}
    dict_time = {}
    to_save = []

    # Create or get the Databse and Container
    db_exists = asyncio.run( CosmosSave.get_or_create_db_and_container() )
    print( db_exists )

    if db_exists == 'db exists':
        print( 'Entrou aqui db exists')
        dict_time =  GetCripytosData.get_time( usdt_pairs )
        intervals = { '1m': 60, '2h': 7200, '1d': 24 * 3600, '1w': 7 * 24 * 3600 }

        for interval in intervals.keys():
            for pair in usdt_pairs: 
                dict_cripyto[ pair + interval ] = 0
                
                # { 'BTCUSDT1m':1,  'ETHUSDT1m':1,  'XMRUSDT1m':1,  'XRPUSDT1m':1,   'BUSDUSDT1m':1,
                #          'DOGEUSDT1m':1, 'LTCUSDT1m':1,  'ETCUSDT1m':1,  'ZECUSDT1m':1,   'MANAUSDT1m':1,
                #          'BTCUSDT2h':1,  'ETHUSDT2h':1,  'XMRUSDT2h':1,  'XRPUSDT2h':1,   'BUSDUSDT2h':1,
                #          'DOGEUSDT2h':1, 'LTCUSDT2h':1,  'ETCUSDT2h':1,  'ZECUSDT2h':1,   'MANAUSDT2h':1,
                #          'BTCUSDT1d':1,  'ETHUSDT1d':1,  'XMRUSDT1d':1,  'XRPUSDT1d':1,   'BUSDUSDT1d':1,
                #          'DOGEUSDT1d':1, 'LTCUSDT1d':1,  'ETCUSDT1d':1,  'ZECUSDT1d':1,   'MANAUSDT1d':1,
                #          'BTCUSDT1w':1,  'ETHUSDT1w':1,  'XMRUSDT1w':1,  'XRPUSDT1w':1,   'BUSDUSDT1w':1,
                #          'DOGEUSDT1w':1, 'LTCUSDT1w':1,  'ETCUSDT1w':1,  'ZECUSDT1w':1,   'MANAUSDT1w':1
                # }

    
    else:
        intervals = { '1m': None, '2h': None, '1d': None, '1w': None }
        dict_time = {}   
        
    while True:
        
        if GetCripytosData.is_on_line():
            
            for api_interval in intervals.keys():                   

                if intervals[ api_interval ] == None:
                    for pair in usdt_pairs:
                        data = GetCripytosData( pair, api_interval )
                        print( f'Getting data for {data.pair} evrey {data.api_interval} seconds' )
                        cripytos_data = data.get_candles( data.pair, data.api_interval )
                        lista_to_cosmos, dict_cripyto =  data.list_to_cosmos( cripytos_data, dict_cripyto, to_save )
                        to_save.extend( lista_to_cosmos )
                        
                    intervals[ api_interval ] = times[ api_interval ]

                else: 
                    interval = intervals[ api_interval ]
                    if ( datetime.now() - dict_time[ 'start_time_' + api_interval ] ).seconds >= interval:

                        for pair in usdt_pairs:
                            dict_time[ 'start_time_' + api_interval ] = datetime.now()

                            print( f'Getting data for {pair} evrey {interval} seconds' )

                            data = GetCripytosData( pair, api_interval )
                            cripytos_data = data.get_candles( data.pair, data.api_interval )
                            lista_to_cosmos, dict_cripyto =  data.list_to_cosmos( cripytos_data, dict_cripyto, to_save )
                            to_save.extend( lista_to_cosmos )
                            print( f'len to_save antes = { len( to_save ) }' )
                
                    else:
                        to_save = save_to_cosmos( to_save )
                        render_page( to_save )
        else:

            print( 'No Internet Connection' )
            print( 'Tring to reconnect in 10 seconds' )
            time.sleep( 10 )
            

app = Flask( __name__ )

# @app.route( '/' )
# def app_run():
#     get_cripyto_data( dict_cripyto, dict_time )

            
if __name__ == '__main__':
    get_cripyto_data( usdt_pairs )
    # app.run()

   
    
                    
