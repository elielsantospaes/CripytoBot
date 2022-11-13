# Importing libaries to request data from a API
import json
import time

from datetime import datetime
from classes.cosmos_save import CosmosSave
from classes.get_cripytos_data import GetCripytosData

# Global variables
dict_time = {
                'start_time_1m':datetime.now(),
                'start_time_2h':datetime.now(),
                'start_time_1d':datetime.now(),
                'start_time_1w':datetime.now(),
            }

times = { '1m': 10, '2h': 40 , '1d': 24 * 3600, '1w': 7 * 24 * 3600 }
intervals = { '1m': None, '2h': None, '1d': None, '1w': None }

usdt_pairs = [ 'BTCUSDT',  'ETHUSDT']#,  'XMRUSDT',  'XRPUSDT',   'BUSDUSDT',
            #    'DOGEUSDT', 'LTCUSDT',  'ETCUSDT',  'ZECUSDT',   'MANAUSDT' ]


def save_to_file( to_save, mode ):

    with open( 'to_save.txt', mode ) as file:
        
        for line in to_save:
            file.write( str( line ).replace( "'", '"' ) + '\n' )
        
        print( f'{len( to_save )} inserted in the to_save.txt')

def overwrite_save_to_file( to_save ):
    save_to_file( to_save, 'w' )

def list_to_save():

    with open( 'to_save.txt', 'r' ) as file:
        save_list = file.read()
        return save_list.split( '\n' )


        
def save_to_cosmos( to_save ):

    to_cosmos = []

    if len( to_save ) > 0:
        if len( to_save ) >= 10:
            for _ in range( 10 ):
                if to_save[ 0 ] != '':
                    to_cosmos.append( json.loads( to_save.pop( 0 ) ) )
                else:
                     to_save.pop( 0 )
        else:
            if to_save[ 0 ] != '':
                to_cosmos.append( json.loads( to_save.pop( 0 ) ) )
            else:
                to_save.pop( 0 )
            
    else:
        print( 'No data to save!' )

    try:
        print( f'len to_save after save in comos = { len( to_save ) }' )
        CosmosSave.run_that( to_cosmos )
        overwrite_save_to_file( to_save )


    except ConnectionError as error:
        print( f'Erro de conexao: {error}' )


def get_cripyto_data( usdt_pairs, dict_time, times, intervals ):

    to_save = []
    dict_cripyto = {}
    # Create or get the Databse and Container
    while True:
        
        if GetCripytosData.is_on_line():
            
            for api_interval in intervals.keys():                   

                if intervals[ api_interval ] == None:
                    for pair in usdt_pairs:
                        data = GetCripytosData( pair, api_interval )
                        print( f'Getting data for {data.pair} evrey {data.api_interval} seconds' )
                        cripytos_data = data.get_candles( data.pair, data.api_interval )[0:5]
                        lista_to_cosmos, dict_cripyto =  data.list_to_cosmos( cripytos_data, dict_cripyto )
                        to_save.extend( lista_to_cosmos )

                    save_to_file( to_save, 'a' )
                    to_save.clear()    
                    intervals[ api_interval ] = times[ api_interval ]

                else: 
                    interval = intervals[ api_interval ]
                    if ( datetime.now() - dict_time[ 'start_time_' + api_interval ] ).seconds >= interval:
                        
                        dict_time[ 'start_time_' + api_interval ] = datetime.now()
                        
                        for pair in usdt_pairs:
                         
                            print( f'Getting data for {pair} evrey {interval} seconds' )

                            data = GetCripytosData( pair, api_interval )
                            cripytos_data = data.get_candles( data.pair, data.api_interval )[0:5]
                            lista_to_cosmos, dict_cripyto =  data.list_to_cosmos( cripytos_data, dict_cripyto )
                            to_save.extend( lista_to_cosmos )
                            print( f'len to_save antes = { len( to_save ) }' )

                        save_to_file( to_save, 'a' )
                        to_save.clear()
                
                    else:
                        to_cosmos = list_to_save()
                        print( f'len to_save before save in comos = { len( to_save ) }' )
                        save_to_cosmos( to_cosmos )
                        
        else:

            print( 'No Internet Connection' )
            print( 'Tring to reconnect in 10 seconds' )
            time.sleep( 10 )
            

           
if __name__ == '__main__':
    get_cripyto_data( usdt_pairs, dict_time, times, intervals )
   

   
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
                    
