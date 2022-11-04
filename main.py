
# Importing libaries to request data from a API
from datetime import date, datetime
import requests
import os

# Importing libraries to pause the kernel
import time


from classes import cosmos_save
from classes.get_cripytos_data import GetCripytosData

# Global variables
dict_time = {
                'start_time_1m':datetime.now(),
                'start_time_2h':datetime.now(),
                'start_time_1d':datetime.now(),
                'start_time_1w':datetime.now(),
                'time_now':datetime.now()
             }
dict_cripyto = {}

def list_to_csv(candles_list, pair, interval):
    new_line = ''

    if os.path.exists( 'data/' + pair + '_' + interval + '.csv' ):
        with open( 'data/' + pair + '_' + interval + '.csv', 'a' ) as file:
            new_line = str( datetime.now() ) + ';'
            line = candles_list[ len( candles_list ) - 1 ]
            for data in line:
                if line.index( data ) < len( line ) - 1:
                    new_line += str( data ) +';'
                else:
                    new_line += str( data ) +'\n'
            file.write( new_line )
            new_line = ''

    else:
        with open( 'data/' + pair + '_' + interval + '.csv', 'a' ) as file:
            for line in candles_list:
                new_line = str( datetime.now() ) + ';'
                for data in line:
                    if line.index( data ) < len( line ) - 1:
                        new_line += str( data ) +';'
                    else:
                        new_line += str( data ) +'\n'
                file.write( new_line )
                new_line = ''

def get_candles( pair, api_interval ):  
         
    now = datetime.now()

    try:
        response = requests.get( "https://api.binance.com/api/v3/klines?symbol=" + pair + "&interval=" + api_interval )

        if response.status_code == 200:
            print( 'Tempo entre requisicao e resposta na Bincace = {} ms \n'.format( int( (datetime.now() - now).microseconds / 1000 ) ) )
            return response.json(), response.status_code
        else:
            print( 'Tempo entre requisicao e resposta na Bincace = {} ms \n'.format( int( (datetime.now() - now).microseconds / 1000 ) ) )
            print( 'Erro na requisicao. Status code {}'.format( response.status_code ) )
            return None, response.status_code

    except Exception as ex:
        print( 'Error de conexao com o servidor: {}'.format( ex.message) )

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
        cosmos_save.run_that( to_cosmos )
        return to_save
    except ConnectionError as error:
        print( f'Erro de conexao: {error}' )
    


usdt_pairs = [ 'LTCUSDT',
              'BTCUSDT',
              'XMRUSDT',
              'MANAUSDT',
              'ETHUSDT',
              'XRPUSDT',
              'DOGEUSDT',
              'ETCUSDT',
              'AXSUSDT']
# intervals = { '1m': 60, '2h': 7200, '1d': 24 * 3600, '1w': 7 * 24 * 3600 }
intervals = { '1m': 60, '2h': 120 }#, '1d': 15, '1w': 20 }
dict_cripyto = {}

if __name__ == '__main__':
  
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
                    cripytos_data, dict_cripyto = data.upadate_cripytos_data_list( cripytos_data, data.pair, data.api_interval, dict_cripyto ) 
                    to_save.extend( data.list_to_cosmos( cripytos_data )[:5] )
                    print( len( to_save ))

                    
            else:
                to_save.extend( save_to_cosmos( to_save ) )

        