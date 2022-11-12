import requests
from datetime import datetime

import asyncio

from azure.cosmos.aio import CosmosClient as cosmos_client
from azure.cosmos import PartitionKey, exceptions
from classes.cosmos_save import CosmosSave

# response in the each candle:
# list of OHLCV values (Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, Number of trades, Taker buy base asset volume, Taker buy quote asset volume, Ignore)

class GetCripytosData():

    def __init__( self, pair, api_interval ):
        self.pair = pair
        self.api_interval = api_interval
        

    def get_candles(self, pair, api_interval ):  
        """
        Documentar
        
        """
        now = datetime.now()
        try:

            response = requests.get( "https://api.binance.com/api/v3/klines?symbol=" + pair + "&interval=" + api_interval )

            if response.status_code == 200:
                print( 'Status code = {}'.format( response.status_code ) )
                return response.json()

            else:
                print( 'Tempo entre requisicao e resposta na Bincace = {} ms \n'.format( int( (datetime.now() - now).microseconds / 1000 ) ) )
                print( 'Erro na requisicao. Status code {}'.format( response.status_code ) )
                return None, response.status_code

        except Exception as ex:
            print( 'Error de conexao com o servidor: {}'.format( ex.message) )
            

            
    def list_to_cosmos(self, cripytos_data, dict_cripyto, to_save, query_text = None ):
        
        list_to_save = []
        
        print( f'Saving data for {self.pair} {self.api_interval}' )
        query_text = f"SELECT * FROM c WHERE c.pair = '{self.pair}' AND c.interval = '{self.api_interval}'"
        items = asyncio.run( GetCripytosData.query_registry( query_text ) )   

        if len( items ) > 0:
            
            print(  f'{ len( items ) } registry found for {self.pair} {self.api_interval}\n' )
            dict_cripyto[ self.pair + self.api_interval ] = items[ len( items ) - 1 ][ "registry" ] + 1

          
        if self.pair + self.api_interval in dict_cripyto.keys():
            
            data = cripytos_data[ len( cripytos_data ) - 1 ]
            data.insert( 0, str( datetime.now() ) )
            data.insert( 1, self.pair )
            data.insert( 2, self.api_interval )
            data.insert( 3, dict_cripyto[ self.pair + self.api_interval ] )

            print( f'Building registry = {dict_cripyto[ self.pair + self.api_interval ]} para {self.pair} {self.api_interval}' )

            to_save = {
                    "id": str( data[ 1 ] ) + str( data[ 2 ] ) + "-" +str( data[ 0 ] ),
                    "datetime":data[ 0 ],
                    "pair":data[ 1 ],
                    "interval":data[ 2 ],
                    "registry":data[ 3 ],
                    "Open_time":data[ 4 ],
                    "Open":data[ 5 ],
                    "High":data[ 6 ],
                    "Low":data[ 7 ],
                    "Close":data[ 8 ],
                    "Volume":data[ 9 ],
                    "Close_time":data[ 10 ],
                    "Quote_asset_volume":data[ 11 ],
                    "Number_of_trades":data[ 12 ],
                    "Taker_buy_base_asset_volume":data[ 13 ],
                    "Taker_buy_quote_asset_volume":data[ 14 ],
                    "Ignore":data[ 15 ]                
                }
            list_to_save.append( to_save )
            
        else:
            
            dict_cripyto[ self.pair + self.api_interval  ] = 1
            

            for data in cripytos_data:
                print( f'Building registry = {dict_cripyto[ self.pair + self.api_interval ]} para {self.pair} {self.api_interval}' )
                                               
                data.insert( 0, str( datetime.now() ) )
                data.insert( 1, self.pair )
                data.insert( 2, self.api_interval )
                data.insert( 3, dict_cripyto[ self.pair + self.api_interval ] )
                
                to_save = {
                    "id": str( data[ 1 ] ) + str( data[ 2 ] ) + "-" +str( data[ 0 ] ),
                    "datetime":data[ 0 ],
                    "pair":data[ 1 ],
                    "interval":data[ 2 ],
                    "registry":data[ 3 ],
                    "Open_time":data[ 4 ],
                    "Open":data[ 5 ],
                    "High":data[ 6 ],
                    "Low":data[ 7 ],
                    "Close":data[ 8 ],
                    "Volume":data[ 9 ],
                    "Close_time":data[ 10 ],
                    "Quote_asset_volume":data[ 11 ],
                    "Number_of_trades":data[ 12 ],
                    "Taker_buy_base_asset_volume":data[ 13 ],
                    "Taker_buy_quote_asset_volume":data[ 14 ],
                    "Ignore":data[ 15 ]                
                }
                list_to_save.append( to_save )

                if dict_cripyto[ self.pair + self.api_interval  ] < len( cripytos_data ):
                    dict_cripyto[ self.pair + self.api_interval  ] += 1

        return list_to_save, dict_cripyto

    @classmethod
    async def query_registry( cls, query ):
        # create_cosmos_client
        async with cosmos_client( CosmosSave.endpoint, credential = CosmosSave.key ) as client:
        
            try:
                # create a database
                database_obj = await CosmosSave.get_or_create_db( client, CosmosSave.database_name )
                # create a container
                container_obj = await CosmosSave.get_or_create_container( database_obj, CosmosSave.container_name)
                          
                response = await CosmosSave.query_items( container_obj, query )
                return response 

            except exceptions.CosmosHttpResponseError as e:
                print('\nrun_sample has caught an error. {0}'.format(e.message))


    @classmethod
    def get_time( cls, usdt_pairs ):

        intervals = [ '1m', '2h', '1d', '1w' ]
        dict_t = {}
        try:
            for interval in intervals:
                # for pair in usdt_pairs:
                query_text = f"SELECT * FROM c WHERE c.pair = 'BTCUSDT' AND c.interval = '{interval}'"
                items = asyncio.run( GetCripytosData.query_registry( query_text ) )
                if len( items ) > 0:
                    print( f'{items[ len( items ) - 1 ][ "registry" ]} found fot {pair} and {interval}' )
                    dict_t[ 'start_time_' + interval ] = datetime.strptime( items[ len( items ) - 1 ][ "datetime" ], '%Y-%m-%d %H:%M:%S.%f' )
            
            print( f'len dict_t = {len( dict_t )}' )
            return dict_t

        except Exception as e:
            print('\nrun_sample has caught an error. {0}'.format(e))
    
    @classmethod
    def is_on_line( cls ):
        try:
            
            requests.head( "http://www.google.com" )
            
            return True

        except requests.ConnectionError:

            return False