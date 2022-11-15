# Importing libaries to request data from a API
import requests
import json
import time
import asyncio

from flask import Flask
from datetime import datetime
from azure.cosmos.aio import CosmosClient as cosmos_client
from azure.cosmos import PartitionKey, exceptions

# COSMOS 
class CosmosSave():

    # Account uri_and_key
    endpoint = "https://webapp-cosmos.documents.azure.com:443/"
    key = "p9Aouo7wUgqh0Jb6OihmQkwiGxAjddqRJCvU9pfDd2btJySzdrJHTUHAQGa6WPipLCTooNMbqq27l8P5Ix2ISQ=="

    # Define_database_and_container_name
    database_name = 'WebAppDB'
    container_name = 'cripytosdatabase'

    # Create_database_if_not_exists
    @classmethod
    async def get_or_create_db( cls, client, database_name ):
        try:
            database_obj = client.get_database_client( database_name )
            await database_obj.read()
            return database_obj
            
        except exceptions.CosmosResourceNotFoundError:
            print( "Creating database" )
            return await client.create_database( database_name )
        
    # Create a container
    # Using a good partition key improves the performance of database operations.
    # Create_container_if_not_exists
    @classmethod
    async def get_or_create_container( cls, database_obj, container_name ):

        try:        
            items_container = database_obj.get_container_client( container_name )
            await items_container.read()   
            return items_container

        except exceptions.CosmosResourceNotFoundError:

            print( "Creating container with pair as partition key" )
            return await database_obj.create_container(
                id = container_name,
                partition_key = PartitionKey( path = "/pair" ),
                offer_throughput = 900 )

        except exceptions.CosmosHttpResponseError:
            raise
        
    # Populate_container_items
    @classmethod
    async def populate_container_items( cls, container_obj, items_to_create ):
            
        # create_item
        for item in items_to_create:
            inserted_item = await container_obj.create_item( body = item )
            print( f"Inserted in cripytodatabase. Item Id: {inserted_item[ 'id' ]}" )


        
    # Query_items
    @classmethod
    async def query_items( cls, container_obj, query_text ):
        # enable_cross_partition_query should be set to True as the container is partitioned
        # In this case, we do have to await the asynchronous iterator object since logic
        # within the query_items() method makes network calls to verify the partition key
        # definition in the container
        # Query_items
        query_items_response = container_obj.query_items(
            query = query_text,
            enable_cross_partition_query = True
        )
        items = [ item async for item in query_items_response ]
        request_charge = container_obj.client_connection.last_response_headers[ 'x-ms-request-charge' ]
        print( '\nQuery returned {0} items. Operation consumed {1} request units'.format( len(items), request_charge ) )
        return items

     # Save data
    @classmethod
    async def save_data( cls, to_save ):
        # <create_cosmos_client>
        async with cosmos_client( CosmosSave.endpoint, credential = CosmosSave.key ) as client:
        # </create_cosmos_client>
            try:
                # create a database
                database_obj = await CosmosSave.get_or_create_db( client, CosmosSave.database_name )
                # create a container
                container_obj = await CosmosSave.get_or_create_container( database_obj, CosmosSave.container_name)
                # generate some family items to test create, read, delete operations
                
                print( 'Populating the cripyto items in container' )
                await CosmosSave.populate_container_items( container_obj, to_save )  
                             
            except exceptions.CosmosHttpResponseError as e:
                print('\nrun_sample has caught an error. {0}'.format(e.message))

    @classmethod       
    def get_or_create_eventloop( cls ):
        try:
            return asyncio.get_event_loop()
        except RuntimeError as ex:
            if "There is no current event loop in thread" in str(ex):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return asyncio.get_event_loop()
    
    @classmethod            
    def run_that( cls, to_save ): 
        loop = CosmosSave.get_or_create_eventloop()
        loop.run_until_complete( CosmosSave.save_data( to_save ) )


# GET CRIPYTO DATA

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
            

            
    def list_to_cosmos(self, cripytos_data, dict_cripyto ):
        
        list_to_save = []
                         
        if self.pair + self.api_interval in dict_cripyto.keys():

            dict_cripyto[ self.pair + self.api_interval ] += 1
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
    def is_on_line( cls ):
        try:
            
            requests.head( "http://www.google.com" )
            
            return True

        except requests.ConnectionError:

            return False



# Global variables
dict_time = {
                'start_time_1m':datetime.now(),
                'start_time_2h':datetime.now(),
                'start_time_1d':datetime.now(),
                'start_time_1w':datetime.now(),
            }

times = { '1m': 60, '2h': 7200 , '1d': 24 * 3600, '1w': 7 * 24 * 3600 }
# times = { '1m': 60, '2h': 90 , '1d': 120, '1w': 150 }
intervals = { '1m': None, '2h': None, '1d': None, '1w': None }

usdt_pairs = [ 'BTCUSDT',  'ETHUSDT',  'XMRUSDT',  'XRPUSDT',   'BUSDUSDT',
               'DOGEUSDT', 'LTCUSDT',  'ETCUSDT',  'ZECUSDT',   'MANAUSDT' ]


def save_to_file( to_save, pair, interval, mode ):

    with open( 'data/' +  str( pair ) + str ( interval ) + '.txt', mode ) as file:
        
        for line in to_save:
            file.write( str( line ).replace( "'", '"' ) + '\n' )
        
        print( f'{len( to_save )} inserted in the to_save.txt')

def overwrite_save_to_file( to_save, pair, interval ):
    save_to_file( to_save , pair, interval , 'w' )

def list_to_save( pair, interval ):
    print( ' lendo arquivo para gravacao: data/' + pair + interval + '.txt' )

    try:
        with open( 'data/' + pair + interval + '.txt', 'r' ) as file:
            save_list = file.read()
            return save_list.split( '\n' )

    except Exception:
        raise

        
def save_to_cosmos( to_save, pair, interval ):

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
        overwrite_save_to_file( to_save, pair, interval )


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
                        cripytos_data = data.get_candles( data.pair, data.api_interval )
                        lista_to_cosmos, dict_cripyto =  data.list_to_cosmos( cripytos_data, dict_cripyto )
                        to_save.extend( lista_to_cosmos )

                        save_to_file( to_save, data.pair, data.api_interval, 'a' )
                        to_save.clear()    

                    intervals[ api_interval ] = times[ api_interval ]

                else: 
                    interval = intervals[ api_interval ]
                    if ( datetime.now() - dict_time[ 'start_time_' + api_interval ] ).seconds >= interval:
                        
                        dict_time[ 'start_time_' + api_interval ] = datetime.now()
                        
                        for pair in usdt_pairs:
                         
                            print( f'Getting data for {pair} evrey {interval} seconds' )

                            data = GetCripytosData( pair, api_interval )
                            cripytos_data = data.get_candles( data.pair, data.api_interval )
                            lista_to_cosmos, dict_cripyto =  data.list_to_cosmos( cripytos_data, dict_cripyto )
                            to_save.extend( lista_to_cosmos )
                            print( f'len to_save antes = { len( to_save ) }' )

                            save_to_file( to_save, data.pair, data.api_interval, 'a' )
                            to_save.clear()
                
                    else:
                        for pair in usdt_pairs:
                            print( pair, data.api_interval )
                            to_cosmos = list_to_save( pair, data.api_interval )
                            print( f'len to_save before save in comos = { len( to_cosmos ) }' )
                            save_to_cosmos( to_cosmos, pair, data.api_interval )
                        
        else:

            print( 'No Internet Connection' )
            print( 'Tring to reconnect in 10 seconds' )
            time.sleep( 10 )


app = Flask( __name__ )

@app.route( '/' )
def get_data():
    get_cripyto_data( usdt_pairs, dict_time, times, intervals )
            



if __name__ == '__main__':
    app.run( debug = True, port = 5000) 
    # get_cripyto_data( usdt_pairs, dict_time, times, intervals )
   

   
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
                    
