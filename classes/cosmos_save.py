from azure.cosmos.aio import CosmosClient as cosmos_client
from azure.cosmos import PartitionKey, exceptions
import asyncio


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


    # Read_items
    @classmethod
    async def read_items( cls, container_obj, items_to_read ):
        # Read items (key value lookups by partition key and id, aka point reads)
        # Read_item
        for item in items_to_read:
            item_response = await container_obj.read_item( item = item[ 'id' ], partition_key = item[ 'pair' ] )
            request_charge = container_obj.client_connection.last_response_headers[ 'x-ms-request-charge' ]
            print( 'Read item with id {0}. Operation consumed {1} request units'.format(item_response['id'], (request_charge)))
            
        
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
        request_charge = container_obj.client_connection.last_response_headers[ 'x-ms-request-charge' ]
        items = [ item async for item in query_items_response ]
        print( 'Query returned {0} items. Operation consumed {1} request units'.format( len(items), request_charge ) )
        return items


    # Run_sample
    @classmethod
    async def run_sample( cls, to_save ):
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
                # read the just populated items using their id and partition key
                # await read_items( container_obj, family_items_to_create )
                # Query these items using the SQL query syntax. 
                # Specifying the partition key value in the query allows Cosmos DB to retrieve data only from the relevant partitions, which improves performance
                #query = "SELECT VALUE MAX(c.registry) FROM c WHERE c.pair = 'BTCUSDT' AND c.interval = '1m'"
                # query = "SELECT * FROM c WHERE c.pair = 'BTCUSDT' AND c.interval = '1m'"
                # response = await CosmosSave.query_items(container_obj, query)
                # print( response[0] )                
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
        loop.run_until_complete(CosmosSave.run_sample( to_save ))

    