import requests
from datetime import datetime

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


    def upadate_cripytos_data_list(self, cripytos_data, pair, api_interval, dict_cripyto ):
        
        for data in cripytos_data:
            
            if pair in dict_cripyto.keys():
                dict_cripyto[ pair ] += 1
            else:
                dict_cripyto[ pair ] = 1

            print( dict_cripyto[ pair ] )

            data.insert( 0, str( datetime.now() ) )
            data.insert( 1, pair )
            data.insert( 2, api_interval )
            data.insert( 3, dict_cripyto[ pair ] )
            
        return cripytos_data, dict_cripyto

    def list_to_cosmos(self, cripytos_data ):
        list_to_save = []
        for data in cripytos_data:
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
        return list_to_save