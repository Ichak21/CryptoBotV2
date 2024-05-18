from pathlib import Path
import itertools
import asyncio
import json
import pytz
import os

from datetime import datetime, timedelta
import ccxt.async_support as ccxt
from tqdm.auto import tqdm
import pandas as pd

from .LoggerManager import LoggerManager



MODULE_NAME = "MDM - Market Data Manager"

PATH_SETTINGS = "./EDM_Settings/Setting.json"
PATH_DOWNLOADS = "../Downloads/"
CCXT_CONFIG = {"enableRateLimit": True}


def path_from_relative_to_absolut(relative:str):
    #Create absolut path with relative one 
    script_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_directory, relative)

def json_loader(path:str) -> {}:
    file_path = path_from_relative_to_absolut(path)
    # file loading from JSON 
    with open(file_path, 'r') as file:
        file = json.load(file)
    return file

class IntervalUnitError(Exception):
    def __init__(self, unit, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unit = unit

    def __str__(self):
        return f'Wrong unit for loading of setting.json for intervals definition || {self.unit} ||, please use only : seconds | minutes | hours | days | weeks'

class TooManyError(Exception):
    pass

class MarketDataManager:
    def _log_message(self, pair:str, interval:str,market:str, messsage:str):
        return f'|{market}|[{pair}] - {interval} - {messsage}'

    #Create ist of interval 
    def _create_timegaps(self, start_date, end_date, interval:str):
        interval_ms = self._create_timedelta(interval)
        while start_date <= end_date:
            yield start_date
            start_date += interval_ms

    #Find interval in loaded configuration | Y -> return timedelta linked | N -> Raise exception
    def _create_timedelta(self, interval:str) -> int:
        try:
            return timedelta(milliseconds=self.intervals[interval]["interval_ms"])
        except Exception:
            raise ValueError(f"{interval} not support interval")

    #Check if the requiere datas in already in the file 
    async def _is_data_missing(self, file_name, last_dt, start_date) -> bool | datetime:

        await self.ccxt_exchange.close()

        if os.path.isfile(file_name):
            df = pd.read_csv(file_name, index_col=0, parse_dates=True)
            df.index = pd.to_datetime(df.index, unit="ms")
            df = df.groupby(df.index).first()

            if int(pytz.utc.localize(df.index[-1]).timestamp() * 1000) >= last_dt:
                return False
        else:
            return datetime.fromisoformat(start_date)
        return pytz.utc.localize(df.index[-2])
    
    #Manage all requet to API 
    async def _download_tf(self, coin, interval, start_timestamp) -> list:
        tests = 0
        while True:
            try:
                r = await self.ccxt_exchange.fetch_ohlcv(
                    symbol=coin.replace('-', '/'),
                    timeframe=interval,
                    since=start_timestamp,
                    limit=self.exchange["limit_size_request"],
                )
                self.pbar.update(1)
                return r
            except Exception:
                tests += 1
                if tests == 3:
                    raise TooManyError
        
    #Tool to convert JSON setting in timedelta
    def _deltatime(self, unit:str, value:int):
        if unit == "seconds":
            return timedelta(seconds=value)
        elif unit == "minutes":
            return timedelta(minutes=value)
        elif unit == "hours":
            return timedelta(hours=value)
        elif unit == "days":
            return timedelta(days=value)
        elif unit == "weeks":
            return timedelta(weeks=value)
        else :
            raise IntervalUnitError(unit)

    #Loading of settingScrapper.json like current settings
    def _loadsettings(self, settings_path:str):

    # Obtenez le chemin du répertoire du script en cours d'exécution
        # file loading from JSON 
        settings_file = json_loader(settings_path)
        
        #Create dict for all exchange supported
        ccxt_exchanges = {
            exchange["name"]: {
                "ccxt_object": getattr(ccxt, exchange["ccxt_object"])(config=CCXT_CONFIG),
                "limit_size_request": exchange["limit_size_request"]
                } for exchange in settings_file["exchanges"]
            }
        
        #Create dict for all intervals supported
        intervals = {
            timeframe["name"]:{
                "timedelta": self._deltatime(str(timeframe["timedelta"]["unit"]),int(timeframe["timedelta"]["value"])),
                "interval_ms": timeframe["interval_ms"]
                } for timeframe in settings_file["timeframes"]
            }
        
        #Create list for all coinspair supported
        coins=[coinpair["pair"] for coinpair in settings_file["coins"]]
        self.logs.log_info(MODULE_NAME, self._log_message("FULL","FULL","FULL"," Loading of configuration OK"))
        return ccxt_exchanges, intervals, coins

    #Load the market data between 2 dates - coin:str -> ex: BTCUSDT| interval:str -> ex: 1h | start_date: year | end_date: year
    def _load_data_from_path(self, data_path) -> pd.DataFrame:
        #Check if file exist sinon FileNotFoundError
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Le fichier {data_path} n'existe pas ==> Data introuvable !")

        #Load selected data from csv file
        df = pd.read_csv(data_path, index_col=0, parse_dates=True)
        return df
    
    #Init for ExchnagerManager - exchange:str -> name of the exchnage (example : binance) | download:str -> alternative folder for data
    def __init__(self, exchange_name:str, download:str = PATH_DOWNLOADS) -> None:
        self.logs = LoggerManager()
        #Loading of all settings from scrapperSetting.json
        self.ccxt_market, self.intervals, self.coins = self._loadsettings(PATH_SETTINGS)

        #Optimization for compatibility
        self.exchange_name = exchange_name.lower()
        self.download = download

        #Check if the exchange is implemented
        try:
            self.exchange = self.ccxt_market[self.exchange_name]
        except Exception:
            raise NotImplementedError(f"L'échange {self.exchangename} n'est pas supporté")
        
        #Select the ccxt object in the collection
        self.ccxt_exchange = self.ccxt_market[self.exchange_name]["ccxt_object"]

        #Create folder if not exist
        os.makedirs(path_from_relative_to_absolut(f"{self.download}/{self.exchange_name}"), exist_ok=True)
        #Preset progressbar
        self.pbar = None

    #Engine for download data from market with coin_pairs and intervals 
    async def download_data(self,
        coins:[],
        intervals:[],
        start_date:str="2017-01-01 00:00:00", #format of string YYYY-MM-DD HH:MM:SS"
        end_date:str=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ) -> None:

        #Check input intervals and coins ==> if empty take full settings
        if len(coins) <= 0 :
            self.logs.log_info(MODULE_NAME, self._log_message("FULL","FULL","FULL","Coins par defaut loaded"))
            coins = self.coins

        if len(intervals) <= 0 :
            self.logs.log_info(MODULE_NAME, self._log_message("FULL","FULL","FULL","Intervals par defaut loaded"))
            intervals = self.intervals.keys()        

        #Loading selected market via CCXT
        await self.ccxt_exchange.load_markets()

        #Parse start and end in time
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    
    
        for interval in intervals:

            #Define all timegaps in the selected interval (example "1h")
            timegaps_for_interval = list(self._create_timegaps(start_date, end_date, interval))
            last_timegaps_period = timegaps_for_interval[-1].astimezone(pytz.utc)
            timestamp_end_period = int(last_timegaps_period.timestamp() * 1000)
          
            for coin in coins:
                print(f"\Download [{coin}] in |{interval}| on - {self.exchange_name}")

                #Create architecture of folders
                file_path = path_from_relative_to_absolut(f"{self.download}/{self.exchange_name}/{interval}/")
                os.makedirs(file_path, exist_ok=True)
                file_name = f"{file_path}{coin.replace('/', '-')}.csv"

                data_already_in_the_file = await self._is_data_missing(file_name, timestamp_end_period, str(start_date))
                
                if data_already_in_the_file:
                    
                    #Define task list for multi routine + start date
                    tasks = []
                    current_timestamp = int(data_already_in_the_file.timestamp() * 1000)
                 
                    while current_timestamp <= timestamp_end_period:
                        tasks.append(asyncio.create_task(self._download_tf(coin, interval, current_timestamp)))
                        current_timestamp = (self.exchange["limit_size_request"] * self.intervals[interval]["interval_ms"] + current_timestamp)

                    self.pbar = tqdm(tasks)
                    results = await asyncio.gather(*tasks)
                    await self.ccxt_exchange.close()
                  
                    results = list(itertools.chain(*results))
                    self.pbar.close()

                    if results:
                        df_final = pd.DataFrame(results,columns=["date", "open", "high", "low", "close", "volume"],)
                        df_final.set_index('date', drop=False, inplace=True)
                        df_final = df_final[~df_final.index.duplicated(keep='first')]
                        
                        
                        flag_header = (("a", False) if os.path.exists(file_name) else ("w", True))
                        with open(file_name, mode=flag_header[0]) as f:
                            df_final.to_csv(path_or_buf=f, header=flag_header[1], index=False)
                        self.logs.log_info(MODULE_NAME, self._log_message(coin, interval, self.exchange_name, "Fully loaded "))
                    else:
                        print(f"\tNo DATA [{coin}] in |{interval}| on period")
                    self.logs.log_error(MODULE_NAME, self._log_message(coin, interval, self.exchange_name, "No data found !"))
                else:
                    self.logs.log_error(MODULE_NAME, self._log_message(coin, interval, self.exchange_name, "No data require !"))
                    print(f"\tAlready in the file for [{coin}] in |{interval}|")

    def load_full_market(self):
        output_df = pd.DataFrame()
        available_data, available_data_path = self.explore_data()
        for file in available_data_path:
            current_file_data = self._load_data_from_path(file)
            file_split = file.split("\\")
            current_file_data["Exchange"] = self.exchange_name
            current_file_data["Pair"] = file_split[7][:-4]
            current_file_data["Interval"] = file_split[6]
            output_df = pd.concat([output_df, current_file_data])

        return output_df, available_data

    #Load the market data between 2 dates - coin:str -> ex: BTCUSDT| interval:str -> ex: 1h | start_date: year | end_date: year
    def load_data_from_csv_db(self, coin_pair:str, interval_name:str,start_date:str="2017-01-01 00:00:00", #format of string YYYY-MM-DD HH:MM:SS"
                              end_date:str=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),) -> pd.DataFrame:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        #Define the target path for reading data (according to interval_name and coin_pair)
        target_file_path = path_from_relative_to_absolut(f"{self.download}/{self.exchange_name}/{interval_name}/")
        target_file_name = f"{target_file_path}{coin_pair.replace('/', '-')}.csv"

        #Check if file exist sinon FileNotFoundError
        if not os.path.exists(target_file_name):
            raise FileNotFoundError(f"Le fichier {target_file_name} n'existe pas ==> Data introuvable !")

        #Load selected data from csv file
        df = pd.read_csv(target_file_name, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, unit="ms")
        df = df.groupby(df.index).first()
        df = df.loc[start_date:end_date]
        df = df.iloc[:-1]
        return df

    #Read all csv and format it for reading
    def explore_data(self):
        files_data = []
        paths = []
        for path, folders, files in os.walk(path_from_relative_to_absolut(f"{self.download}/{self.exchange_name}")):
            for folder in folders:
                for file in  os.listdir(os.path.join(path, folder)):
                    if os.path.join(path, folder, file).endswith(".csv"):
                        current_file = os.path.join(path, folder, file)
                        file_split = current_file.split("\\")
                        try:
                            df_file = pd.read_csv(current_file)
                        except Exception:
                            continue
                            
                        paths.append(path_from_relative_to_absolut(os.path.join(path, folder, file)))
                                                             
                        files_data.append(
                            {
                                "exchange": self.exchange_name,
                                "timeframe": file_split[6],
                                "pair": file_split[7][:-4],
                                "occurences": len(df_file),
                                "start_date": str(
                                    datetime.fromtimestamp(df_file.iloc[0]["date"] / 1000)
                                ),
                                "end_date": str(
                                    datetime.fromtimestamp(df_file.iloc[-1]["date"] / 1000)
                                ),
                            }
                        )


        return pd.DataFrame(files_data), paths

# =========================================Example utilisation - load all data according to the sereting ======================================================

async def main():
    myExchange = MarketDataManager(exchange_name="binance")
    await myExchange.download_data([], [], "2020-01-01 00:00:00")

    
if __name__ == "__main__":
    asyncio.run(main())