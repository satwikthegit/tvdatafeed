import datetime
import pytz
import enum
import json
import os
import logging
import random
import re
import string
import pandas as pd
from websocket import create_connection
import requests
import json

logger = logging.getLogger(__name__)
NYSE_TIME = pytz.timezone('America/New_York')

class Interval(enum.Enum):
    in_1_second = "1S"
    in_3_second = "3S"
    in_5_second = "5S"
    in_10_second = "10S"
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"


class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://www.tradingview.com", "User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 5

    def __init__(
        self,
        username: str = None,
        password: str = None,
    ) -> None:
        """Create TvDatafeed object

        Args:
            username (str, optional): tradingview username. Defaults to None.
            password (str, optional): tradingview password. Defaults to None.
        """

        self.ws_debug = False

        self.token = self.__auth(username, password)
        print('token: ', self.token)
        # self.token = "eyJhbGciOiJSUzUxMiIsImtpZCI6IkdaeFUiLCJ0eXAiOiJKV1QifQ.eyJ1c2VyX2lkIjoxMTg0MjQ3OSwiZXhwIjoxNzI4MDcyMDkzLCJpYXQiOjE3MjgwNTc2OTMsInBsYW4iOiJwcm9fcHJlbWl1bSIsImV4dF9ob3VycyI6MSwicGVybSI6IiIsInN0dWR5X3Blcm0iOiJ0di1jaGFydHBhdHRlcm5zLHR2LWNoYXJ0X3BhdHRlcm5zLHR2LXByb3N0dWRpZXMsdHYtdm9sdW1lYnlwcmljZSIsIm1heF9zdHVkaWVzIjoyNSwibWF4X2Z1bmRhbWVudGFscyI6MTAsIm1heF9jaGFydHMiOjgsIm1heF9hY3RpdmVfYWxlcnRzIjo0MDAsIm1heF9zdHVkeV9vbl9zdHVkeSI6MjQsImZpZWxkc19wZXJtaXNzaW9ucyI6WyJyZWZib25kcyJdLCJtYXhfb3ZlcmFsbF9hbGVydHMiOjIwMDAsIm1heF9hY3RpdmVfcHJpbWl0aXZlX2FsZXJ0cyI6NDAwLCJtYXhfYWN0aXZlX2NvbXBsZXhfYWxlcnRzIjo0MDAsIm1heF9jb25uZWN0aW9ucyI6NTB9.VovxBBJy82zTp8OTsAPQdY8LcrDYaR20vClR-Vl6TykgGzVBn_jO9w_DP1-RrWrA3buJ3kSx7v5jsc8gh_wjrOl-iXWLpiZZw0VOZCQ3uqT_EE3ZLIczBas7YAMXNnH0pdbY7KQDKZMNO9dvcfZIRUy_UsxiivBSqID6u0Zy3fA"

        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning(
                "you are using nologin method, data you access may be limited"
            )

        self.ws = None
        self.session = self.__generate_session()
        self.replay_session = self.__generate_replay_session()
        self.replay_session_rand = self.__generate_random()
        self.chart_session = self.__generate_chart_session()


    def __auth(self, username, password):

        if (username is None or password is None):
            token = None

        else:
            data = {"username": username,
                    "password": password,
                    "remember": "on"}
            try:
                response = requests.post(
                    url=self.__sign_in_url, data=data, headers=self.__signin_headers)
                logger.debug(response.json())
                token = response.json()['user']['auth_token']
            except Exception as e:
                logger.error('error while signin')
                token = None
                exit()

        return token

    def __create_connection(self):
        logging.debug("creating websocket connection")
        self.ws = create_connection(
            "wss://prodata.tradingview.com/socket.io/websocket", headers=self.__ws_headers, timeout=self.__ws_timeout
        )

    @staticmethod
    def __filter_raw_message(text):
        try:
            found = re.search('"m":"(.+?)",', text).group(1)
            found2 = re.search('"p":(.+?"}"])}', text).group(1)

            return found, found2
        except AttributeError:
            logger.error("error in filter_raw_message")

    @staticmethod
    def __generate_random(stringLength = 12):
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return random_string

    @staticmethod
    def __generate_session():
        return "qs_" + TvDatafeed.__generate_random()

    @staticmethod
    def __generate_replay_session():
        return "rs_" + TvDatafeed.__generate_random()

    @staticmethod
    def __generate_chart_session():
        return "cs_" + TvDatafeed.__generate_random()

    @staticmethod
    def __prepend_header(st):
        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, paramList):
        return self.__prepend_header(self.__construct_message(func, paramList))

    def __send_message(self, func, args):
        m = self.__create_message(func, args)
        if self.ws_debug:
            logger.debug(m)
        self.ws.send(m)

    @staticmethod
    def __create_df(raw_data, symbol):
        try:
            # re.sub('"s":[],','', data)
            out = re.search('"s":\[(.+?)\}\]', raw_data.replace('"s":[]', '')).group(1)
            x = out.split(',{"')
            data = list()
            volume_data = True

            for xi in x:
                xi = re.split("\[|:|,|\]", xi)
                ts = datetime.datetime.fromtimestamp(float(xi[4]), tz=NYSE_TIME)

                row = [ts]

                for i in range(5, 10):

                    # skip converting volume data if does not exists
                    if not volume_data and i == 9:
                        row.append(0.0)
                        continue
                    try:
                        row.append(float(xi[i]))

                    except ValueError:
                        volume_data = False
                        row.append(0.0)
                        logger.debug('no volume data')

                data.append(row)

            data = pd.DataFrame(
                data, columns=["datetime", "open",
                               "high", "low", "close", "volume"]
            ).set_index("datetime")
            # data.insert(0, "symbol", value=symbol)
            return data
        except AttributeError:
            logger.error("no data, please check the exchange and symbol")

    @staticmethod
    def __format_symbol(symbol, exchange, contract: int = None):

        if ":" in symbol:
            pass
        elif contract is None:
            symbol = f"{exchange}:{symbol}"

        elif isinstance(contract, int):
            symbol = f"{exchange}:{symbol}{contract}!"

        else:
            raise ValueError("not a valid contract")

        return symbol

    def add_replay_to_hist(self, symbol, interval, extended_session, start_date):
        logger.debug('---'*50)
        self.ws.recv() # dumping old messages
        self.ws.recv()
        self.__send_message("replay_create_session", [self.replay_session])
        # print(self.ws.recv())
        self.__send_message("replay_get_depth", [ 
                self.replay_session,
                self.replay_session_rand+'0',
                '={"symbol":"'
                + symbol
                + '","adjustment":"splits","session":'
                + ('"regular"' if not extended_session else '"extended"')
                + "}",
                interval
            ])
        # print(self.ws.recv())
        self.__send_message("replay_reset", [self.replay_session,  self.replay_session_rand+'1',int(start_date.timestamp())])
        self.ws.recv()
        self.__send_message("replay_add_series", [
                self.replay_session,
                self.replay_session_rand+'2',
                '={"symbol":"'
                + symbol
                + '","adjustment":"splits","session":'
                + ('"regular"' if not extended_session else '"extended"')
                + "}",
                interval
            ])
        # print(self.ws.recv())
        self.__send_message("resolve_symbol", [
                self.chart_session,
                "sds_sym_2",
                '={"replay":"'
                + self.replay_session
                +'","symbol":{"adjustment":"splits","currency-id":"USD","session":'
                + ('"regular"' if not extended_session else '"extended"')
                +',"symbol":"'
                + symbol
                +'"}}'
            ])
        self.ws.recv()
        self.__send_message("modify_series", [self.chart_session,  "sds_1", "s2", 'sds_sym_2', interval, ""])
        # print(self.ws.recv())


    def get_hist(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        exchange: str = "NSE",
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        fut_contract: int = None,
        extended_session: bool = False,
    ) -> pd.DataFrame:
        """get historical data

        Args:
            symbol (str): symbol name
            exchange (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            interval (str, optional): chart interval. Defaults to 'D'.
            n_bars (int, optional): no of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.
            extended_session (bool, optional): regular session if False, extended session if True, Defaults to False.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        symbol = self.__format_symbol(
            symbol=symbol, exchange=exchange, contract=fut_contract
        )

        interval = interval.value
        replay = False
        if start_date is not None and end_date is not None:
            start_date = datetime.datetime.strptime(start_date+' 23:59:59-04:00','%Y-%m-%d %H:%M:%S%z')
            end_date = datetime.datetime.strptime(end_date+' 09:30:00-04:00','%Y-%m-%d %H:%M:%S%z')
            replay = True

        self.__create_connection()

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])
        self.__send_message("quote_create_session", [self.session])
        self.__send_message(
            "quote_set_fields",
            [
                self.session,
                "ch",
                "chp",
                "current_session",
                "description",
                "local_description",
                "language",
                "exchange",
                "fractional",
                "is_tradable",
                "lp",
                "lp_time",
                "minmov",
                "minmove2",
                "original_name",
                "pricescale",
                "pro_name",
                "short_name",
                "type",
                "update_mode",
                "volume",
                "currency_code",
                "rchp",
                "rtc",
            ],
        )

        self.__send_message(
            "quote_add_symbols", [self.session, symbol,
                                  # {"flags": ["force_permission"]}
                                  ]
        )
        self.__send_message("quote_fast_symbols", [self.session, symbol])

        self.__send_message(
            "resolve_symbol",
            [
                self.chart_session,
                "sds_sym_1",
                '={"symbol":"'
                + symbol
                + '","adjustment":"splits","session":'
                + ('"regular"' if not extended_session else '"extended"')
                + "}",
            ],
        )
        # logger.debug(self.ws.recv())
        self.__send_message(
            "create_series",
            [self.chart_session, "sds_1", "s1", "sds_sym_1", interval, n_bars],
        )
        # logger.debug(self.ws.recv())

        self.__send_message("switch_timezone", [
                            self.chart_session, "exchange"])
        if replay:
            self.add_replay_to_hist(symbol, interval, extended_session, start_date)

        raw_data = ""

        logger.debug(f"getting data for {symbol}...")
        i = 0
        df = None
        while True:
            # logger.debug('getting data for '+ str(i*1000))
            try:
                result = self.ws.recv()
                logger.debug(result)
                if('qsd' not in result):
                    # print(result)
                    raw_data = raw_data + result + "\n"
            except Exception as e:
                logger.error(e)
                break
            if 'error' in result:
                print('error reported:---')
                print(result)
                print('error complete:---')
            if re.match('~m~\d+~m~~h~\d+', result):
                self.ws.send(re.match('~m~\d+~m~~h~\d+', result)[0])
            if "series_completed" in result:
                df_ = self.__create_df(raw_data, symbol)
                if df is not None:
                    df = pd.concat([df, df_])
                else:
                    df = df_
                if "data_completed" in result:
                    n_dt = self.save_df(df,symbol)
                    df = None
                    if replay and end_date.timestamp() < n_dt:
                        print(f'getting data from {n_dt}')
                        self.__send_message("replay_reset", [self.replay_session,  self.replay_session_rand+'1',int(n_dt)])
                    else:
                        self.ws.close()
                        break
                else:
                    i+=1
                    self.__send_message(
                        "request_more_data",
                        [self.chart_session, "sds_1", 2000],
                    )
                raw_data = ""
        

    def save_df(self,df, symbol):
        if df is not None:
            df.sort_index(ascending=True, inplace=True)
            dates = pd.to_datetime(df.index.to_series()).dt.date.unique()
            dir_name = f'/home/satwik/projects/livep/historical-data/nyse/{symbol}/'
            if not os.path.exists(dir_name):
                os.mkdir(dir_name)
            for d in dates:
                d_t = d.strftime('%Y-%m-%d')
                fname = dir_name+ symbol+'-'+d_t+'.csv'
                df.loc[d_t: d_t].to_csv(fname)
                print(f'saved to {fname}')
            print(df.size)
            n_dt = df.index[0].timestamp() - 3600*8
            print(f'next time stamp {n_dt}')
            return n_dt


    def search_symbol(self, text: str, exchange: str = ''):
        url = self.__search_url.format(text, exchange)

        symbols_list = []
        try:
            resp = requests.get(url)

            symbols_list = json.loads(resp.text.replace(
                '</em>', '').replace('<em>', ''))
        except Exception as e:
            logger.error(e)

        return symbols_list


if __name__ == "__main__":
    logging.basicConfig(filename = 'reply.log', filemode='w',level=logging.DEBUG)
    tv = TvDatafeed('satwikeshwar', 'p01a91r1')
    tv.ws_debug=True
    # print(tv.get_hist("CRUDEOIL", "MCX", fut_contract=1))
    # print(tv.get_hist("NIFTY", "NSE", fut_contract=1))
    symbols = ['NASDAQ:NVDA']
    # symbols = ['NYSE:PLTR', 'NYSE:VALE', 'NYSE:F', 'NASDAQ:JD', 'NYSE:RBLX', 'NYSE:BAC', 'NYSE:NU', 'NASDAQ:GOOGL', 'NASDAQ:WBD', 'NYSE:UBER', 'NYSE:XPEV', 'NYSE:PBR', 'NYSE:GM', 'NASDAQ:BILI', 'NYSE:YMM', 'NYSE:WMT', 'NYSE:CVE', 'NYSE:XOM', 'NASDAQ:DOCU', 'NYSE:SNAP', 'NYSE:NOK', 'NYSE:OXY', 'NYSE:KO', 'NYSE:SLB', 'NASDAQ:HBAN', 'NYSE:FCX', 'NASDAQ:AFRM', 'NASDAQ:CSCO', 'NYSE:WFC', 'NASDAQ:GOOG', 'NYSE:HPE', 'NYSE:PR', 'NYSE:KVUE', 'NYSE:EQT', 'NYSE:NEE']
    for symbol in symbols:
        tv.get_hist(
            symbol=symbol,
            # exchange="NASDAQ",
            interval=Interval.in_1_second,
            n_bars=5000,
            extended_session=False,
            start_date='2024-12-18',
            end_date ='2024-10-01',
        )
    