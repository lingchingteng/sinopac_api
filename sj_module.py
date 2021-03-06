import os
from pathlib import Path

import pandas as pd
import shioaji as sj
from dotenv import load_dotenv

from category_mapping import category_mapping as cm


class SJ_module:
    def __init__(self):
        load_dotenv()
        user_id = os.environ.get("USER_ID")
        password = os.environ.get("PASSWORD")
        ca_password = os.environ.get("CA_PASSWORD")
        ca_path = os.environ.get("CA_PATH")
        if (
            user_id is None
            or password is None
            or ca_password is None
            or ca_path is None
        ):
            print(user_id, password, ca_password, ca_path)
            raise ValueError("user_id, password, ca_password, ca_path error!")

        self.api = sj.Shioaji()
        self.accounts = self.api.login(user_id, password)
        # self.api.activate_ca(
        #     ca_path=ca_path, ca_passwd=ca_password, person_id=user_id,
        # )

    def get_ticks(self, stock_symbol, date):
        return self.api.ticks(
            contract=self.api.Contracts.Stocks[stock_symbol], date=date
        )

    def get_kbars(self, stock_symbol, date):
        return self.api.kbars(
            contract=self.api.Contracts.Stocks[stock_symbol],
            start=date,
            end=date,
        )

    def get_list_stock(self):
        ret = []
        for m in (
            self.api.Contracts.Stocks.TSE,
            self.api.Contracts.Stocks.OTC,
            self.api.Contracts.Stocks.OES,
        ):
            ret.extend([s for s in m if s.category in cm.keys()])
        return ret

    def save_tick_to_csv(self, stock, date):

        tick_folder = Path(f"data/ticks/{stock}")
        tick_folder.mkdir(parents=True, exist_ok=True)

        csv_filename = f"{stock}_{date}.csv"

        if (tick_folder / csv_filename).exists():
            print((tick_folder / csv_filename), "exist!")
            return

        ticks = self.get_ticks(stock, date)
        df = pd.DataFrame({**ticks})

        if len(df) == 0:
            # print(f"{stock} at {date} have no tick data.")
            return

        df.ts = pd.to_datetime(df.ts)

        cols = [
            "ts",
            "close",
            "volume",
            "ask_price",
            "ask_volume",
            "bid_price",
            "bid_volume",
        ]

        # print(f"Saving tick for {stock} at {date}")
        df.to_csv(tick_folder / csv_filename, index=False, columns=cols)

    def save_kbar_to_csv(self, stock, date):
        kbar_folder = Path(f"data/kbars/{stock}")
        kbar_folder.mkdir(parents=True, exist_ok=True)

        csv_filename = f"{stock}_{date}.csv"

        if (kbar_folder / csv_filename).exists():
            print((kbar_folder / csv_filename), "exist!")
            return

        kbars = self.get_kbars(stock, date)

        df = pd.DataFrame({**kbars})

        if len(df) == 0:
            # print(f"{stock} at {date} have no kbar data.")
            return

        df.ts = pd.to_datetime(df.ts)

        cols = ["ts", "Open", "Close", "High", "Low", "Volume"]

        # print(f"Saving kbar for {stock} at {date}")
        df.to_csv(kbar_folder / csv_filename, index=False, columns=cols)
