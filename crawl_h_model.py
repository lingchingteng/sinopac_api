import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup


def daterange(start_date, end_date):
    cur_date = start_date
    while cur_date <= end_date:
        yield cur_date
        cur_date += timedelta(days=1)


def main(start_date=datetime.today(), end_date=datetime.today()):
    future_url = "https://www.taifex.com.tw/cht/3/futDailyMarketReport"

    future_params = {
        "queryType": "2",
        "marketCode": "0",
        "dateaddcnt": "",
        "commodity_id": "TX",
        "commodity_id2": "",
        "queryDate": "",
        "MarketCode": "0",
        "commodity_idt": "TX",
        "commodity_id2t": "",
        "commodity_id2t2": "",
    }

    records = []
    error_flag = False
    for d in daterange(start_date, end_date):
        if d != start_date:
            print("Sleep 6 seconds")
            time.sleep(6)

        data = {}
        data["date"] = d.strftime("%Y/%m/%d")

        print(f'crawl date: {d.strftime("%Y/%m/%d")}')

        queryDate = d.strftime("%Y/%m/%d")
        future_params["queryDate"] = queryDate

        r = requests.post(future_url, future_params)
        soup = BeautifulSoup(r.text, features="lxml")
        table = soup.find("table", class_="table_f")
        if table is None:
            print("no data")
            continue
        df = pd.read_html(table.prettify())[0]
        data["cur_future"] = str(int(df.iloc[:2]["最後  成交價"][0]))
        data["next_future"] = str(int(df.iloc[:2]["最後  成交價"][1]))

        volume_url_pattern = (
            "https://www.twse.com.tw/exchangeReport/MI_5MINS?response=json&date={date}"
        )
        r = requests.get(volume_url_pattern.format(date=d.strftime("%Y%m%d")))
        if r.status_code == requests.codes.ok:
            try:
                j = r.json()
                data["volume"] = j["data"][-1][-1].replace(",", "")
            except Exception as e:
                print("get volume error!", e)
                error_flag = True
                break

        index_url_pattern = (
            "https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date}"
        )
        r = requests.get(index_url_pattern.format(date=d.strftime("%Y%m%d")))
        if r.status_code == requests.codes.ok:
            try:
                j = r.json()
                for i in j["data"]:
                    if i[0] == f'{d.year-1911}/{d.strftime("%m/%d")}':
                        data["index"] = i[4].replace(",", "")
                        break
            except Exception as e:
                print("get index error!", e)
                error_flag = True
                break

        print(data)
        records.append(data)

    if not error_flag:
        df = pd.DataFrame(
            records, columns=["date", "index", "volume", "cur_future", "next_future"]
        )
        df.to_csv("test_future.csv", index=False, header=False)
    else:
        print("somethings error!")


if __name__ == "__main__":
    # start_date, end_date
    main(datetime(2021, 6, 1), datetime(2021, 7, 24))
    main()
