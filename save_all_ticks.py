from datetime import datetime, timedelta

from main import SJ_module


def daterange(start_date, end_date):
    cur_date = start_date
    while cur_date != end_date:
        yield cur_date
        cur_date += timedelta(days=1)


if __name__ == "__main__":
    sj = SJ_module()

    list_stock_code = [s.code for s in sj.get_list_stock_symbol()]

    start_date = datetime(2018, 12, 7)
    tmp = datetime.today() - timedelta(hours=14, minutes=35) + timedelta(days=1)
    end_date = datetime(tmp.year, tmp.month, tmp.day)
    list_daterange = [d.strftime("%Y-%m-%d") for d in daterange(start_date, end_date)]

    for stock_code in list_stock_code:
        for d in list_daterange:
            sj.save_tick_to_csv(stock_code, d)
