from datetime import datetime, timedelta

from main import SJ_module


def daterange(start_date, end_date):
    cur_date = start_date
    while cur_date != end_date:
        yield cur_date
        cur_date += timedelta(days=1)


if __name__ == "__main__":
    sj = SJ_module()
    for stock_symbol in sj.get_list_stock_symbol():
        tmp = datetime.today() - timedelta(hours=14, minutes=30)
        end_date = datetime(tmp.year, tmp.month, tmp.day)
        for d in daterange(start_date=datetime(2018, 12, 7), end_date=end_date):
            sj.save_tick_to_csv(stock_symbol.code, d.strftime("%Y-%m-%d"))
