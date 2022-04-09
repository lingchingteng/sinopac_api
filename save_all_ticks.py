import glob
from datetime import datetime, timedelta
from pathlib import Path

from sj_module import SJ_module
from tqdm import tqdm


def daterange(start_date, end_date):
    cur_date = start_date
    while cur_date <= end_date:
        yield cur_date
        cur_date += timedelta(days=1)


if __name__ == "__main__":
    sj = SJ_module()

    list_stock = sorted(sj.get_list_stock(), key=lambda x: x.code)
    for idx, stock in enumerate(list_stock):
        glob_files = glob.glob(str(Path("data") / "ticks" / f"{stock.code}" / "*.csv"))
        if len(glob_files) > 0:
            latest_file = sorted(
                glob_files,
                reverse=True,
            )[0]
            latest_date = datetime.strptime(
                Path(latest_file).stem.split("_")[-1], "%Y-%m-%d"
            ) + timedelta(days=1)
        else:
            latest_date = datetime(2018, 12, 7)

        start_date = max(latest_date, datetime(2018, 12, 7))

        update_date = datetime.strptime(stock.update_date, "%Y/%m/%d")
        today = datetime.today() - timedelta(hours=14, minutes=35)

        end_date = min(update_date, datetime(today.year, today.month, today.day))
        list_daterange = [
            d.strftime("%Y-%m-%d") for d in daterange(start_date, end_date)
        ]
        if len(list_daterange) > 0:
            print(
                f"[{idx}/{len(list_stock)}] save {stock.code} from {list_daterange[0]} ~ {list_daterange[-1]}"
            )
            for d in tqdm(list_daterange):
                sj.save_tick_to_csv(stock.code, d)
        else:
            print(f"[{idx}/{len(list_stock)}] {stock.code} is up-to-date")

    for idx, stock in enumerate(list_stock):
        glob_files = glob.glob(str(Path("data") / "kbars" / f"{stock.code}" / "*.csv"))
        if len(glob_files) > 0:
            latest_file = sorted(
                glob_files,
                reverse=True,
            )[0]
            latest_date = datetime.strptime(
                Path(latest_file).stem.split("_")[-1], "%Y-%m-%d"
            ) + timedelta(days=1)
        else:
            latest_date = datetime(2018, 12, 7)

        start_date = max(latest_date, datetime(2018, 12, 7))

        update_date = datetime.strptime(stock.update_date, "%Y/%m/%d")
        today = datetime.today() - timedelta(hours=14, minutes=35)

        end_date = min(update_date, datetime(today.year, today.month, today.day))
        list_daterange = [
            d.strftime("%Y-%m-%d") for d in daterange(start_date, end_date)
        ]
        if len(list_daterange) > 0:
            print(
                f"[{idx}/{len(list_stock)}] save {stock.code} from {list_daterange[0]} ~ {list_daterange[-1]}"
            )
            for d in tqdm(list_daterange):
                sj.save_kbar_to_csv(stock.code, d)
        else:
            print(f"[{idx}/{len(list_stock)}] {stock.code} is up-to-date")
