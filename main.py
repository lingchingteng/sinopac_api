from sj_module import SJ_module

if __name__ == "__main__":
    list_stocks = ["1101B"]
    list_date = ["2021-06-24"]

    sj_module = SJ_module()

    for stock in list_stocks:
        for date in list_date:
            sj_module.save_tick_to_csv(stock, date)
