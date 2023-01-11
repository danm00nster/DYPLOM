import matplotlib.pyplot as plt
import pandas as pd
import wget
import os
import sqlalchemy
import requests
import json
from requests.exceptions import HTTPError


def add_bollinger_band(input_dataframe: pd.DataFrame, data_column):
    """ dodaje pola L-band i U-band do dataframe przekazanego do funkcji
        input_dataframe - dane wejściowe,
        data_column - kolumna zawierająca dane do obliczeń"""
    data_frame = input_dataframe[[data_column]]
    std_dev = data_frame.rolling(window=20).std()
    sma = data_frame.rolling(window=20).mean()
    lower_band = sma - 2 * std_dev
    lower_band = lower_band.rename(columns={data_column: "L-band"})
    upper_band = sma + 2 * std_dev
    upper_band = upper_band.rename(columns={data_column: "U-band"})
    data_frame = data_frame.join(upper_band).join(lower_band)
    data_frame.drop([data_column], axis=1, inplace=True)
    data_frame = data_frame.round(2)
    output_dataframe = input_dataframe.join(data_frame)
    return output_dataframe


def get_quotes(symbol, date_set):
    """ pobiera notowania giełdowe z serwisu stooq.pl
        symbol - ticker dla którego pobierane są dane
        date_set - zestaw dat w postaci tupli data początkowa, data końcowa.
        maksymalny jednorazowy przedział to 12 miesięcy"""
    filename = symbol + ".txt"
    output_data_frame = pd.DataFrame()
    print(date_set)
    for START_DATE, END_DATE in date_set:
        print(f'>{START_DATE}< >{END_DATE}<')
        if os.path.exists(filename):
            os.remove(filename)
        url = "https://stooq.pl/q/d/l/?s={0}&d1={1}&d2={2}&i=d".format(symbol, START_DATE.replace("-", ""),
                                                                       END_DATE.replace("-", ""))
        wget.download(url, filename)
        try:
            df_notowania = pd.read_csv(filename, index_col='Data', parse_dates=True)
            df_notowania['walor'] = symbol
            df_notowania.dropna(inplace=True)
            if len(output_data_frame.index) == 0:
                output_data_frame = df_notowania
            else:
                output_data_frame = pd.concat([output_data_frame, df_notowania])
        except:
            print("brak danych za okres ", symbol, START_DATE, END_DATE)
            output_data_frame = pd.DataFrame()
    return output_data_frame


def get_symbol_list(filename, column):
    df_symbol_list = pd.read_csv(filename, sep=';')
    print(df_symbol_list)
    symbol_list = df_symbol_list[column].to_list()
    return symbol_list


def get_all_stooq(date_set, walor_list, to_csv=True, csv_mode='a', to_database=False, data_base_mode='append'):
    """ pobiera cały zestaw danych ze stooq
    date_set - zestaw dat start, end
    to_csv - True (domyślnie) /False - zapis do pliku csv
    csv_mode - a= append (domyślnie), w=write
    to_database_ True/False(domyślnie) - zapis do bazy danych
    data_base_mode - wartości jak w .to_sql append= wstawia wiersze(domyślnie), replace=drop table przed zapisem do bazy
    """
    if os.path.exists('kursy.csv'):
        os.remove('kursy.csv')
    csv_header = True
    base_data_frame = pd.DataFrame()
    for walor in walor_list:
        column_name = walor
        base_data_frame = pd.DataFrame()
        base_data_frame = get_quotes(column_name, date_set)
        base_data_frame = add_bollinger_band(base_data_frame, 'Zamkniecie')
        if to_csv:
            base_data_frame.to_csv("kursy.csv", mode=csv_mode, encoding="utf-8", sep=';', header=csv_header)
            csv_mode = 'a'
            csv_header = False

        if to_database:
            engine = sqlalchemy.create_engine('mssql+pymssql://adminuser:TjmnhdMySQL1!@pwserver2.database.windows.net:'
                                              '1433/PWdatabase')
            base_data_frame.to_sql('notowaniaGPW', if_exists=data_base_mode, con=engine)
            data_base_mode = 'append'
    print('Base data frame')
    print(base_data_frame)
    print(base_data_frame.dtypes)


def plot_bollinger(base_data_frame, walor):
    ax = base_data_frame[['Zamkniecie', 'U-band', 'L-band']].plot(title=walor)
    ax.fill_between(base_data_frame.index, base_data_frame['L-band'], base_data_frame['U-band'], color='#5F9F9F',
                    alpha=0.20)
    ax.set_xlabel('Date')
    ax.set_ylabel('Kurs')
    ax.grid()
    plt.show(block=True)


def get_data_range_of_currency(currency, start_date, end_date):
    try:
        url = f'http://api.nbp.pl/api/' \
              f'exchangerates/rates/a/' \
              f'{currency}/' \
              f'{start_date}/' \
              f'{end_date}' \
              f'?format=json'
        response = requests.get(url)

    except HTTPError as http_error:
        print(f'HTTP error: {http_error}')
    except Exception as e:
        print(f'Other exception: {e}')
    else:
        if response.status_code == 200:
            return json.dumps(response.json(), indent=4, sort_keys=True)


def get_data_range_of_gold(start_date, end_date):
    try:
        url = f'http://api.nbp.pl/api/' \
              f'cenyzlota/' \
              f'{start_date}/' \
              f'{end_date}' \
              f'?format=json'
        response = requests.get(url)
        # print(url)
    except HTTPError as http_error:
        print(f'HTTP error: {http_error}')
    except Exception as e:
        print(f'Other exception: {e}')
    else:
        if response.status_code == 200:
            return json.dumps(response.json(), indent=4, sort_keys=True)


def get_all_nbp(date_set, to_csv=True, csv_mode='a', to_database=False, data_base_mode='append'):
    df_currency = pd.DataFrame(columns=['effectiveDate', 'mid', 'no', 'code'])
    df_gold = pd.DataFrame(columns=['data', 'cena'])
    currency_set = ['USD', 'GBP', 'EUR', 'CHF', 'JPY']
    # print(date_set)
    for start, end in date_set:
        print('dekodowanie', start, end, 'gold')
        jsongold = json.loads(get_data_range_of_gold(start, end))
        for dgold in jsongold:
            dictgold = dict(dgold)
            tmpgold = pd.DataFrame.from_dict(dictgold, orient='index')
            tmpgold = tmpgold.transpose()
            df_gold = pd.concat([df_gold, tmpgold])
    for currency in currency_set:
        for start, end in date_set:
            jsonNBP = json.loads(get_data_range_of_currency(currency, start, end))
            print("dekodowanie", start, end, currency)
            Rrates = (jsonNBP['rates'])

            for RatesDict in Rrates:
                dRatesDict = dict(RatesDict)
                tmpdf = pd.DataFrame.from_dict(dRatesDict, orient='index')
                tmpdf = tmpdf.transpose()
                tmpdf['mid'] = tmpdf['mid'].astype(float)
                tmpdf['code'] = currency
                df_currency = pd.concat([df_currency, tmpdf])

    df_gold = df_gold[['data', 'cena']]
    print(df_gold)
    if to_csv:
        df_gold.to_csv("gold.csv", mode=csv_mode, encoding="utf-8")
        df_currency.to_csv("currency.csv", mode=csv_mode, encoding="utf-8")
        print("to_csv done")

    if to_database:
        engine = sqlalchemy.create_engine('mssql+pymssql://adminuser:TjmnhdMySQL1!@pwserver2.database.windows.net:'
                                          '1433/PWdatabase')
        df_gold.to_sql('gold', index=False, if_exists=data_base_mode, con=engine)
        df_currency.to_sql('Currency', index=False, if_exists=data_base_mode, con=engine)


ex_walor_list = get_symbol_list('gielda2.csv', 'WALOR')
all_date_set = [['2018-01-01', '2018-12-31'],
                ['2019-01-01', '2019-12-31'],
                ['2020-01-01', '2020-12-31'],
                ['2021-01-01', '2021-12-31'],
                ['2022-01-01', '2022-12-31']]

# date_set = [['2022-12-30', '2022-12-30']]
get_all_stooq(all_date_set, ex_walor_list, to_csv=True, csv_mode='w', to_database=False, data_base_mode='append')
get_all_nbp(all_date_set, to_csv=True, csv_mode='w', to_database=False, data_base_mode='replace')
