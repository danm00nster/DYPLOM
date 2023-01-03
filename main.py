import matplotlib.pyplot as plt
import pandas as pd
import wget
import os
import sqlalchemy
import requests
import json
from requests.exceptions import HTTPError


def add_bollinger_band(inputDataFrame: pd.DataFrame, dataColumn):
    data_frame = inputDataFrame[[dataColumn]]
    # odhylenie standardowe dla 20 dni
    std_dev = data_frame.rolling(window=20).std()
    # średnia krącząca dla 20 dni
    sma = data_frame.rolling(window=20).mean()
    lower_band = sma - 2 * std_dev
    lower_band = lower_band.rename(columns={dataColumn: "L-band"})
    upper_band = sma + 2 * std_dev
    upper_band = upper_band.rename(columns={dataColumn: "U-band"})
    data_frame = data_frame.join(upper_band).join(lower_band)
    data_frame.drop([dataColumn], axis=1, inplace=True)
    data_frame = data_frame.round(2)
    outputDataFrame = inputDataFrame.join(data_frame)
    return outputDataFrame


def get_quotes(symbol, DateSET):
    FILENAME = symbol + ".txt"
    output_data_frame = pd.DataFrame()
    print(DateSET)
    for START_DATE, END_DATE in DateSET:
        print(f'>{START_DATE}< >{END_DATE}<')
        if os.path.exists(FILENAME):
            os.remove(FILENAME)
        url = "https://stooq.pl/q/d/l/?s={0}&d1={1}&d2={2}&i=d".format(symbol, START_DATE.replace("-", ""),
                                                                       END_DATE.replace("-", ""))
        wget.download(url, FILENAME)
        try:
            DF_notowania = pd.read_csv(FILENAME, index_col='Data', parse_dates=True)
            DF_notowania['WALOR'] = symbol
            DF_notowania.dropna(inplace=True)
            if len(output_data_frame.index) == 0:
                output_data_frame = DF_notowania
            else:
                output_data_frame = pd.concat([output_data_frame, DF_notowania])
        except:
            print("brak danych za okres ", symbol, START_DATE, END_DATE)
            output_data_frame = pd.DataFrame()
    return output_data_frame


def get_symbol_list(FILENAME, column):
    DF_symbolList = pd.read_csv(FILENAME, sep=';')
    print(DF_symbolList)
    symbolList = DF_symbolList[column].to_list()
    return symbolList


def get_all_stooq(DateSET, to_csv=True, csv_mode='a', to_database=False, data_base_mode='append'):
    """ pobiera cały zestaw danych ze stooq
    to_csv - True(domyślnie)/False - zapis do pliku csv
    csv_mode - a= append (domyślnie), w=overwrite
    to_database_ True/False(domyślnie) - zapis do bazy danych
    data_base_mode - opcje dataframe.to_sql append= wstawia wiersze(domyślnie), replace=drop table przed zapisem do bazy
    DateSET - zestaw dat start, end"""
    if os.path.exists('kursy.csv'):
        os.remove('kursy.csv')

    WALOR_LIST = get_symbol_list('gielda.csv', 'WALOR')
    base_data_frame = pd.DataFrame()
    for WALOR in WALOR_LIST:
        NAZWA_K = WALOR
        base_data_frame = pd.DataFrame()
        base_data_frame = get_quotes(NAZWA_K, DateSET)
        base_data_frame = add_bollinger_band(base_data_frame, 'Zamkniecie')
        if to_csv:
            base_data_frame.to_csv("kursy.csv", mode=csv_mode, encoding="utf-8")
        # działa połączenie do bazy

        if to_database:
            engine = sqlalchemy.create_engine('mssql+pymssql://adminuser:TjmnhdMySQL1!@pwserver2.database.windows.net:'
                                              '1433/PWdatabase')
            # działa - zapis do bazy
            base_data_frame.to_sql('notowaniaGPW', if_exists=data_base_mode, con=engine)

    print('Base data frame')
    print(base_data_frame)
    print(base_data_frame.dtypes)


def plot_bollinger(base_data_frame, WALOR):
    ax = base_data_frame[['Zamkniecie', 'U-band', 'L-band']].plot(title=WALOR)
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
        # print(url)
    except HTTPError as http_error:
        print(f'HTTP error: {http_error}')
    except Exception as e:
        print(f'Other exception: {e}')
    else:
        if response.status_code == 200:
            return json.dumps(response.json(), indent=4, sort_keys=True)


def get_data_range_of_GOLD(start_date, end_date):
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


def get_all_nbp(DateSET, to_csv=True, csv_mode='a', to_database=False, data_base_mode='append'):
    dfCurrency = pd.DataFrame(columns=['effectiveDate', 'mid', 'no', 'code'])
    dfGOLD = pd.DataFrame(columns=['data', 'cena'])
    currencySET = ['USD', 'GBP', 'EUR', 'CHF']
    # print(DateSET)
    for start, end in DateSET:
        print('dekodowanie', start, end, 'GOLD')
        jsonGOLD = json.loads(get_data_range_of_GOLD(start, end))
        for dGOLD in jsonGOLD:
            dictGOLD = dict(dGOLD)
            tmpGOLD = pd.DataFrame.from_dict(dictGOLD, orient='index')
            tmpGOLD = tmpGOLD.transpose()
            dfGOLD = pd.concat([dfGOLD, tmpGOLD])
    for currency in currencySET:
        for start, end in DateSET:
            jsonNBP = json.loads(get_data_range_of_currency(currency, start, end))
            print("dekodowanie", start, end, currency)
            Rrates = (jsonNBP['rates'])

            for RatesDict in Rrates:
                dRatesDict = dict(RatesDict)
                tmpdf = pd.DataFrame.from_dict(dRatesDict, orient='index')
                tmpdf = tmpdf.transpose()
                tmpdf['mid'] = tmpdf['mid'].astype(float)
                tmpdf['code'] = currency
                dfCurrency = pd.concat([dfCurrency, tmpdf])

    dfGOLD = dfGOLD[['data', 'cena']]
    print(dfGOLD)
    if to_csv:
        dfGOLD.to_csv("gold.csv", mode=csv_mode, encoding="utf-8")
        dfCurrency.to_csv("currency.csv", mode=csv_mode, encoding="utf-8")
        print("to_csv done")
    # działa połączenie do bazy
    if to_database:
        engine = sqlalchemy.create_engine('mssql+pymssql://adminuser:TjmnhdMySQL1!@pwserver2.database.windows.net:'
                                          '1433/PWdatabase')
        # działa - zapis do bazy
        dfGOLD.to_sql('GOLD', index=False, if_exists=data_base_mode, con=engine)
        dfCurrency.to_sql('Currency', index=False, if_exists=data_base_mode, con=engine)


DateSET = [['2018-01-01', '2018-12-31'],
           ['2019-01-01', '2019-12-31'],
           ['2020-01-01', '2020-12-31'],
           ['2021-01-01', '2021-12-31'],
           ['2022-01-01', '2022-12-31']]

# DateSET = [['2022-12-30', '2022-12-30']]
get_all_stooq(DateSET, to_csv=True, csv_mode='w', to_database=True, data_base_mode='append')
get_all_nbp(DateSET,to_csv=True, csv_mode='w', to_database=True, data_base_mode='append')