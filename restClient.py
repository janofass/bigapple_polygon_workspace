import datetime
from polygon import RESTClient
import psycopg2
import time


def ts_to_datetime(ts) -> str:
    return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M')

def setupDBConnection(hostname,username,password,database):
    return psycopg2.connect( host=hostname, user=username, password=password, dbname=database )


def populateHistoricalData(key,cursor,from_,to,symbols):
    sql = """INSERT INTO public."TICK_SENT"(symbol,volume,volume_weighted_average_price,open_price,close_price,high_price,low_price,start_time) VALUES ('{}', {}, {}, {}, {}, {}, {}, {});"""
    with RESTClient(key) as client:
        for symbol in symbols: 
            resp = client.stocks_equities_aggregates(symbol, 1, "minute", from_, to, unadjusted=False)
            print(f"Minute aggregates for {resp.ticker} between {from_} and {to}.")
            for result in resp.results:
                print(result)
                insertStatement = sql.format(symbol,result['v'],result['vw'],result['o'],result['c'],result['h'],result['l'],result['t'])
                cursor.execute(insertStatement)
        

def main(key,cursor,year,symbols):
    selectSQL = """SELECT YEAR,MONTH,DAY,ID From public."TRADING_CALENDAR" Where Year = {} AND Month = 01 AND DAY = 02 AND TO_DATE(YEAR||'-'||MONTH||'-'||DAY, 'YYYY-MM-DD') < now()"""
    selectStatement = selectSQL.format(year)
    cursor = conn.cursor()
    cursor.execute (selectStatement)
    results = cursor.fetchall()
    for row in results:
        date = "{:04d}-{:02d}-{:02d}".format(row[0],row[1],row[2])
        try:
            populateHistoricalData(key=key,cursor=cursor,from_=date,to=date,symbols=symbols)
            conn.commit
        except:
            print("Oops!")
            time.sleep(90)


if __name__ == "__main__":
    print("Main Method Called. Starting Python Script to download Historical Data ....")
    conn = setupDBConnection(hostname="localhost",username="faisal",password="faisal0!",database="application")
    main(key="jbukxOh7HOCP9QO7DdpBxLhd8pbl88bQ",cursor = conn.cursor(),year=2020,symbols=['AAPL','MSFT','NFLX','TWTR','NVDA','LK','SBUX'])
    conn.commit()
    print("Transaction Committed")