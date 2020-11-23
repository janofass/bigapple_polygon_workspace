import datetime

import psycopg2
import pytz


def local_datetime_to_utc(year,month,day,hour,minute,second,epoch):
    localTZone = pytz.timezone ("US/Eastern")
    utcTZone = pytz.utc
    naive = datetime.datetime.strptime ("{}-{}-{} {}:{}:{}".format(year,month,day,hour,minute,second), "%Y-%m-%d %H:%M:%S")
    local_dt = localTZone.localize(naive, is_dst=None)
    epoch_dt = utcTZone.localize(epoch)
    return (local_dt.astimezone(pytz.utc) - epoch_dt).total_seconds()*1000

def setupDBConnection(hostname,username,password,database):
    return psycopg2.connect( host=hostname, user=username, password=password, dbname=database )

def main(cursor):
    selectSQL = """SELECT YEAR,MONTH,DAY,ID From public."TRADING_CALENDAR" Where Year = {}"""
    updateSQL = """UPDATE public."TRADING_CALENDAR" SET START_TIMESTAMP = {}, END_TIMESTAMP = {} Where Id = {}"""
    selectStatement = selectSQL.format('2020')
    cursor = conn.cursor()
    cursor.execute (selectStatement)
    epoch = datetime.datetime(1970, 1, 1)
    results = cursor.fetchall()
    for row in results:
        startDateTime = local_datetime_to_utc(row[0],row[1],row[2],'09','30','00',epoch)
        endDateTime =   local_datetime_to_utc(row[0],row[1],row[2],'16','00','00',epoch)
        updateStatement = updateSQL.format(startDateTime,endDateTime,row[3])
        print("Stating at {}, finishing at {}".format(startDateTime,endDateTime))
        cursor.execute(updateStatement)


if __name__ == "__main__":
    print("Main Method Called. To populate Calendar Table")
    conn = setupDBConnection(hostname="localhost",username="faisal",password="faisal0!",database="application")
    main(cursor = conn.cursor())
    conn.commit()
    print("Transaction Committed")
