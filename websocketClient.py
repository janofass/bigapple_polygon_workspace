import websocket
import psycopg2
import json
import datetime

def ts_to_datetime(ts) -> str:
    return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M')

connection = None
sql = """INSERT INTO public."TICK_RECEIVED"(Calendar_id,symbol, volume, volume_weighted_average_price, open_price, close_price, high_price, low_price, start_time, entry_time,counter)
	     SELECT {}, '{}', {}, {}, {}, {}, {}, {}, {}, '{}', count(*) + 1 From public."TICK_RECEIVED" Where Calendar_id = {} AND symbol = '{}';"""

calculateSql  = """SELECT symbol 		 AS SYMBOL,
						max(rank) 		 AS COUNTER,
						avg(close_price) AS AVG_CLOSE
				FROM 
					(SELECT close_price, start_time,counter,symbol, rank() OVER (PARTITION BY Symbol ORDER BY start_time DESC)
					 FROM public."TICK_RECEIVED" 
					 WHERE Calendar_id={}) TICKS
				WHERE rank <={}
				GROUP BY symbol 
				ORDER BY symbol """

martixSql = """ INSERT INTO public."DECISION_MATRIX"(calendar_id, timestamp_value, symbol, volume, volume_weighted_average_price, open_price, close_price, high_price, low_price,AVG_CLOSE_10,AVG_CLOSE_15,AVG_CLOSE_20,AVG_CLOSE_50)
				SELECT {},{},MA10.symbol,{},{},{},{},{},{}, 
				                    CASE WHEN MA10.Counter = 10 THEN MA10.avg_closing_price ELSE NULL END As MA10ClsoingAvg, 
									CASE WHEN MA15.Counter = 15 THEN MA15.avg_closing_price ELSE NULL END As MA15ClsoingAvg,
									CASE WHEN MA20.Counter = 20 THEN MA20.avg_closing_price ELSE NULL END As MA20ClsoingAvg,
									CASE WHEN MA50.Counter = 50 THEN MA50.avg_closing_price ELSE NULL END As MA50ClsoingAvg
				FROM CalculateClosingAverage('{}',{}, 10) AS MA10,
					 CalculateClosingAverage('{}',{}, 15) AS MA15,
					 CalculateClosingAverage('{}',{}, 20) AS MA20,
					 CalculateClosingAverage('{}',{}, 50) AS MA50
				WHERE 
					MA10.symbol = MA15.symbol AND
					MA15.symbol = MA20.symbol AND
					MA20.symbol = MA50.symbol AND
					MA10.Counter >= 10;"""

timeStampSql = """ SELECT id,start_timestamp,end_timestamp  From public."TRADING_CALENDAR" Where {} between START_TIMESTAMP AND END_TIMESTAMP;"""

def setupDBConnection(hostname,username,password,database):
    return psycopg2.connect( host=hostname, user=username, password=password, dbname=database )


def on_message(ws, message):
	print("Message received from Server ==", message)
	if message.startswith("{"):
		record = json.loads(message)
		cursor = connection.cursor()
		currentTimeStamp = record['s']
		symbol = record['sym']
		timeStampSelectStatement = timeStampSql.format(currentTimeStamp,)
		cursor.execute(timeStampSelectStatement)
		print('Time stamp Query executed')
		timeStampResult = cursor.fetchone()
		if timeStampResult is not None:
			print('Timestamp {} is Accepted'.format(currentTimeStamp))
			calendarId = timeStampResult[0]
			print('Calendar Id found to be {}'.format(calendarId))
			martixStatement = martixSql.format(calendarId,currentTimeStamp,record['v'],record['vw'],record['o'],record['c'],record['h'],record['l'],symbol,calendarId,symbol,calendarId,symbol,calendarId,symbol,calendarId)
			cursor.execute(martixStatement)
			insertStatement = sql.format(calendarId,symbol,record['v'],record['vw'],record['o'],record['c'],record['h'],record['l'],currentTimeStamp,ts_to_datetime(currentTimeStamp),calendarId,symbol)
			cursor.execute(insertStatement)
			connection.commit()

def on_error(ws, error):
	print(error)

def on_close(ws):
	print("### closed ###")

def on_open(ws):
	ws.send('{"action":"auth","params":"jbukxOh7HOCP9QO7DdpBxLhd8pbl88bQ"}')
	ws.send('{"action":"subscribe","params":"T.MSFT,T.AAPL,T.AMD,T.NVDA"}')

if __name__ == "__main__":
	websocket.enableTrace(True)#conn = setupDBConnection(hostname="localhost",username="faisal",password="faisal0!",database="application")
	connection = setupDBConnection(hostname="localhost",username="faisal",password="faisal0!",database="application")
	ws = websocket.WebSocketApp("ws://127.0.0.1:13254/", #"wss://socket.polygon.io/stocks",
							  on_message = on_message,
							  on_error = on_error,
							  on_close = on_close)						  
	ws.on_open = on_open
	ws.run_forever()