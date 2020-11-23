import logging
from websocket_server import WebsocketServer
import psycopg2
import time


def new_client(client, server):
	print("New client connected and was given id %d" % client['id'])
	server.send_message_to_all("Hey all, a new client has joined us")

def message_received(client, server, message):
	print("Client(%d) said: %s" % (client['id'], message))
	if "subscribe" in message:
		send_tickers(symbol=message,server=server)

def setupDBConnection(hostname,username,password,database):
    return psycopg2.connect( host=hostname, user=username, password=password, dbname=database )

def send_tickers(symbol,server):
	conn = setupDBConnection(hostname="localhost",username="faisal",password="faisal0!",database="application")
	sql = """SELECT symbol, volume, volume_weighted_average_price, open_price, close_price, high_price, low_price, start_time FROM public."TICK_SENT" """
	cursor = conn.cursor()
	cursor.execute (sql)
	results = cursor.fetchall()
	print("Row Count = %s",len(results))
	for row in results:
		json = """{"ev":"AM", "sym":"%s", "v":%d, "av":0, "op":0, "vw":%d, "o":%d, "c":%d, "h":%d, "l":%d, "a":0, "s":%d, "e":0}""" % (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7])
		server.send_message_to_all(json)
		time.sleep(0.1)

	
server = WebsocketServer(13254, host='127.0.0.1')
server.set_fn_new_client(new_client)
server.set_fn_message_received(message_received)
server.run_forever()