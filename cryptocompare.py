#!/usr/bin/python2
import sys
import time
import socket
from decimal import Decimal
import urllib2
import json
from pybitx.api import BitX
import multiprocessing
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read('cryptocompare.properties')

CARBON_SERVER = config.get('CARBON', 'CARBON_SERVER')
CARBON_PORT = int(config.get('CARBON', 'CARBON_PORT'))

LUNO_APIKEY = config.get('KEYS', 'LUNO_APIKEY')
LUNO_SECRET = config.get('KEYS', 'LUNO_SECRET')

SECONDS_INTERVAL = int(config.get('TIMER', 'SECONDS_INTERVAL'))

def getMetric(exchange,currency_pair,metric,value,time):
    text = 'stats.crypto.exchanges.{0}.{1}.{2}.gauge {3:.6f} {4}\n'.format(exchange,currency_pair,metric,value,time)
    # print text
    return text

def openGraphiteSocket():

    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))
    return sock

def publishLunoTicker():

    sock = openGraphiteSocket()

    exchange = 'luno'
    api = BitX(LUNO_APIKEY, LUNO_SECRET, {})
    dict = api.get_ticker()

    ts = int(int(dict['timestamp'])/1000)
    print '{0} Luno Last:{1:.6f} to graphite'.format(ts,float(dict['last_trade']))

    sock.sendall(getMetric(exchange, dict['pair'],'last_trade',float(dict['last_trade']), ts))
    sock.sendall(getMetric(exchange, dict['pair'],'bid',float(dict['bid']), ts))
    sock.sendall(getMetric(exchange, dict['pair'],'vol_24',float(dict['rolling_24_hour_volume']), ts))
    sock.sendall(getMetric(exchange, dict['pair'],'ask',float(dict['ask']), ts))

    #trades = api.get_trades()
    #oldest_time = ts - SECONDS_INTERFAL
    #for trade in trades['trades']:
    #    tt = int(int(trade['timestamp'])/1000)
    #    if (tt >= oldest_time):
    #       print 'Trade Volume {0:.6f}'.format(float(trade['volume']))

    sock.close()

def publishKrakenTicker():
    sock = openGraphiteSocket()

    exchange = 'kraken'
    pair = 'XXBTZUSD'

    opener = urllib2.build_opener()

    opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36')]

    ts = int(time.time())
    response = opener.open("https://api.kraken.com/0/public/Ticker?pair="+pair)
    r=json.load(response)
    t = r['result'][pair]

    # print t

    ask = float(t['a'][0])
    ask_volume = Decimal(t['a'][2])
    ask_whole_volume = Decimal(t['a'][1])

    bid = Decimal(t['b'][0])
    bid_volume = Decimal(t['b'][2])
    bid_whole_volume = Decimal(t['b'][1])

    last_trade = float(t['c'][0])
    last_trade_volume = float(t['c'][1])

    vol_today = Decimal(t['v'][0])
    vol_24 = Decimal(t['v'][1])
    vol_weighted_avg_price = Decimal(t['p'][0])

    low_today = Decimal(t['l'][0])
    low_24 = Decimal(t['l'][1])
    high_today = Decimal(t['h'][0])
    high_24 = Decimal(t['h'][1])
    open_price = Decimal(t['o'])

    print '{0} Krkn Last:{1:.6f} to graphite'.format(ts,last_trade)

    sock.sendall(getMetric(exchange, pair, 'last_trade',last_trade, ts))
    sock.sendall(getMetric(exchange, pair, 'bid', bid, ts))
    sock.sendall(getMetric(exchange, pair, 'vol_24', vol_24, ts))
    sock.sendall(getMetric(exchange, pair, 'ask', ask, ts))

    sock.sendall(getMetric(exchange, pair, 'bid_lot_vol', bid_volume, ts))
    sock.sendall(getMetric(exchange, pair, 'bid_whole_lot_vol', bid_whole_volume, ts))

    sock.sendall(getMetric(exchange, pair, 'ask_lot_vol', ask_volume, ts))
    sock.sendall(getMetric(exchange, pair, 'ask_whole_lot_vol', ask_whole_volume, ts))

    sock.sendall(getMetric(exchange, pair, 'last_trade_volume', last_trade_volume, ts))
    sock.sendall(getMetric(exchange, pair, 'vol_today', vol_today, ts))
    sock.sendall(getMetric(exchange, pair, 'vol_weighted_avg_price', vol_weighted_avg_price, ts))

    sock.sendall(getMetric(exchange, pair, 'low_today', low_today, ts))
    sock.sendall(getMetric(exchange, pair, 'low_24', low_24, ts))
    sock.sendall(getMetric(exchange, pair, 'high_today', high_today, ts))
    sock.sendall(getMetric(exchange, pair, 'high_24', high_24, ts))
    sock.sendall(getMetric(exchange, pair, 'open_price', open_price, ts))

    sock.close()


def publishBitfinexTicker():
    sock = openGraphiteSocket()

    exchange = 'bitfinex'
    pair = 'BTCUSD'

    opener = urllib2.build_opener()

    opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36')]

    response = opener.open("https://api.bitfinex.com/v1/pubticker/"+pair.lower())
    t=json.load(response)

    # print t

    ts = int(float(t['timestamp']))

    print '{0} Bfnx Last:{1:.6f} to graphite'.format(ts, float(t['last_price']))

    sock.sendall(getMetric(exchange, pair, 'last_trade', float(t['last_price']), ts))
    sock.sendall(getMetric(exchange, pair, 'bid', float(t['bid']), ts))
    sock.sendall(getMetric(exchange, pair, 'vol_24', float(t['volume']), ts))
    sock.sendall(getMetric(exchange, pair, 'ask', float(t['ask']), ts))

    sock.sendall(getMetric(exchange, pair, 'mid', float(t['mid']), ts))

    sock.sendall(getMetric(exchange, pair, 'low_24', float(t['low']), ts))
    sock.sendall(getMetric(exchange, pair, 'high_24', float(t['high']), ts))

    sock.close()

def publishBTCChinaTicker():
    sock = openGraphiteSocket()

    exchange = 'btcchina'
    pair = 'BTCCNY'

    opener = urllib2.build_opener()

    opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36')]

    response = opener.open("https://data.btcchina.com/data/ticker?market="+pair.lower())
    r = json.load(response)
    t = r['ticker']

    # print t

    ts = int(t['date'])

    print '{0} BChn Last:{1:.6f} to graphite'.format(ts, float(t['last']))

    sock.sendall(getMetric(exchange, pair, 'last_trade', float(t['last']), ts))
    sock.sendall(getMetric(exchange, pair, 'bid', float(t['buy']), ts))
    sock.sendall(getMetric(exchange, pair, 'vol_24', float(t['vol']), ts))
    sock.sendall(getMetric(exchange, pair, 'ask', float(t['sell']), ts))

    sock.sendall(getMetric(exchange, pair, 'low_24', float(t['low']), ts))
    sock.sendall(getMetric(exchange, pair, 'high_24', float(t['high']), ts))

    sock.sendall(getMetric(exchange, pair, 'avg_24', float(t['vwap']), ts))
    sock.sendall(getMetric(exchange, pair, 'prev_close', float(t['prev_close']), ts))
    sock.sendall(getMetric(exchange, pair, 'open_price', float(t['open']), ts))

    sock.close()


def publishTickers():

    luno = multiprocessing.Process(target=publishLunoTicker)
    kraken = multiprocessing.Process(target=publishKrakenTicker)
    bitfinex = multiprocessing.Process(target=publishBitfinexTicker)
    btcchina = multiprocessing.Process(target=publishBTCChinaTicker)

    luno.start()
    kraken.start()
    bitfinex.start()
    btcchina.start()

    timeoutSeconds = 30

    luno.join(timeoutSeconds)
    kraken.join(timeoutSeconds)
    bitfinex.join(timeoutSeconds)
    btcchina.join(timeoutSeconds)

while True:
        publishTickers()
        time.sleep(SECONDS_INTERVAL)
