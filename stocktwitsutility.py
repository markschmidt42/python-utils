"""
    EXTRACT DATA FROM STOCKTWITS API
    WORKAROUND RATE LIMITS USING PROXY
    CHANGED WORKAROUND METHOD TO USING MULTIPLE ACCESS KEYS
"""
import csv
import json
import os
import time
import requests
import pendulum

def get_stock_stream(symbol, from_date, DEBUG=False):
  
  FIELDS = ['message_id', 'symbol', 'message', 'datetime', 'user', 'user_followers', 'sentiment', 'like_count', 'reply_count', 'symbol_count', 'watchlist_count']
  SYMBOL = symbol
  FILE_NAME = 'stocktwits_' + SYMBOL + '.csv'
  token = 0
  access_token = ['', 'access_token=32a3552d31b92be5d2a3d282ca3a864f96e95818&',
                  'access_token=44ae93a5279092f7804a0ee04753252cbf2ddfee&',
                  'access_token=990183ef04060336a46a80aa287f774a9d604f9c&']
  
  
  from_date = pendulum.parse(from_date)
  from_date = from_date.subtract(days=1)
  if DEBUG: print("Getting tweets for", symbol, ". from: ", from_date)
  
  file = open(FILE_NAME, 'a', newline='', encoding='utf-8')
  # DETERMINE WHERE TO START IF RESUMING SCRIPT
  if os.stat(FILE_NAME).st_size == 0:
      # OPEN FILE IN APPEND MODE AND WRITE HEADERS TO FILE
      last_message_id = None
      csvfile = csv.DictWriter(file, FIELDS)
      csvfile.writeheader()
  else:
      # FIRST EXTRACT LAST MESSAGE ID THEN OPEN FILE IN APPEND MODE WITHOUT WRITING HEADERS
      file = open(FILE_NAME, 'r', newline='', encoding='utf-8')
      csvfile = csv.DictReader((line.replace('\0', '') for line in file))
      data = list(csvfile)
      data = data[-1]
      last_message_id = data['message_id']
      file.close()
      file = open(FILE_NAME, 'a', newline='', encoding='utf-8')
      csvfile = csv.DictWriter(file, FIELDS)

  # req_proxy = RequestProxy()

  stocktwit_url = "https://api.stocktwits.com/api/2/streams/symbol/" + SYMBOL + ".json?" + access_token[token]
  if last_message_id is not None:
      stocktwit_url += "max=" + str(last_message_id)

  api_hits = 0
  continue_procesing = True
  while continue_procesing:
      # response = req_proxy.generate_proxied_request(stocktwit_url)
      try:
          print(stocktwit_url)
          response = requests.get(stocktwit_url)
      except Exception:
          response = None

      if response is not None:

          if response.status_code == 429:
              print("###############")
              print("REQUEST IP RATE LIMITED FOR {} seconds !!!".format(
                  int(response.headers['X-RateLimit-Reset']) - int(time.time())))

          if not response.status_code == 200:
              stocktwit_url = "https://api.stocktwits.com/api/2/streams/symbol/" + SYMBOL + ".json?" + access_token[
                  token] + "max=" + str(
                  last_message_id)
              token = (token + 1) % (len(access_token))
              continue

          api_hits += 1
          response = json.loads(response.text)
          last_message_id = response['cursor']['max']

          # WRITE DATA TO CSV FILE
          first = True
          for message in response['messages']:
              dt = pendulum.parse(message['created_at'])
              #print(dt, from_date, dt.diff(from_date, False).in_days())
              
              # if we hit our date range, stop
              if dt.diff(from_date, False).in_days() > 0:
                continue_procesing = False
                break
                
              # PREPARE OBJECT TO WRITE IN CSV FILE
              #if DEBUG: print(message)

              if first:
                print(message['created_at'])
                first = False
                
              obj = {}
              obj['message_id'] = message['id']
              obj['symbol'] = SYMBOL
              obj['message'] = message['body']
              obj['datetime'] = message['created_at']
              obj['user'] = message['user']['id']
              obj['user_followers'] = message['user']['followers']
              obj['symbol_count'] = len(message['symbols'])

              if 'conversation' in message:
                obj['reply_count'] = message['conversation']['replies']
              
              if 'likes' in message:
                obj['like_count'] = message['likes']['total']
              
              if 'entities' in message:
                if 'sentiment' in message['entities']:
                  if message['entities']['sentiment'] and 'basic' in message['entities']['sentiment']:
                    obj['sentiment'] = message['entities']['sentiment']['basic']

              for sym in message['symbols']:
                if sym['symbol'] == SYMBOL:
                  obj['watchlist_count'] = sym['watchlist_count']
                  break

              csvfile.writerow(obj)
              file.flush()

          if DEBUG: print("API HITS TILL NOW = {}".format(api_hits))

          # NO MORE MESSAGES
          if not response['messages']:
              break

      # ADD MAX ARGUMENT TO GET OLDER MESSAGES
      stocktwit_url = "https://api.stocktwits.com/api/2/streams/symbol/" + SYMBOL + ".json?" + access_token[
          token] + "max=" + str(last_message_id)
      token = (token + 1) % (len(access_token))

  file.close()

  return FILE_NAME
