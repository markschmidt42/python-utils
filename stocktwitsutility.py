"""
    EXTRACT DATA FROM STOCKTWITS API
    WORKAROUND RATE LIMITS USING PROXY
    CHANGED WORKAROUND METHOD TO USING MULTIPLE ACCESS KEYS
"""
import csv
import sys
import json
import os
import time
import math
import requests
import pendulum

def get_record(filepath, which='last', column_index=0):
    record = None
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) #skip header
        try:
          if which == 'first':
              record = min(reader, key=lambda column: int(column[column_index]))
          elif which == 'last':
              record = max(reader, key=lambda column: int(column[column_index]))
        except:
          pass
    return record


def get_stock_stream(symbol, from_date, verbosity=1):
  LOG_INFO = 1
  LOG_DEBUG = 10
  WALK_MODE = 'forward'
  IDX_MSG_ID   = 0
  IDX_DATETIME = 3
  FIELDS = ['message_id', 'symbol', 'message', 'datetime', 'user', 'user_followers', 'sentiment', 'like_count', 'reply_count', 'symbol_count', 'watchlist_count']
  SYMBOL = symbol
  FILE_NAME = 'stocktwits_' + SYMBOL + '.csv'
  token = 0
  access_token = [
    '', 
    'access_token=759b619b9d2def2226e80a896f17599663e503bb&', # mark's test app: https://api.stocktwits.com/developers/apps/4805
    'access_token=f99e9de127e08b762a181c774bfae836b0292d84&', # mark's test app: https://api.stocktwits.com/developers/apps/4806
    'access_token=551b250b1b117557bc267c8affe51f893d8fd461&', # mark's test app: https://api.stocktwits.com/developers/apps/4807
    'access_token=32a3552d31b92be5d2a3d282ca3a864f96e95818&',
    'access_token=44ae93a5279092f7804a0ee04753252cbf2ddfee&',
    'access_token=990183ef04060336a46a80aa287f774a9d604f9c&',
  ]

  # duplicate the accesstokens (except the public one), # 200 public, 400 private. You can use the private x2
  access_token.extend(access_token[1:]) # duplicate, skip first
  
  from_date = pendulum.parse(from_date)
  from_date = from_date.subtract(days=1)
  if verbosity >= LOG_INFO: print("Getting tweets for", symbol, ". from: ", from_date)
  
  file = open(FILE_NAME, 'a', newline='', encoding='utf-8')
  csvfile = csv.DictWriter(file, FIELDS)

  def get_first_last_info_from_csv():
    first_message_id = None
    last_message_id = None
    first_message = get_record(FILE_NAME, 'first')
    last_message  = get_record(FILE_NAME, 'last')
    if first_message:
      first_message_id = first_message[IDX_MSG_ID]
      last_message_id  = last_message[IDX_MSG_ID]
      if verbosity >= LOG_DEBUG: print('first_message_id', first_message_id)
      if verbosity >= LOG_DEBUG: print(first_message)
      if verbosity >= LOG_DEBUG: print('last_message_id', last_message_id)
      if verbosity >= LOG_DEBUG: print(last_message)
      if verbosity >= LOG_DEBUG: print('FIRST TO LAST: ', first_message_id, last_message_id)
    return first_message_id, last_message_id, first_message, last_message

  def create_obj(message):
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

    return obj
  # def create_obj ############################################

  def process_msg(message):
    obj = create_obj(message)

    csvfile.writerow(obj)
    file.flush()
  #############################################################

  def add_direction(first_id, last_id):
    if verbosity >= LOG_DEBUG: print('FIRST TO LAST: ', 'first_id', first_id, '               last_id', last_id)
    if WALK_MODE == 'backward':
      if first_id:
        return "max=" + str(int(first_id)-1)
      else:
        return ''
    else:
      if last_id:
        return "since=" + str(int(last_id)+1)
      else:
        return ''
  #############################################################

  # DETERMINE WHERE TO START IF RESUMING SCRIPT
  last_message_id = None
  first_message_id = None
  WALK_MODE = 'backward'
  if os.stat(FILE_NAME).st_size == 0:
    # OPEN FILE IN APPEND MODE AND WRITE HEADERS TO FILE
    csvfile.writeheader()
  else:
    # FIRST EXTRACT LAST MESSAGE ID THEN OPEN FILE IN APPEND MODE WITHOUT WRITING HEADERS
    first_message_id, last_message_id, first_message, last_message = get_first_last_info_from_csv()
    if first_message:
      earliest_datetime = pendulum.parse(first_message[IDX_DATETIME])
      # if we have all the old data... let's walk forward only
      if (from_date.diff(earliest_datetime, False).in_days() <= 0):
        WALK_MODE = 'forward'
      if verbosity >= LOG_DEBUG: print(first_message[IDX_DATETIME], earliest_datetime, from_date, from_date.diff(earliest_datetime, False).in_days())
  
  #sys.exit()

  if verbosity >= LOG_DEBUG: print(f'last_message_id: {last_message_id}')

  # req_proxy = RequestProxy()

  # stocktwit_url = "https://api.stocktwits.com/api/2/streams/symbol/" + SYMBOL + ".json?" + access_token[token]
  # if last_message_id is not None:
  #   stocktwit_url += add_direction(first_message_id, last_message_id)

  def add_token(token):
    return '?'+ access_token[token]

  def get_wait_time():
    # https://api.stocktwits.com/developers/docs/rate_limiting
    LIMIT_HITS_PER_HOUR = 200 # 200 public, 400 private. Treat them all like public. I duplicated the privates, so they will get hit double
    hits_allowed_per_hour = (60.0 * 60.0) / (LIMIT_HITS_PER_HOUR * len(access_token)) # if we have 4 tokens, we can x 4 it
    if verbosity >= LOG_DEBUG: print('hits_allowed_per_hour', hits_allowed_per_hour)
    return hits_allowed_per_hour
    #return int(math.ceil(hits_allowed_per_hour))

  api_hits = 0
  continue_procesing = True
  while continue_procesing:
      # build the url
      stocktwit_url = "https://api.stocktwits.com/api/2/streams/symbol/" + SYMBOL + ".json"
      stocktwit_url += add_token(token)
      stocktwit_url += add_direction(first_message_id, last_message_id)
      token = (token + 1) % (len(access_token)) # next

      # response = req_proxy.generate_proxied_request(stocktwit_url)
      try:
          if verbosity >= LOG_DEBUG: print(f'WALK_MODE:     {WALK_MODE}')
          if verbosity >= LOG_DEBUG: print(f'stocktwit_url: {stocktwit_url}')
          if verbosity >= LOG_INFO:  print(f'{WALK_MODE},\t{first_message_id}\t{last_message_id}\t{stocktwit_url}')
          time.sleep(get_wait_time())
          response = requests.get(stocktwit_url)
      except Exception as ex:
          if verbosity >= LOG_DEBUG: print(f'EXCEPTION: {ex}')
          response = None

      if response is not None:

          if response.status_code == 429:
              print("###############")
              print("REQUEST IP RATE LIMITED FOR {} seconds !!!".format(
                  int(response.headers['X-RateLimit-Reset']) - int(time.time())))

          if not response.status_code == 200:
            print('RESPONSE.STATUS_CODE', response.status_code)
            continue_procesing = False
            continue

          api_hits += 1
          response = json.loads(response.text)
          if verbosity >= LOG_DEBUG: print(response['cursor'])
          first_message_id = response['cursor']['max']   # smallest, oldest
          last_message_id  = response['cursor']['since'] # largest, newest
          # print('PROCESS RESPONSE')
          # stocktwit_url += add_direction(first_message_id, last_message_id)

          # WRITE DATA TO CSV FILE
          first = True
          for message in response['messages']:
              dt = pendulum.parse(message['created_at'])
              if verbosity >= LOG_DEBUG: print(message['id'], dt, from_date, dt.diff(from_date, False).in_days())
              
              # if we hit our date range, stop
              if dt.diff(from_date, False).in_days() > 0:
                if verbosity >= LOG_DEBUG: print('WE HIT OUR DATE RANGE')
                continue_procesing = False
                break
                
              # PREPARE OBJECT TO WRITE IN CSV FILE
              #if verbosity >= LOG_DEBUG: print(message)

              if first:
                if verbosity >= LOG_INFO: print(message['id'], message['created_at'])
                first = False

              process_msg(message)                

          if verbosity >= LOG_DEBUG: print("API HITS TILL NOW = {}".format(api_hits))

          # NO MORE MESSAGES
          if not response['messages']:
            if verbosity >= LOG_DEBUG: print('NO MORE MESSAGES')
            
            if WALK_MODE == 'backward':
              # once we are done going all the way back.... go forward
              WALK_MODE = 'forward'
              first_message_id, last_message_id, _, _ = get_first_last_info_from_csv()
            else:
              continue_procesing = False
              break

      # # ADD MAX ARGUMENT TO GET OLDER MESSAGES
      # print('ADD MAX ARGUMENT TO GET OLDER MESSAGES')
      # stocktwit_url = "https://api.stocktwits.com/api/2/streams/symbol/" + SYMBOL + ".json?" + access_token[token]
      # stocktwit_url += add_direction(first_message_id, last_message_id)
      # token = (token + 1) % (len(access_token))

  file.close()

  return FILE_NAME
