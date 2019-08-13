from aiohttp import web
import json
import sys, traceback
import aiohttp
import asyncio
import time
import zlib
# For some environment variables
import os


# redis
import redis

# regex
import re

# MySQL
import pymysql, pymysqlpool
import pymysql.cursors

redis_pool = None
redis_conn = None
COIN = "BTCM"

def init():
    global redis_pool
    print("PID %d: initializing redis pool..." % os.getpid())
    redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True, db=0)

pymysqlpool.logger.setLevel('DEBUG')
myconfig = {
    'host': os.getenv('MYSQL_HOST_BTCM', 'default_host'),
    'user': os.getenv('MYSQL_USERNAME_BTCM', 'default_user'),
    'password': os.getenv('MYSQL_PASSWORD_BTCM', 'default_password'),
    'database': os.getenv('MYSQL_DATABASE_BTCM', 'default_db'),
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit':True
    }

connPool = pymysqlpool.ConnectionPool(size=5, name='connPool', **myconfig)
conn = connPool.get_connection(timeout=5, retry_num=2)
fee = 250000
fee_address = "btcmzTHkHtyhMoh8rjKgfvD13yFhs4eMmVBbuRcBMBZLdK67HCFbc4LegfEggApq8R2JJo8198vc4SwRjytTEbFZVvz6AyEUkuP"
decimal = 10000
daemon_rpc = os.getenv('BTCM_DAEMON_RPC', 'http://localhost:11358'),

def openConnection():
    global conn, connPool
    try:
        if conn is None:
            conn = connPool.get_connection(timeout=5, retry_num=2)
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()


# /amounts
async def handle_amounts(request):
    global conn
    mixin = 3
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ SELECT `toim`.`amount`, `toim`.`globalIndex` + 1 AS `outputs`, `t`.`timestamp`, `b`.`height`, `t`.`txnHash`, `b`.`hash` 
                      FROM `transaction_outputs_index_maximums` AS `toim` 
                      LEFT JOIN `transaction_outputs` AS `to` ON `to`.`amount` = `toim`.`amount` AND `to`.`globalIndex` = %s 
                      LEFT JOIN `transactions` AS `t` ON `t`.`txnHash` = `to`.`txnHash` 
                      LEFT JOIN `blocks` AS `b` ON `b`.`hash` = `t`.`blockHash` 
                      ORDER BY `toim`.`amount` """
            cur.execute(sql, (mixin))
            result = cur.fetchall()
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    if len(result) > 0:
        response_obj = result
        json_string = json.dumps(response_obj).replace(" ", "")
        return web.Response(text=json_string, status=200)
    else:
        text = 'Mixable amounts not found'
        return web.Response(text=text, status=404)


# /chain/stats
async def handle_chain_stats(request):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ SELECT `timestamp`, `difficulty`, `nonce`, `size`, 
                      (SELECT COUNT(*) FROM `transactions` WHERE `blockHash` = `hash`) AS `txnCount`
                      FROM `blocks` ORDER BY `height` DESC LIMIT 1440 """
            cur.execute(sql,)
            result = cur.fetchall()
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    if len(result) > 0:
        response_obj = result
        json_string = json.dumps(response_obj).replace(" ", "")
        return web.Response(text=json_string, status=200)
    else:
        text = 'Internal Server Error'
        return web.Response(text=text, status=500)


# /fee
async def handle_fee(request):
    global fee_address, fee
    reply = {
        "address": fee_address,
        "amount": fee,
        "status": "OK"
    }
    response_obj = reply
    json_string = json.dumps(response_obj).replace(" ", "")
    return web.Response(text=json_string, status=200)


# /height
async def handle_height(request):
    global conn, daemon_rpc
    json_data = None
    async with aiohttp.ClientSession() as session:
        async with session.get(daemon_rpc+'/info', timeout=8) as response:
            if response.status == 200:
                res_data = await response.read()
                res_data = res_data.decode('utf-8')
                await session.close()
                decoded_data = json.loads(res_data)
                json_data = decoded_data
    if json_data:
        reply = {
            "height": json_data['height'],
            "network_height": json_data['network_height']
        }
        response_obj = reply
        json_string = json.dumps(response_obj).replace(" ", "")
        return web.Response(text=json_string, status=200)
    else:
        text = 'Internal Server Error'
        return web.Response(text=text, status=500)


# /info
async def handle_info(request):
    global conn
    reply = None
    key = "getinfo"
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ SELECT `payload` FROM `information` WHERE `key` = %s """
            cur.execute(sql, (key))
            result = cur.fetchone()
            reply = json.loads(result['payload'].decode())
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    if reply:
        response_obj = reply
        json_string = json.dumps(response_obj).replace(" ", "")
        return web.Response(text=json_string, status=200)
    else:
        text = 'Internal Server Error'
        return web.Response(text=text, status=500)


# /supply
async def handle_supply(request):
    global conn, decimal
    reply = None
    key = "getinfo"
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                      `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                      FROM `blocks` ORDER BY `height` DESC LIMIT 1 """
            cur.execute(sql,)
            result = cur.fetchone()
            reply = str(result['alreadyGeneratedCoins'] / decimal)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    if reply:
        response_obj = reply
        return web.Response(text=response_obj, status=200)
    else:
        text = 'Internal Server Error'
        return web.Response(text=text, status=500)


# /block... /block/xxx
async def handle_block_more(request):
    global conn
    try:
        call_urel = str(request.rel_url).split("/")
        # print(call_urel) /block/xx/fdfs
        # ['', 'block', 'xx', 'fdfs']
        if len(call_urel) == 5:
            # DONE TEST
            # /block/headers/{height}/bulk
            # Try number
            try: 
                height = int(call_urel[3])
                cnt = 1000
                min = height - (cnt - 1)
                max = height
                try:
                    openConnection()
                    with conn.cursor() as cur:
                        sql = """ SELECT `size`, `difficulty`, `hash`, `height`, `timestamp`, `nonce`, 
                                  (SELECT COUNT(*) FROM `transactions` WHERE `transactions`.`blockHash` = `blocks`.`hash`) AS `tx_count` 
                                  FROM `blocks` WHERE `height` BETWEEN %s AND %s 
                                  ORDER BY `height` DESC """
                        cur.execute(sql, (min, max))
                        result = cur.fetchall()
                        if len(result) > 0:
                            response_obj = result
                            json_string = json.dumps(response_obj).replace(" ", "")
                            return web.Response(text=json_string, status=200)
                        else:
                            reply = "Not found"
                            return web.Response(text=reply, status=404)
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
            except ValueError:
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
        elif len(call_urel) == 4:
            # /block/header/{term}
            # /block/header/top
            # /block/headers/{height}
            if call_urel[2].lower() == "header" and call_urel[3].lower() == "top":
                # /block/header/top
                try:
                    openConnection()
                    with conn.cursor() as cur:
                        sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                  `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                  FROM `blocks` ORDER BY `height` DESC LIMIT 1 """
                        cur.execute(sql, )
                        result = cur.fetchone()
                        if result:
                            response_obj = result
                            # depth = 0 topblock
                            response_obj['depth'] = 0
                            json_string = json.dumps(response_obj).replace(" ", "")
                            return web.Response(text=json_string, status=200)
                        else:
                            reply = "Internal Server Error"
                            return web.Response(text=reply, status=500)
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
            elif call_urel[2].lower() == "header" and call_urel[3].lower() != "top":
                # /block/header/{term}
                if len(call_urel[3]) == 64:
                    # hash
                    if not re.match(r'[a-zA-Z0-9]{64,}', call_urel[3]):
                        reply = "Internal Server Error"
                        return web.Response(text=reply, status=500)
                    else:
                        try:
                            openConnection()
                            with conn.cursor() as cur:
                                sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                          `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                          FROM `blocks` WHERE `hash` = %s LIMIT 1 """
                                cur.execute(sql, call_urel[3])
                                result = cur.fetchone()
                                if result:
                                    response_obj = result
                                    sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                              `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                              FROM `blocks` ORDER BY `height` DESC LIMIT 1 """
                                    cur.execute(sql,)
                                    result = cur.fetchone()
                                    response_obj['depth'] = result['height'] - response_obj['height']
                                    json_string = json.dumps(response_obj).replace(" ", "")
                                    return web.Response(text=json_string, status=200)
                                else:
                                    reply = "Not Found"
                                    return web.Response(text=reply, status=404)
                        except Exception as e:
                            traceback.print_exc(file=sys.stdout)
                else:
                    # /block/header/{height}
                    # Try number
                    try: 
                        height = int(call_urel[3])
                        try:
                            openConnection()
                            with conn.cursor() as cur:
                                sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                          `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                          FROM `blocks` WHERE `height` = %s LIMIT 1 """
                                cur.execute(sql, height)
                                result = cur.fetchone()
                                if result:
                                    response_obj = result
                                    sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                              `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                              FROM `blocks` ORDER BY `height` DESC LIMIT 1 """
                                    cur.execute(sql,)
                                    result = cur.fetchone()
                                    response_obj['depth'] = result['height'] - response_obj['height']
                                    json_string = json.dumps(response_obj).replace(" ", "")
                                    return web.Response(text=json_string, status=200)
                                else:
                                    reply = "Not Found"
                                    return web.Response(text=reply, status=404)
                        except Exception as e:
                            traceback.print_exc(file=sys.stdout)
                    except ValueError:
                        reply = "Bad Request"
                        return web.Response(text=reply, status=400)
            elif call_urel[2].lower() == "headers":
                # /block/headers/{height}
                # Try number
                try: 
                    height = int(call_urel[3])
                    cnt = 30
                    min = height - (cnt - 1)
                    max = height
                    try:
                        openConnection()
                        with conn.cursor() as cur:
                            sql = """ SELECT `size`, `difficulty`, `hash`, `height`, `timestamp`, `nonce`, 
                                      (SELECT COUNT(*) FROM `transactions` WHERE `transactions`.`blockHash` = `blocks`.`hash`) AS `tx_count` 
                                      FROM `blocks` WHERE `height` BETWEEN %s AND %s 
                                      ORDER BY `height` DESC """
                            cur.execute(sql, (min, max))
                            result = cur.fetchall()
                            if len(result) > 0:
                                response_obj = result
                                json_string = json.dumps(response_obj).replace(" ", "")
                                return web.Response(text=json_string, status=200)
                            else:
                                reply = "Not found"
                                return web.Response(text=reply, status=404)
                    except Exception as e:
                        traceback.print_exc(file=sys.stdout)
                except ValueError:
                    reply = "Internal Server Error"
                    return web.Response(text=reply, status=500)
        elif len(call_urel) == 3:
            # /block/count
            # /block/{term}
            if len(call_urel[2].strip()) == 64:
                # hash
                hash = call_urel[2].strip()
                if not re.match(r'[a-zA-Z0-9]{64,}', hash):
                    reply = "Bad Request"
                    return web.Response(text=reply, status=400)
                else:
                    try:
                        openConnection()
                        with conn.cursor() as cur:
                            sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                      `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                      FROM `blocks` WHERE `hash` = %s LIMIT 1 """
                            cur.execute(sql, (hash))
                            result = cur.fetchone()
                            if result:
                                response_obj = result
                                sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                          `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                          FROM `blocks` ORDER BY `height` DESC LIMIT 1 """
                                cur.execute(sql,)
                                result = cur.fetchone()
                                response_obj['depth'] = result['height'] - response_obj['height']
                                sql = """ SELECT `totalOutputsAmount` AS `amount_out`, `fee`, `txnHash` AS `hash`, `size` 
                                          FROM `transactions` WHERE `blockHash` = %s """
                                cur.execute(sql, (call_urel[2]))
                                result = cur.fetchone()
                                response_obj['transactions'] = result
                                json_string = json.dumps(response_obj).replace(" ", "")
                                return web.Response(text=json_string, status=200)
                            else:
                                reply = "Not found"
                                return web.Response(text=reply, status=404)
                    except Exception as e:
                        traceback.print_exc(file=sys.stdout)
                        reply = "Internal Server Error"
                        return web.Response(text=reply, status=500)
            elif call_urel[2] == "count":
                # /block/count
                try:
                    openConnection()
                    with conn.cursor() as cur: 
                        sql = """ SELECT COUNT(*) AS `cnt` FROM `blocks` """
                        cur.execute(sql,)
                        result = cur.fetchone()
                        if result:
                            reply = {
                              "blockCount": result['cnt']
                            }
                            response_obj = reply
                            json_string = json.dumps(response_obj).replace(" ", "")
                            return web.Response(text=json_string, status=200)
                        else:
                            reply = "Internal Server Error"
                            return web.Response(text=reply, status=500)
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
            else:
                # /block/{integer}
                # Try number
                try: 
                    height = int(call_urel[2])
                    try:
                        openConnection()
                        with conn.cursor() as cur:
                            sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                      `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                      FROM `blocks` WHERE `height` = %s LIMIT 1 """
                            cur.execute(sql, (height))
                            result = cur.fetchone()
                            if result:
                                response_obj = result
                                sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                          `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                          FROM `blocks` ORDER BY `height` DESC LIMIT 1 """
                                cur.execute(sql,)
                                result = cur.fetchone()
                                response_obj['depth'] = result['height'] - response_obj['height']
                                sql = """ SELECT `totalOutputsAmount` AS `amount_out`, `fee`, `txnHash` AS `hash`, `size` 
                                          FROM `transactions` WHERE `blockHash` = %s """
                                cur.execute(sql, (response_obj['hash']))
                                result = cur.fetchone()
                                response_obj['transactions'] = result
                                json_string = json.dumps(response_obj).replace(" ", "")
                                return web.Response(text=json_string, status=200)
                            else:
                                reply = "Not found"
                                return web.Response(text=reply, status=404)
                    except Exception as e:
                        traceback.print_exc(file=sys.stdout)
                        reply = "Internal Server Error"
                        return web.Response(text=reply, status=500)
                except ValueError:
                    reply = "Bad Request"
                    return web.Response(text=reply, status=400)
        elif len(call_urel) == 2:
            # /block
            reply = "Internal Server Error"
            return web.Response(text=reply, status=500)
    except Exception as e:
        # TODO: 400 and 404
        reply = "Internal Server Error"
        return web.Response(text=reply, status=500)


# /transaction... /transaction/xxx
async def handle_transaction_more(request):
    global conn
    try:
        call_urel = str(request.rel_url).split("/")
        if len(call_urel) == 4:
            # /transaction/{hash}/inputs
            # /transaction/{hash}/outputs
            hash = call_urel[2]
            if not re.match(r'[a-zA-Z0-9]{64,}', hash):
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
            if call_urel[3].lower() == "inputs":
                # /transaction/{hash}/inputs
                try:
                    openConnection()
                    with conn.cursor() as cur:
                        sql = """ SELECT `keyImage`, `amount`, `type` FROM `transaction_inputs` WHERE `txnHash` = %s ORDER BY `amount`, `keyImage` """
                        cur.execute(sql, (hash))
                        result = cur.fetchall()
                        if result:
                            response_obj = result
                            json_string = json.dumps(response_obj).replace(" ", "")
                            return web.Response(text=json_string, status=200)
                        else:
                            reply = "Not Found"
                            return web.Response(text=reply, status=404)
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
                    reply = "Internal Server Error"
                    return web.Response(text=reply, status=500)
            elif call_urel[3].lower() == "outputs":
                # /transaction/{hash}/outputs
                try:
                    openConnection()
                    with conn.cursor() as cur:
                        sql = """ SELECT `outputIndex`, `globalIndex`, `amount`, `key`, `type` 
                                  FROM `transaction_outputs` WHERE `txnHash` = %s ORDER BY `outputIndex` """
                        cur.execute(sql, (hash))
                        result = cur.fetchall()
                        if result:
                            response_obj = result
                            json_string = json.dumps(response_obj).replace(" ", "")
                            return web.Response(text=json_string, status=200)
                        else:
                            reply = "Not Found"
                            return web.Response(text=reply, status=404)
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
                    reply = "Internal Server Error"
                    return web.Response(text=reply, status=500)
            else:
                reply = "Internal Server Error"
                return web.Response(text=reply, status=500)
        elif len(call_urel) == 3:
            # /transaction/{hash}
            # /transaction/pool
            if call_urel[2].lower() == "pool":
                # /transaction/pool
                try:
                    openConnection()
                    with conn.cursor() as cur:
                        sql = """ SELECT * FROM `transaction_pool` """
                        cur.execute(sql,)
                        result = cur.fetchall()
                        if len(result) > 0:
                            response_obj = result
                            json_string = json.dumps(response_obj).replace(" ", "")
                            return web.Response(text=json_string, status=200)
                        else:
                            reply = "Not Found"
                            return web.Response(text=reply, status=404)
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
                    reply = "Internal Server Error"
                    return web.Response(text=reply, status=500)
            else:
                # /transaction/{hash}
                hash = call_urel[2]
                if not re.match(r'[a-zA-Z0-9]{64,}', hash):
                    reply = "Bad Request"
                    return web.Response(text=reply, status=400)
                else:
                    try:
                        openConnection()
                        with conn.cursor() as cur:
                            sql = """ SELECT `transactions`.*, `unlockTime`  
                                      FROM `transactions` WHERE `transactions`.`txnHash` = %s LIMIT 1 """
                            cur.execute(sql, (hash))
                            result = cur.fetchone()
                            if len(result) > 0:
                                blockHash = result['blockHash']
                                tx = {
                                    'amount_out': result['totalOutputsAmount'],
                                    'fee': result['fee'],
                                    'hash': result['txnHash'],
                                    'mixin': result['mixin'],
                                    'paymentId': result['paymentId'],
                                    'size': result['size'],
                                    'extra': result['extra'].hex(),
                                    'unlock_time': str(result['unlockTime']),
                                    'nonce': result['nonce'],
                                    'publicKey': result['publicKey']
                                }
                                sql = """  SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                                           `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                                           FROM `blocks` WHERE `hash` = %s LIMIT 1 """
                                cur.execute(sql, (blockHash))
                                result = cur.fetchone()
                                block = {
                                    'cumul_size': result['size'],
                                    'difficulty': result['difficulty'],
                                    'hash': result['hash'],
                                    'height': result['height'],
                                    'timestamp': result['timestamp'],
                                    'tx_count': result['transactionCount']
                                }
                                sql = """ SELECT `keyImage`, `amount`, `type` FROM `transaction_inputs` WHERE `txnHash` = %s ORDER BY `amount`, `keyImage` """
                                cur.execute(sql, (hash))
                                result = cur.fetchall()
                                inputs = result
                                sql = """ SELECT `outputIndex`, `globalIndex`, `amount`, `key`, `type` 
                                          FROM `transaction_outputs` WHERE `txnHash` = %s ORDER BY `outputIndex` """
                                cur.execute(sql, (hash))
                                result = cur.fetchall()
                                outputs = result
                                response_obj = {'tx': tx, 'inputs': inputs, 'outputs': outputs, 'block': block}
                                json_string = json.dumps(response_obj).replace(" ", "")
                                return web.Response(text=json_string, status=200)
                            else:
                                reply = "Not Found"
                                return web.Response(text=reply, status=404)
                    except Exception as e:
                        traceback.print_exc(file=sys.stdout)
                        reply = "Internal Server Error"
                        return web.Response(text=reply, status=500)
        elif len(call_urel) <= 2:
            reply = "Internal Server Error"
            return web.Response(text=reply, status=500)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


# /transactions... /transactions/xxx
async def handle_transactions_more(request):
    global conn
    try:
        call_urel = str(request.rel_url).split("/")
        if len(call_urel) == 3:
            # /transactions/{paymentId}
            hash = call_urel[2]
            if not re.match(r'[a-zA-Z0-9]{64,}', hash):
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
            else:
                try:
                    openConnection()
                    with conn.cursor() as cur:
                        sql = """ SELECT `txnHash` AS `hash`,`mixin`,`timestamp`,`fee`,`size`, 
                                  `totalOutputsAmount` AS `amount` 
                                  FROM `transactions` 
                                  WHERE `paymentId` = %s 
                                  ORDER BY `timestamp` """
                        cur.execute(sql, (hash))
                        result = cur.fetchall()
                        if len(result) > 0:
                            response_obj = result
                            json_string = json.dumps(response_obj).replace(" ", "")
                            return web.Response(text=json_string, status=200)
                        else:
                            reply = "Not Found"
                            return web.Response(text=reply, status=404)
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
                    reply = "Internal Server Error"
                    return web.Response(text=reply, status=500)
        elif len(call_urel) <= 2:
            reply = "Internal Server Error"
            return web.Response(text=reply, status=500)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


# post /getwalletsyncdata
async def handle_getwalletsyncdata_post(request):
    global conn
    data = await request.json()
    # This seemed OK now print OFF
    #print(str(request.rel_url))
    #print('post /getwalletsyncdata - data')
    #print(json.dumps(data))
    call_urel = str(request.rel_url).split("/")
    if len(call_urel) == 3:
        # /getwalletsyncdata/preflight
        if 'startHeight' in data:
            try:
                startHeight = data['startHeight']
            except ValueError:
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
        if 'startTimestamp' in data:
            try:
                startTimestamp = data['startTimestamp']
            except ValueError:
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
        if 'blockHashCheckpoints' in data:
            blockHashCheckpoints = data['blockHashCheckpoints'] or []
        topHeight = 0
        if len(blockHashCheckpoints) > 0:
            blockHashCheckpoints_str = "("+",".join(['"{0}"'.format(w) for w in blockHashCheckpoints]) + ")"
            try:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT `height` FROM `blocks` WHERE `hash` IN """+blockHashCheckpoints_str+""" ORDER BY `height` DESC LIMIT 1 """
                    cur.execute(sql,)
                    result = cur.fetchone()
                    if result:
                        topHeight = result['height'] + 1
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
        # if there is startTimestamp
        if startTimestamp:
            try:
                timestamp = int(startTimestamp)
                try:
                    openConnection()
                    with conn.cursor() as cur:
                        sql = """ SELECT `height` FROM `blocks` WHERE `timestamp` <= %s ORDER BY `height` DESC LIMIT 1 """
                        cur.execute(sql, (timestamp))
                        result = cur.fetchone()
                        if result:
                            topHeight = result['height']
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
            except ValueError:
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
        if startHeight > topHeight:
            topHeight = startHeight
        try:
            openConnection()
            with conn.cursor() as cur:
                sql = """ SELECT `hash`, `height`, `timestamp` FROM `blocks` WHERE `height` >= %s ORDER BY `height` ASC LIMIT 1 """
                cur.execute(sql, (topHeight))
                result = cur.fetchone()
                topBlock = get_TopBlock()
                if result:
                    resolvableBlocks = topBlock['height'] - result['height'] + 1
                    if resolvableBlocks > 100:
                        resolvableBlocks = 100
                    else:
                        resolvableBlocks = resolvableBlocks
                    reply = {
                        'height': result['height'],
                        'blockCount': resolvableBlocks
                    }
                    response_obj = reply
                    json_string = json.dumps(response_obj).replace(" ", "")
                    return web.Response(text=json_string, status=200)
                else:
                    reply = "Internal Server Error"
                    return web.Response(text=reply, status=500)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    elif len(call_urel) == 2:
        start = time.time()
        # /getwalletsyncdata
        if 'startHeight' in data:
            try:
                startHeight = int(data['startHeight'])
            except ValueError:
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
        startTimestamp = None
        if 'startTimestamp' in data:
            try:
                startTimestamp = int(data['startTimestamp'])
            except ValueError:
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
        blockCount = None
        if 'blockCount' in data:
            try:
                blockCount = int(data['blockCount'])
            except ValueError:
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
        blockHashCheckpoints = None
        if 'blockHashCheckpoints' in data:
            blockHashCheckpoints = data['blockHashCheckpoints'] or []
        # hack from 100 to 1000
        blockCount = blockCount or 100
        skipCoinbaseTransactions = None
        if 'skipCoinbaseTransactions' in data:
            skipCoinbaseTransactions = data['skipCoinbaseTransactions'] or False
        validHash = True
        if blockHashCheckpoints and len(blockHashCheckpoints) > 0:
            for hash in blockHashCheckpoints:
                if not re.match(r'[a-zA-Z0-9]{64,}', hash):
                    validHash = False
            if validHash == False:
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
        if startHeight and startTimestamp:
            # can not supply both
            reply = "Bad Request"
            return web.Response(text=reply, status=400)
        # skipCoinbaseTransactions
        if skipCoinbaseTransactions:
            skipCoinbaseTransactions = True
        # blockCount. Hack from 100 to 1000
        if blockCount > 1000:
            blockCount = 1000
        elif blockCount < 1:
            blockCount = 1
        topHeight = 0
        topHash = ''
        # start check blockHashCheckpoints
        if blockHashCheckpoints and len(blockHashCheckpoints) > 0:
            blockHashCheckpoints_str = "(" + ",".join(['"{0}"'.format(w) for w in blockHashCheckpoints]) + ")"
            try:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT `height` FROM `blocks` WHERE `hash` IN """+blockHashCheckpoints_str+""" ORDER BY `height` DESC LIMIT 1 """
                    cur.execute(sql,)
                    result = cur.fetchone()
                    if result:
                        topHeight = result['height'] + 1
                        print('from blockHashCheckpoints get height {}'.format(topHeight))
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
        # if there is startTimestamp
        if startTimestamp:
            try:
                timestamp = int(startTimestamp)
                try:
                    openConnection()
                    with conn.cursor() as cur:
                        sql = """ SELECT `height` FROM `blocks` WHERE `timestamp` <= %s ORDER BY `height` DESC LIMIT 1 """
                        cur.execute(sql, (timestamp))
                        result = cur.fetchone()
                        if result:
                            topHeight = result['height']
                            print('startTimestamp {} get height {}'.format(startTimestamp, topHeight))
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
            except ValueError:
                reply = "Bad Request"
                return web.Response(text=reply, status=400)
        if startHeight > topHeight:
            topHeight = startHeight
        sql = """ SELECT `hash`, `height`, `timestamp` FROM `blocks` """
        if skipCoinbaseTransactions:
            #print('post /getwalletsyncdata skipCoinbaseTransactions')
            sql += """ LEFT JOIN (SELECT `blockHash`, COUNT(*) AS `txnCount` FROM `transactions` GROUP BY `blockHash`) """
            sql += """ AS `transactions` ON `transactions`.`blockHash` = `blocks`.`hash` """
        sql += """ WHERE `height` >= %s """
        if skipCoinbaseTransactions:
            sql += """ AND `transactions`.`txnCount` > 1 """
        sql += """ ORDER BY `height` ASC LIMIT %s """
        try:
            global COIN
            result = None
            openConnection()
            with conn.cursor() as cur:
                cur.execute(sql, (topHeight, blockCount))
                result = cur.fetchall()
                print('{} post /getwalletsyncdata: get blockCount: {} topHeight:{}'.format(COIN, blockCount, topHeight))
                end = time.time()
                print('{} Query time to fetch blockHashes Data before query tx: {}s'.format(COIN, end-start))
                blockList = await post_getwalletsyncdata(result)
                print('{} Query time from blockList: {}s'.format(COIN, time.time()-end))
                if blockList is None:
                    reply = "Bad Request"
                    return web.Response(text=reply, status=400)
                if len(blockList) > 0:
                    response_obj = {
                        'items': blockList,
                        'status': 'OK',
                        'synced': False
                    }
                else:
                    with conn.cursor() as cur:
                        sql = """ SELECT `hash`, `height` FROM `blocks` ORDER BY `height` DESC LIMIT 1 """
                        cur.execute(sql,)
                        result = cur.fetchone()
                        if result:
                            topHeight = result['height']
                            topHash = result['hash']
                    response_obj = {
                        'items': blockList,
                        'status': 'OK'
                    }
                print('return #{} blocks.'.format(len(blockList)))
                json_string = json.dumps(response_obj).replace(" ", "")
                return web.Response(text=json_string, status=200, content_type='application/json')
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    else:
        reply = "Bad Request"
        return web.Response(text=reply, status=400)


# get /getwalletsyncdata
async def handle_getwalletsyncdata_get(request):
    global conn
    data = await request.json()
    call_urel = str(request.rel_url).split("/")
    # /getwalletsyncdata/{height}/{blockCount}
    # /getwalletsyncdata/{height}
    if len(call_urel) == 4:
        # /getwalletsyncdata/{height}/{blockCount}
        # function HERE
        height = call_urel[2]
        blockCount = call_urel[3]
        try:
            height = int(call_urel[2])
            blockCount = int(call_urel[3])
            reply = await get_wallet_data(height, blockCount)
            return web.Response(text=reply, status=200)
        except ValueError:
            reply = "Bad Request"
            return web.Response(text=reply, status=400)
        
    elif len(call_urel) == 3:
        # /getwalletsyncdata/{height}
        try: 
            height = int(call_urel[2])
            blockCount = 100
            reply = await get_wallet_data(height, blockCount)
            return web.Response(text=reply, status=200)
        except ValueError:
            reply = "Bad Request"
            return web.Response(text=reply, status=400)
    else:
        reply = "Bad Request"
        return web.Response(text=reply, status=400)


# /sync
async def handle_sync(request):
    global conn
    data = await request.json()
    print(data)
    print(str(request.rel_url))
    lastKnownBlockHashes = data['lastKnownBlockHashes'] or []
    blockCount = data['blockCount']
    scanHeight  = data['scanHeight']
    if isinstance(lastKnownBlockHashes, list) and scanHeight is None:
        reply = "Bad Request"
        return web.Response(text=reply, status=400)


async def get_TopBlock():
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE 
                      `transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` 
                      FROM `blocks` ORDER BY `height` DESC LIMIT 1 """
            cur.execute(sql, (min, max))
            result = cur.fetchone()
        return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


# to return blockList
async def get_wallet_data(height: int, blockCount: int):
    global conn
    print('function get_wallet_data called height: {} blockCount: {}'.format(height, blockCount))
    try: 
        min = height
        max = min + blockCount
        blockList = []
        try:
            openConnection()
            with conn.cursor() as cur:
                sql = """ SELECT `hash` AS `blockHash`, `height`, `timestamp` 
                          FROM `blocks` 
                          WHERE `height` >= %s AND `height` < %s 
                          ORDER BY `height` """
                cur.execute(sql, (min, max))
                result = cur.fetchall()
            if result:
                hashes = []
                blockHashes = {}
                heights = {}
                timestamps = {}
                for item in result:
                    hashes.append("'"+item['hash']+"'")
                    blockHashes[item['hash']] = item['hash']
                    heights[item['hash']] = item['height']
                    timestamps[item['hash']] = item['timestamp']
                hashes_str = "(" + ",".join(hashes) + ")"
                sql = """ SELECT `blockHash`, `txnHash`, `publicKey`, 
                          `unlockTime`, 
                          `paymentId` FROM `transactions` WHERE `blockHash` IN (SELECT `hash` AS `blockHash` FROM `blocks` WHERE  blocks.hash IN """+hashes_str+""");
                      """
                cur.execute(sql,)
                result_tx = cur.fetchall()
                tx_hashes = []
                for item in result_tx:
                    tx_hashes.append("'"+item['txnHash']+"'")
                txnHashes_str = "(" + ",".join(tx_hashes) + ")"
                sql = """ SELECT `txnHash`, `keyImage`, `amount`, `type` 
                          FROM `transaction_inputs` 
                          WHERE `txnHash` IN """+txnHashes_str+""" ORDER BY `amount`;
                      """
                cur.execute(sql,)
                result_tx_inputs = cur.fetchall()
                input_list = []
                inputs = []
                for input_tx in result_tx_inputs:
                    if input_tx['type'] != 255:
                        # != coinBase
                        input_list.append(input_tx['txnHash'])
                        inputs.append({'amount': input_tx['amount'], 'k_image': input_tx['keyImage']})

                sql = """ SELECT `txnHash`, `outputIndex`, `globalIndex`,
                          `key`, `amount`, `type` 
                          FROM `transaction_outputs` 
                          WHERE `txnHash` IN """+txnHashes_str+""" 
                          ORDER BY `outputIndex`;
                      """
                cur.execute(sql,)
                result_tx_outputs = cur.fetchall()
                output_list = []
                outputs = []
                for output_tx in result_tx_outputs:
                    output_list.append(output_tx['txnHash'])
                    outputs.append({'amount': output_tx['amount'], 'key': output_tx['key'], 'globalIndex': output_tx['globalIndex']})

                # indices for inputs, outputs
                tx_in = {}
                tx_out = {}
                set_in = list(set(input_list))
                set_out = list(set(output_list))
                for key, value in enumerate(set_in):
                    indices = [i for i, x in enumerate(input_list) if x == value]
                    tx_in[value] = [inputs[i] for i in indices]
                for key, value in enumerate(set_out):
                    indices = [i for i, x in enumerate(output_list) if x == value]
                    tx_out[value] = [outputs[i] for i in indices]

                tx_list = []
                txs = []
                coinBaseList = {}
                for item in result_tx:
                    tx_list.append(item['blockHash'])
                    if item['txnHash'] in tx_in:
                        # normal tx
                        txs.append({
                            'hash': item['txnHash'],
                            'paymentID': item['paymentId'],
                            'txPublicKey': item['publicKey'],
                            'unlockTime': str(item['unlockTime']),
                            'inputs': tx_in[item['txnHash']],
                            'outputs': tx_out[item['txnHash']]
                            })
                    else:
                        # coinBase
                        coinBaseList[item['blockHash']] = {
                            'hash': item['txnHash'],
                            'txPublicKey': item['publicKey'],
                            'unlockTime': str(item['unlockTime']),
                            'outputs': tx_out[item['txnHash']]
                        }
                block_list = {}
                block_tx = list(set(tx_list))
                for key, value in enumerate(block_tx):
                    indices = [i for i, x in enumerate(block_tx) if x == value]
                    block_list[value] = [txs[i] for i in indices]

                # loop hash blocks
                for key, value in blockHashes.items():
                    blockList.append({
                        'blockHash': key,
                        'blockHeight': heights[key],
                        'blockTimestamp': timestamps[key],
                        'coinbaseTX': coinBaseList[key],
                        'transactions': block_list[key]
                    })
            response_obj = {
                'items': blockList,
                'status': 'OK',
            }
            json_string = json.dumps(response_obj).replace(" ", "")
            return web.Response(text=json_string, status=200)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    except ValueError:
        reply = "Bad Request"
        return web.Response(text=reply, status=400)


async def post_getwalletsyncdata(blockHashesResult):
    global conn, redis_pool, redis_conn
    blockList = []
    hashes = []
    blockHashes = {}
    heights = {}
    timestamps = {}
    if len(blockHashesResult) == 0:
        return None
    for item in blockHashesResult:
        if redis_conn is None:
            try:
                redis_conn = redis.Redis(connection_pool=redis_pool)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
        key = item['height'] # block hash
        if redis_conn.exists(f'BTCMBLOCK:{key}'):
            blockList.append(json.loads(zlib.decompress(redis_conn.get(f'BTCMBLOCK:{key}')).decode()))
        else:
            hashes.append("'"+item['hash']+"'")
            blockHashes[item['hash']] = item['hash']
            heights[item['hash']] = item['height']
            timestamps[item['hash']] = item['timestamp']

    if len(list(blockHashes.keys())) == 0:
        return blockList
    else:
        pass
 
    start = time.time()
    with conn.cursor() as cur:
        hashes_str = "(" + ",".join(hashes) + ")"
        sql = """ SELECT `blockHash`, `txnHash`, `publicKey`, 
                  `unlockTime`, 
                 `paymentId` FROM `transactions` WHERE `blockHash` IN (SELECT `hash` AS `blockHash` FROM `blocks` WHERE  blocks.hash IN """+hashes_str+""");
             """
        cur.execute(sql,)
        result_tx = cur.fetchall()
        
        tx_hashes = []
        for item in result_tx:    
            tx_hashes.append("'"+item['txnHash']+"'")
        txnHashes_str = "(" + ",".join(tx_hashes) + ")"
        sql = """ SELECT `txnHash`, `keyImage`, `amount`, `type` 
                  FROM `transaction_inputs` 
                  WHERE `txnHash` IN """+txnHashes_str+""" ORDER BY `amount`;
             """
        cur.execute(sql,)
        result_tx_inputs = cur.fetchall()

        input_list = []
        inputs = []
        for input_tx in result_tx_inputs:
            if input_tx['type'] != 255:
                # != coinBase
                input_list.append(input_tx['txnHash'])
                inputs.append({'amount': input_tx['amount'], 'k_image': input_tx['keyImage']})
        sql = """ SELECT `txnHash`, `outputIndex`, `globalIndex`,
                  `key`, `amount`, `type` 
                  FROM `transaction_outputs` 
                  WHERE `txnHash` IN """+txnHashes_str+""" 
                  ORDER BY `outputIndex`;
             """
        cur.execute(sql,)
        result_tx_outputs = cur.fetchall()
        end = time.time()
        print('Query SQL in post_get_data: {}s'.format(end-start))
        start = time.time()
        output_list = []
        outputs = []
        for output_tx in result_tx_outputs:
            output_list.append(output_tx['txnHash'])
            outputs.append({'amount': output_tx['amount'], 'key': output_tx['key'], 'globalIndex': output_tx['globalIndex']})

        # indices for inputs, outputs
        tx_in = {}
        tx_out = {}
        for value in list(set(input_list)):
            tx_in[value] = [inputs[i] for i in [i for i, x in enumerate(input_list) if x == value]]
        for value in list(set(output_list)):
            tx_out[value] = [outputs[i] for i in [i for i, x in enumerate(output_list) if x == value]]
        tx_list = []
        txs = []
        coinBaseList = {}
        for item in result_tx:
            if item['txnHash'] in tx_in:
                tx_list.append(item['blockHash'])
                # normal tx
                txs.append({
                    'hash': item['txnHash'],
                    'paymentID': item['paymentId'],
                    'txPublicKey': item['publicKey'],
                    'unlockTime': str(item['unlockTime']),
                    'inputs': tx_in[item['txnHash']],
                    'outputs': tx_out[item['txnHash']]
                    })
            else:
                # coinBase
                if item['txnHash'] in tx_out:
                    coinBaseList[item['blockHash']] = {
                        'hash': item['txnHash'],
                        'txPublicKey': item['publicKey'],
                        'unlockTime': str(item['unlockTime']),
                        'outputs': tx_out[item['txnHash']]
                    }
                else:
                    coinBaseList[item['blockHash']] = {
                        'hash': item['txnHash'],
                        'txPublicKey': item['publicKey'],
                        'unlockTime': str(item['unlockTime']),
                        'outputs': []
                    }
        block_list = {}
        for value in list(set(tx_list)):
            block_list[value] = [txs[i] for i in [i for i, x in enumerate(tx_list) if x == value]]

        if redis_conn is None:
            try:
                redis_conn = redis.Redis(connection_pool=redis_pool)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
        for key, value in blockHashes.items():
            each_block = {
                'blockHash': key,
                'blockHeight': heights[key],
                'blockTimestamp': timestamps[key],
                'coinbaseTX': coinBaseList[key],
                'transactions': block_list[key] if key in block_list else []
            }
            if redis_conn:
                if redis_conn.exists(f'BTCMBLOCK:{heights[key]}') == False:
                    try:
                        if key in block_list:
                            numTx = len(coinBaseList[key]) + len(block_list[key])
                        else:
                            numTx = 1
                        # add to mariaDB `cache_blocks`
                        sql = """ INSERT INTO cache_blocks (`hash`, `height`, `timestamp`, `txnum`, `blockinfo`) 
                                  VALUES (%s, %s, %s, %s, %s) """
                        cur.execute(sql, (key, heights[key], timestamps[key], numTx, json.dumps(each_block).replace(" ", "")))
                        conn.commit()
                        # add to redis
                        redis_conn.set(f'BTCMBLOCK:{heights[key]}', zlib.compress(json.dumps(each_block).replace(" ", "").encode(), 9))
                    except Exception as e:
                        traceback.print_exc(file=sys.stdout)
            blockList.append(each_block)
        end = time.time()
        print('Operation after Query to issue blockList in post_get_data: {}s'.format(end-start))
        return blockList


app = web.Application()

routes = [
    web.get('/amounts', handle_amounts),
    web.get('/chain/stats', handle_chain_stats),
    web.get('/fee', handle_fee),
    web.get('/height', handle_height),
    web.get('/info', handle_info),
    web.get('/supply', handle_supply),
    # web.post('/block', handle_block),
    web.get('/block/{tail:.*}', handle_block_more),
    web.get('/transaction/{tail:.*}', handle_transaction_more),
    web.get('/transactions/{tail:.*}', handle_transactions_more),
    web.post('/getwalletsyncdata', handle_getwalletsyncdata_post),
    web.get('/getwalletsyncdata/{tail:.*}', handle_getwalletsyncdata_get),
    web.post('/sync', handle_sync),
]

app.add_routes(routes)

web.run_app(app, host='127.0.0.1', port=8082)
