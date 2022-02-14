import asyncio, barnum, json, math, random, requests, signal, os, socket, sys, time
from datetime import datetime, timezone
from kafka import KafkaProducer
from mysql.connector import connect, Error
from concurrent.futures import CancelledError

# CONFIG
user_seed_count     = 10000
item_seed_count     = 1000
purchases_per_second_key = 'purchases_per_second'
pageviews_per_second_key = 'pageviews_per_second'
params              = {  purchases_per_second_key : int(os.environ['PURCHASES_PER_SECOND_START']),
                         pageviews_per_second_key : int(os.environ['PAGEVIEWS_PER_SECOND_START']) }
command_endpoint    = os.environ['COMMAND_ENDPOINT']
log_period_seconds  = 10
item_inventory_min  = 1000
item_inventory_max  = 5000
item_price_min      = 5
item_price_max      = 500
mysql_host          = 'mysql'
mysql_port          = '3306'
mysql_user          = 'root'
mysql_pass          = 'debezium'
kafka_endpoint      = 'redpanda:9092'
kafka_topic         = 'pageviews'
debezium_endpoint   = 'debezium:8083'
channels            = ['organic search', 'paid search', 'referral', 'social', 'display']
categories          = ['widgets', 'gadgets', 'doodads', 'clearance']

# INSERT TEMPLATES
item_insert     = "INSERT INTO shop.items (name, category, price, inventory) VALUES ( %s, %s, %s, %s )"
user_insert     = "INSERT INTO shop.users (email, is_vip) VALUES ( %s, %s )"
purchase_insert = "INSERT INTO shop.purchases (user_id, item_id, quantity, purchase_price) VALUES ( %s, %s, %s, %s )"

def log(msg):
    tstamp = datetime.now(tz=timezone.utc).isoformat().replace('+00:00', 'Z')
    print(f"{tstamp}  {msg}", flush=True)

#
# Methods to support generation of pageview events and database updates
#
def generate_pageview(viewer_id, target_id, page_type):
    return {
        "user_id": viewer_id,
        "url": f'/{page_type}/{target_id}',
        "channel": random.choice(channels),
        "received_at": int(time.time())
    }

def initialize_database(connection, cursor):
    log("Initializing shop database...")
    cursor.execute('CREATE DATABASE IF NOT EXISTS shop;')
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS shop.users
            (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255),
                is_vip BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS shop.items
            (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                category VARCHAR(100),
                price DECIMAL(7,2),
                inventory INT,
                inventory_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS shop.purchases
            (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNSIGNED REFERENCES user(id),
                item_id BIGINT UNSIGNED REFERENCES item(id),
                status TINYINT UNSIGNED DEFAULT 1,
                quantity INT UNSIGNED DEFAULT 1,
                purchase_price DECIMAL(12,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );"""
    )
    connection.commit()
    log("Initializing shop database DONE.")

def seed_data(connection, cursor):
    log("Seeding data...")
    cursor.executemany(
        item_insert,
        [
            (
                barnum.create_nouns(),
                random.choice(categories),
                random.randint(item_price_min*100,item_price_max*100)/100,
                random.randint(item_inventory_min,item_inventory_max)
            ) for i in range(item_seed_count)
        ]
    )
    cursor.executemany(
        user_insert,
        [
            (
                barnum.create_email(),
                (random.randint(0,10) > 8)
             ) for i in range(user_seed_count)
        ]
    )
    connection.commit()
    log("Seeding data DONE.")

def get_item_prices(cursor):
    log("Getting item ID and PRICEs...")
    cursor.execute("SELECT id, price FROM shop.items")
    log("Getting item ID and PRICEs DONE.")
    return [(row[0], row[1]) for row in cursor]

async def loop(action_fun, param_key, action_desc):
    start = time.time()
    sent_at_rate = 0
    time_last_log = start
    sent_since_last_log = 0
    rate_s = params[param_key]
    old_rate_s = rate_s
    period_s = 1.0/rate_s
    log(f"Simulating {action_desc} actions with an initial rate of {rate_s} per second.")
    tight_count = 0
    try:
        while True:
            action_fun()
            sent_at_rate += 1
            sent_since_last_log += 1
            now = time.time()
            time_last_log_diff = now - time_last_log
            if time_last_log_diff >= log_period_seconds:
                period_str = "%.1f" % time_last_log_diff
                log(f"Simulated {sent_since_last_log} {action_desc} actions in the last {period_str} seconds.")
                time_last_log = now
                sent_since_last_log = 0
            next_time = start + (sent_at_rate+1)*period_s
            sleep_s = next_time - time.time()
            if sleep_s > 0:
                tight_count = 0
                await asyncio.sleep(sleep_s)
            else:
                tight_count += 1
            if tight_count * period_s >= 1.0:
                log(f"Rate {rate_s} is too high, can't honor it.")
                params[param_key] = math.floor(sent_at_rate / (time.time() - start))
            rate_s = params[param_key]
            if old_rate_s != rate_s:
                log(f"Changing {action_desc} rate from {old_rate_s} to {rate_s} per second.")
                period_s = 1.0/rate_s
                old_rate_s = rate_s
                start = time.time()
                sent_at_rate = 0
    except CancelledError:
        pass
    log(f"Stopped simulating {action_desc} actions.")

async def loop_purchases(connection, cursor, producer, item_prices):
    log("Preparing to loop + seed kafka pageviews and purchases.")
    def make_purchase():
        # Get a user and item to purchase
        purchase_item = random.choice(item_prices)
        purchase_user = random.randint(0,user_seed_count-1)
        purchase_quantity = random.randint(1,5)

        # Write purchaser pageview
        producer.send(kafka_topic, key=str(purchase_user).encode('ascii'), value=generate_pageview(purchase_user, purchase_item[0], 'products'))

        # Write purchase row
        cursor.execute(
            purchase_insert,
            (
                purchase_user,
                purchase_item[0],
                purchase_quantity,
                purchase_item[1] * purchase_quantity
            )
        )
        connection.commit()
    await loop(make_purchase, purchases_per_second_key, "purchase")

async def loop_random_pageviews(producer):
    # Write random pageviews to products or profiles
    def make_random_pageview():
        rand_user = random.randint(0,user_seed_count)
        rand_page_type = random.choice(['products', 'profiles'])
        target_id_max_range = item_seed_count if rand_page_type == 'products' else user_seed_count
        producer.send(kafka_topic, key=str(rand_user).encode('ascii'), value=generate_pageview(rand_user, random.randint(0,target_id_max_range), rand_page_type))
    await loop(make_random_pageview, pageviews_per_second_key, "pageview")

def chomp(s):
    return s if not s.endswith(os.linesep) else s[:-len(os.linesep)]
#
# Simple socket-based command interface.
#
async def handle_command_client(reader, writer):
    peer = reader._transport.get_extra_info('peername')
    me = f"Command interface to {peer}"
    log(f"{me} connected.")
    async def send(msg):
        out_bytes = (msg + "\n").encode('utf8')
        writer.write(out_bytes)
        await writer.drain()
        log(f"{me} sent: '{msg}'.")
    await send("LOADGEN Connected.")
    try:
        while True:
            in_bytes = await reader.readline()
            if len(in_bytes) == 0:
                log(f"{me} received zero bytes.")
                break;
            in_str = chomp(in_bytes.decode('utf8'))
            log(f"{me} received: '{in_str}'.")
            cmd_words = in_str.split()
            if len(cmd_words) == 0:
                await send("Empty request.")
                continue
            if cmd_words[0].lower() == 'quit':
                await send("Goodbye.")
                break
            if cmd_words[0].lower() != "set":
                await send(f"Unrecognized command: '{cmd_words[0]}'.")
                continue
            if len(cmd_words) != 3:
                await send(f"{cmd_words[0]} command takes 2 arguments, {len(cmd_words)} given.")
                continue
            def to_int(s):
                try:
                    return int(s)
                except ValueError:
                    return -1
            key = cmd_words[1].lower()
            if key == purchases_per_second_key or key == pageviews_per_second_key:
                value = to_int(cmd_words[2])
                if value <= 0:
                    await send(f"Invalid value: '{cmd_words[2]}'.")
                    continue
                old_value = params[key]
                params[key] = value
                await send(f"Setting {key}: old value was {old_value}, new value is {value}.")
                continue
            await send(f"Unrecognized key '{key}' for {cmd_words[0]} command.")
    except CancelledError:
        pass
    log(f"Disconnecting from {peer}.")
    writer.close()

async def run_command_server():
    host, port = command_endpoint.split(':', 2)
    server = await asyncio.start_server(handle_command_client, host, int(port))
    log(f"Listening for commands on {server.sockets[0].getsockname()}.")
    try:
        async with server:
            await server.serve_forever()
    except CancelledError:
        pass
    log(f"Shutting down command interface.")

##########################

# 
# Initialize Debezium (Kafka Connect Component)
#
requests.post(('http://%s/connectors' % debezium_endpoint),
    json={
        "name": "mysql-connector",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": mysql_host,
            "database.port": mysql_port,
            "database.user": mysql_user,
            "database.password": mysql_pass,
            "database.server.name": mysql_host,
            "database.server.id": '1234',
            "database.history.kafka.bootstrap.servers": kafka_endpoint,
            "database.history.kafka.topic": "mysql-history",
            "time.precision.mode": "connect"
        }
    }
)

#Initialize Kafka
producer = KafkaProducer(bootstrap_servers=[kafka_endpoint],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

exit_code = 0

try:
    with connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_pass,
    ) as connection:
        with connection.cursor() as cursor:
            initialize_database(connection, cursor)
            seed_data(connection, cursor)
            item_prices = get_item_prices(cursor)
            async def shutdown():
                for task in asyncio.Task.all_tasks():
                        task.cancel()
            async def run_async():
                event_loop = asyncio.get_event_loop()
                signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
                async def handle_signal(signal):
                    await shutdown()
                for s in signals:
                    event_loop.add_signal_handler(
                        s, lambda s=s: asyncio.create_task(handle_signal(s)))
                await asyncio.gather(
                    loop_purchases(connection, cursor, producer, item_prices),
                    loop_random_pageviews(producer),
                    run_command_server()
                )
            log("Starting simulation loops.")
            try:
                asyncio.run(run_async())
            except Error as e:
                log(e)
                exit_code = 1
            finally:
                try:
                    asyncio.run(shutdown())
                except Error:
                    pass
                connection.close()
except Error as e:
    log(e)
    exit_code = 1
finally:
    log("Finished.")

sys.exit(exit_code)
