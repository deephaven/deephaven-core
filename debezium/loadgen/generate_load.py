import asyncio
import barnum
import concurrent.futures
import json
import math
import random
import requests
import signal
import os
import socket
import sys
import time
from collections import deque
from concurrent.futures import CancelledError
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from mysql.connector import connect, Error
from typing import Deque

# CONFIG
user_seed_count     = 10000
item_seed_count     = 1000
max_parallel_purchases = int(os.environ['MAX_PARALLEL_PURCHASES'])
max_parallel_pageviews = int(os.environ['MAX_PARALLEL_PAGEVIEWS'])
purchases_per_second_key = 'purchases_per_second'
pageviews_per_second_key = 'pageviews_per_second'
kafka_partitions         = int(os.environ['KAFKA_PARTITIONS'])
kafka_producer_acks      = int(os.environ['KAFKA_PRODUCER_ACKS'])
kafka_batch_size         = int(os.environ['KAFKA_BATCH_SIZE'])
kafka_linger_ms          = 40  # Note we flush manually, so this only applies in between our managed batches.
params              = {  purchases_per_second_key : int(os.environ['PURCHASES_PER_SECOND_START']),
                         pageviews_per_second_key : int(os.environ['PAGEVIEWS_PER_SECOND_START']) }
command_endpoint    = os.environ['COMMAND_ENDPOINT']
log_period_seconds  = 10
rate_check_seconds  = 20
item_inventory_min  = 1000
item_inventory_max  = 5000
min_sleep_seconds   = 0.010
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
    cursor.execute("SELECT id, price FROM shop.items")
    return [(row[0], row[1]) for row in cursor]

def calc_max_burst(rate_s, max_parallel):
    return math.ceil(1.0 * rate_s / max_parallel)

class PeriodRateTracker:
    start_ts: float   # Timestamp for beginning of period; set on creation
    sent: int         # Count of messages sent in the period.
    def __init__(self, start_ts: float = 0.0, initial_sent: int = 0):
        self.reset(start_ts, initial_sent)
    def reset(self, start_ts: float, initial_sent: int):
        self.start_ts = start_ts
        self.sent = initial_sent

# Class to support tracking rate of messages sent.
class RateTracker:
    max_periods: int                       # Total number of periods to keep
    period_window_s: float                 # Size of time window per period, in s.
    total_sent: int                        # Total messages sent across all trackers
    # We keep trackers in a deque; when we send messages we add to the individual
    # period tracker's sent count and to our total; when a period expires we remove the
    # period tracker and its sent count from the total.  This way we keep a
    # rolling window of approximately (max_periods * period_window_s).
    period_trackers: Deque['RateTracker']
    free_trackers: list                    # We keep unused trackers in a free list

    def __init__(self, max_periods: int, period_window_s: float, now: float):
        self.max_periods = max_periods
        self.period_window_s = period_window_s
        self.period_trackers = deque(maxlen = max_periods)
        self.period_trackers.append(PeriodRateTracker(now, 0))
        self.free_trackers = [ PeriodRateTracker() for _ in range(max_periods) ]
        self.total_sent = 0
    
    def track(self, now: float, sent: int):
        self.total_sent += sent
        period_tracker = self.period_trackers[-1]
        if now - period_tracker.start_ts < self.period_window_s:
            period_tracker.sent += sent
            return

        if len(self.period_trackers) < self.max_periods:
           period_tracker = self.free_trackers.pop()
           period_tracker.reset(now, sent)
           self.period_trackers.append(period_tracker)
           return

        period_tracker = self.period_trackers.popleft()
        self.total_sent -= period_tracker.sent
        period_tracker.reset(now, sent)
        self.period_trackers.append(period_tracker)

    def reset(self, now: float):
        self.total_sent = 0
        while len(self.period_trackers) > 1:
            self.free_trackers.append(self.period_trackers.popleft())
        t = self.period_trackers[0]
        t.start_ts = now
        t.sent = 0

    def read(self, now: float):
        return self.period_trackers[0].start_ts, self.total_sent

    def steady_state(self):
        return len(self.period_trackers) == self.max_periods

def rate_tracker_for_rate(rate_s: float, now: float):
    rt_period_window_s = max(1.0/rate_s, 0.1)
    rt_max_periods = min(10, math.ceil(10.0/rt_period_window_s))
    return RateTracker(rt_max_periods, rt_period_window_s, now)

# Main async loop for executing a given action type while rate controlling it.
# The action fun should accept a count argument allowing this function to size batches.
async def loop(executor, max_parallel: int, executor_action_fun, param_key, action_desc: str):
    now = time.time()
    last_log = now              # Timestamp for the last time we logged summary information about events sent.
    sent_since_last_log = 0     # Number of events sent not included in the last summary log.
    last_rate_check = now       # Timestamp for the last time we checked actual events sent versus rate.
    burst_cap_last_log = 0      # If we limited the max burst since the last summary log, the count that hit the cap; zero otherwise.
    rate_s = params[param_key]  # Target actions per second.
    rate_measure_window = 2     # Time in second to accumulate samples to estimate rate.
    # limit rate if we are falling too far behind.
    max_burst = calc_max_burst(rate_s, max_parallel)
    old_rate_s = rate_s         # Used to detect if rate_s has changed.
    period_s = 1.0/rate_s
    rate_tracker = rate_tracker_for_rate(rate_s, now)
    log(f"Simulating {action_desc} actions with an initial rate of {rate_s}/s, max {max_parallel} in parallel; " +
        f"tracking rate using {rate_tracker.max_periods} periods of {rate_tracker.period_window_s} seconds.")
    sending_threads = 0
    delta_per_thread = 0
    futures = []                # Dictionary where we accumulate unfinished futures scheduled on the executor.
    def reap_futures():         # Remove from futures the ones already finished, returning how many where finished.
        not_done = []
        actions_done = 0
        for future in futures:
            if future.done():
                dt, count = future.result()
                actions_done += count
            else:
                not_done.append(future)
        return not_done, actions_done
    event_loop = asyncio.get_event_loop()
    try:
        while True:
            futures, actions_done = reap_futures()
            while len(futures) > max_parallel - 1:
                await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
                futures, actions_done = reap_futures()
            now = time.time()
            tracker_start, tracker_sent = rate_tracker.read(now)
            expected_count_by_now = round((now - tracker_start) / period_s)
            available_threads = max_parallel - len(futures)
            delta_count = expected_count_by_now - tracker_sent
            if delta_count > 0:
                delta_per_thread = math.ceil(delta_count / available_threads)
                if delta_per_thread < 1:
                    delta_per_thread = 1
                elif delta_per_thread > max_burst:
                    burst_cap_last_log = delta_per_thread
                    delta_per_thread = max_burst
                    delta_count = max_burst * available_threads

                sent_since_last_log += delta_count
                sending_threads = 0
                while delta_count > 0:
                    action_count = min(delta_count, delta_per_thread)
                    future = event_loop.run_in_executor(executor, executor_action_fun, action_count)
                    now = time.time()
                    rate_tracker.track(now, action_count)
                    futures.append(future)
                    sending_threads += 1
                    delta_count -= action_count

            last_log_diff = now - last_log
            if last_log_diff >= log_period_seconds:
                qlen = len(futures)
                log(f"Simulated {sent_since_last_log} {action_desc} actions in the last {last_log_diff:.1f} seconds, " +
                    f"effective rate {sent_since_last_log / last_log_diff:.2f}/s; " +
                    f"on last trigger threads total sending={qlen}, new={sending_threads}, delta_per_thread={delta_per_thread}, burst cap={burst_cap_last_log}.")
                last_log = now
                sent_since_last_log = 0
                burst_cap_last_log = 0

            next_time = tracker_start + (tracker_sent + delta_count + 1)*period_s

            last_rate_check_diff = now - last_rate_check
            if last_rate_check_diff >= rate_check_seconds:
                if not rate_tracker.steady_state():
                    last_rate_check += 1.0
                    log(f"Warning: rate tracker not in steady state when scheduled to check rate: len(rate_tracker.period_trackers)={len(rate_tracker.period_trackers)}.")
                else:
                    now = time.time()
                    tracker_start, tracker_sent = rate_tracker.read(now)
                    dt = now - tracker_start
                    last_rate_check = now
                    effective_rate_s = tracker_sent / dt
                    if abs(effective_rate_s - rate_s)/rate_s > 0.02:
                        log(f"Warning: too far from target rate {rate_s}/s; " +
                            f"sent {tracker_sent} in the last {dt:.1f} seconds for an effective rate of {effective_rate_s:.2f}/s.")

            sleep_s = next_time - time.time()
            if sleep_s < min_sleep_seconds:
                sleep_s = min_sleep_seconds
            await asyncio.sleep(sleep_s)

            rate_s = params[param_key]
            if old_rate_s != rate_s:
                log(f"Changing {action_desc} rate from {old_rate_s} to {rate_s} per second.")
                period_s = 1.0/rate_s
                old_rate_s = rate_s
                now = time.time()
                rate_tracker = rate_tracker_for_rate(rate_s, now)
                last_rate_check = now
                max_burst = calc_max_burst(rate_s, max_parallel)
    except CancelledError:
        pass
    log(f"Stopped simulating {action_desc} actions.")

def executor_make_purchases(count):
    start_time = time.time()
    partition = random.randint(0, kafka_partitions - 1)
    for i in range(count):
        purchase_item = random.choice(executor_item_prices)
        purchase_user = random.randint(0, user_seed_count - 1)
        purchase_quantity = random.randint(1,5)

        executor_producer.send(
            kafka_topic,
            partition=partition,
            key=str(purchase_user).encode('ascii'),
            value=generate_pageview(purchase_user, purchase_item[0], 'products')
        )

        # Write purchase row
        executor_cursor.execute(
            purchase_insert,
            (
                purchase_user,
                purchase_item[0],
                purchase_quantity,
                purchase_item[1] * purchase_quantity
            )
        )
        executor_connection.commit()
        executor_producer.flush()
    dt = time.time() - start_time
    return dt, count

async def loop_purchases(executor):
    log("Preparing to loop to send purchases and associated pageviews.")
    await loop(executor, max_parallel_purchases, executor_make_purchases, purchases_per_second_key, "purchase")

def executor_make_pageviews(count):
    start_time = time.time()
    partition = random.randint(0, kafka_partitions - 1)
    for i in range(count):
        rand_user = random.randint(0, user_seed_count - 1)
        rand_page_type = random.choice(['products', 'profiles'])
        target_id_max_range = item_seed_count if rand_page_type == 'products' else user_seed_count
        executor_producer.send(
            kafka_topic,
            partition=partition,
            key=str(rand_user).encode('ascii'),
            value=generate_pageview(rand_user, random.randint(0, target_id_max_range - 1), rand_page_type)
        )
    executor_producer.flush()
    dt = time.time() - start_time
    return dt, count

async def loop_random_pageviews(executor):
    # Write random pageviews to products or profiles
    log("Preparing to loop to send non-purchase associated pageviews.")
    await loop(executor, max_parallel_pageviews, executor_make_pageviews, pageviews_per_second_key, "pageview")

def chomp(s):
    return s if not s.endswith(os.linesep) else s[:-len(os.linesep)]
#
# Simple socket-based command interface, to control message rate externally.
# Sample use from the command line:
#
# $ nc localhost 8090
# LOADGEN Connected.
# set pageviews_per_second 300000
# Setting pageviews_per_second: old value was 200000, new value is 300000.
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
    await writer.wait_closed()

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

# We use process-based executors to run purchase and pageview actions;
# Each has its own kafka producer; purchase action executors also need
# a database connection.
# Note these variables and the methods around initializing and releasing the
# resources associated to them are not used from main, but only from the
# code run by the forked process executor processes.
executor_producer = None
executor_cursor = None
executor_connection = None
executor_item_prices = None
def executor_init_resources(kafka_only = False):
    global executor_producer, executor_cursor, executor_connection, executor_item_prices
    if not kafka_only:
        executor_connection = connect(host=mysql_host, user=mysql_user, password=mysql_pass)
        executor_cursor = executor_connection.cursor()
        executor_item_prices = get_item_prices(executor_cursor)

    executor_producer = KafkaProducer(
        bootstrap_servers = [kafka_endpoint],
        acks = kafka_producer_acks,
        batch_size = kafka_batch_size,
        max_in_flight_requests_per_connection = max_parallel_purchases + max_parallel_pageviews,
        linger_ms = kafka_linger_ms,
        value_serializer = lambda x: json.dumps(x).encode('utf-8')
    )

# In the current version of concurrent.futures there is no clean and
# easy way to register tear down code; as it stands our code will
# cleanup the kafka and database resources only as the processes
# started by process executor finish.
def executor_release_resources():
    global executor_producer, executor_cursor, executor_connection
    for x in (executor_producer, executor_cursor, executor_connection):
        if x is not None:
            x.close()
    executor_producer = executor_cursor = executor_connection = None

# main sets up the initial database and redpanda state,
# and creates executor pools where the actions are executed async.
# We use process executors, so the actions run in separate
# processes forked from the parent that runs main.
# Main schedules actions on the executor processes asynchronously,
# monitoring effective rates and trying to keep the
# requested action rate.
def main():
    # Initialize Debezium (Kafka Connect Component)
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

    exit_code = 0
    try:
        # Setup our topic; necessary for num_partitions > 1.
        KafkaAdminClient(bootstrap_servers=kafka_endpoint).create_topics([
            NewTopic(
                name="pageviews",
                num_partitions=kafka_partitions,
                replication_factor=1
            )
        ])

        # With python 3.10 we will be able to use parenthesis around
        # 'with' with multiple 'as', and get rid
        # of the backslashes.
        with \
             connect(
                 host=mysql_host, user=mysql_user, password=mysql_pass
             ) as connection, \
             connection.cursor() as cursor \
        :
            initialize_database(connection, cursor)
            seed_data(connection, cursor)

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
            # With python 3.10 we will be able to use parenthesis around
            # 'with' with multiple 'as', and get rid
            # of the backslashes.
            with \
                concurrent.futures.ProcessPoolExecutor(
                    max_workers = max_parallel_purchases,
                    initializer = lambda: executor_init_resources(kafka_only = False)
                ) as purchases_executor, \
                concurrent.futures.ProcessPoolExecutor(
                    max_workers = max_parallel_pageviews,
                    initializer = lambda: executor_init_resources(kafka_only = True)
                ) as pageviews_executor \
            :
                await asyncio.gather(
                    loop_purchases(purchases_executor),
                    loop_random_pageviews(pageviews_executor),
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
    except Error as e:
        log(e)
        exit_code = 1
    finally:
        log("Finished.")

    sys.exit(exit_code)

if __name__ == '__main__':
    main()
