import barnum, random, time, json, requests, math
from mysql.connector import connect, Error
from kafka import KafkaProducer

# CONFIG
userSeedCount      = 10000
itemSeedCount      = 1000
purchaseGenCount   = 1000
purchaseGenEveryMS = 5000
pageviewMultiplier = 75       # Translates to 75x purchases, currently 750/sec or 65M/day
itemInventoryMin   = 1000
itemInventoryMax   = 5000
itemPriceMin       = 5
itemPriceMax       = 500
mysqlHost          = 'mysql'
mysqlPort          = '3306'
mysqlUser          = 'root'
mysqlPass          = 'debezium'
kafkaHostPort      = 'redpanda:9092'
kafkaTopic         = 'pageviews'
debeziumHostPort   = 'debezium:8083'
channels           = ['organic search', 'paid search', 'referral', 'social', 'display']
categories         = ['widgets', 'gadgets', 'doodads', 'clearance']

# INSERT TEMPLATES
item_insert     = "INSERT INTO shop.items (name, category, price, inventory) VALUES ( %s, %s, %s, %s )"
user_insert     = "INSERT INTO shop.users (email, is_vip) VALUES ( %s, %s )"
purchase_insert = "INSERT INTO shop.purchases (user_id, item_id, quantity, purchase_price) VALUES ( %s, %s, %s, %s )"

#Initialize Debezium (Kafka Connect Component)
requests.post(('http://%s/connectors' % debeziumHostPort),
    json={
        "name": "mysql-connector",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": mysqlHost,
            "database.port": mysqlPort,
            "database.user": mysqlUser,
            "database.password": mysqlPass,
            "database.server.name": mysqlHost,
            "database.server.id": '1234',
            "database.history.kafka.bootstrap.servers": kafkaHostPort,
            "database.history.kafka.topic": "mysql-history",
            "time.precision.mode": "connect"
        }
    }
)

#Initialize Kafka
producer = KafkaProducer(bootstrap_servers=[kafkaHostPort],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

def generatePageview(viewer_id, target_id, page_type):
    return {
        "user_id": viewer_id,
        "url": f'/{page_type}/{target_id}',
        "channel": random.choice(channels),
        "received_at": int(time.time())
    }

try:
    with connect(
        host=mysqlHost,
        user=mysqlUser,
        password=mysqlPass,
    ) as connection:
        with connection.cursor() as cursor:
            print("Initializing shop database...")
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
            print("Seeding data...")
            cursor.executemany(
                item_insert,
                [
                    (
                        barnum.create_nouns(),
                        random.choice(categories),
                        random.randint(itemPriceMin*100,itemPriceMax*100)/100,
                        random.randint(itemInventoryMin,itemInventoryMax)
                    ) for i in range(itemSeedCount)
                ]
            )
            cursor.executemany(
                user_insert,
                [
                    (
                        barnum.create_email(),
                        (random.randint(0,10) > 8)
                     ) for i in range(userSeedCount)
                ]
            )
            connection.commit()

            print("Getting item ID and PRICEs...")
            cursor.execute("SELECT id, price FROM shop.items")
            item_prices = [(row[0], row[1]) for row in cursor]

            print("Preparing to loop + seed kafka pageviews and purchases")
            for i in range(purchaseGenCount):
                # Get a user and item to purchase
                purchase_item = random.choice(item_prices)
                purchase_user = random.randint(0,userSeedCount-1)
                purchase_quantity = random.randint(1,5)

                # Write purchaser pageview
                producer.send(kafkaTopic, key=str(purchase_user).encode('ascii'), value=generatePageview(purchase_user, purchase_item[0], 'products'))

                # Write random pageviews to products or profiles
                pageviewOscillator = int(pageviewMultiplier + (math.sin(time.time()/1000)*50))
                for i in range(pageviewOscillator):
                    rand_user = random.randint(0,userSeedCount)
                    rand_page_type = random.choice(['products', 'profiles'])
                    target_id_max_range = itemSeedCount if rand_page_type == 'products' else userSeedCount
                    producer.send(kafkaTopic, key=str(rand_user).encode('ascii'), value=generatePageview(rand_user, random.randint(0,target_id_max_range), rand_page_type))

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

                #Pause
                time.sleep(purchaseGenEveryMS/1000)

    connection.close()

except Error as e:
    print(e)
