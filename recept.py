import paho.mqtt.client as subscribe
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from time import sleep

broker = "test.mosquitto.org"
topic = "horta/in2ici5afdn/um1d4d3_S1"

db = "soil_moisture.db"

now = datetime.now(ZoneInfo("America/Sao_Paulo"))

now_text = now.strftime("%Y %m %d %H:%m")

def create_table():
    connection = sqlite3.connect(db)
    
    cursor = connection.cursor()
    
    print("Database created and connected successfully!")
    
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS sensor_soil_moisture (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        value TEXT NOT NULL,
        time TEXT
    );
    '''
    
    cursor.execute(create_table_query)

    connection.commit()

    print("Table 'Students' created successfully!")
    
create_table()

values = ""

def on_message(client, userdata, msg):
    global values
    values = msg.payload.decode("utf-8")
    
client = subscribe.Client()
client.on_message = on_message

client.connect(broker)
client.subscribe(topic)
client.loop_start()


while True:
    try: 
        if values is not None:       
            now = datetime.now(ZoneInfo("America/Sao_Paulo"))
            now_text = now.strftime("%Y-%m-%d %H:%M:%S")
            
            with sqlite3.connect(db) as connection:
                cursor = connection.cursor()
                insert_query = '''
                INSERT INTO sensor_soil_moisture (value, time) 
                VALUES (?, ?);
                '''
                cursor.execute(insert_query, (values, now_text))
                connection.commit()

            print(f"Valor recebido: {values} em {now_text}")
            
            values = ""
    except Exception as e:
        print(f"Erro ao receber/inserir dado: {e}")

    sleep(10)



# while True:
#     msg = subscribe.simple(topic, hostname= broker)
#     print("%s" % (msg.payload.decode("utf-8"))[:2])
# print(type((msg.payload.decode("utf-8"))))


# client = subscribe.Client()
# client
