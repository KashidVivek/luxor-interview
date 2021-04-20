#######################################################
# IMPORTS
#######################################################

import socket 
import threading
import json
import uuid
import psycopg2
import logging

logging.basicConfig(format='%(asctime)s %(message)s')

logger=logging.getLogger()
  
#Setting the threshold of logger to INFO
logger.setLevel(logging.INFO)

HEADER = 64
PORT = 5050
SERVER = '127.0.0.1'
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
TABLES = []

def connect_db():
    """
    Desc: Connecting to POSTGRE-SQL DB
    Args: None
    Returns: Connection on Success
    """
    con = psycopg2.connect(database="luxor", user="luxor", password="luxor", host="127.0.0.1", port="5433")
    if con:
        logger.info("[DATABASE] Connected to DB..")
        return con
    return False


def check_tables(con):
    """
    Desc: Check for pre-existing tables in POSTGRE-SQL DB
    Args: DB connection
    Returns: None
    """
    cursor = con.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'""")
    for table in cursor.fetchall():
        table_name = table[0]
        TABLES.append(table_name)

    if "AUTH_REQUESTS".lower() not in TABLES:
        cursor.execute('''CREATE TABLE AUTH_REQUESTS
        (ID VARCHAR(150)   PRIMARY KEY   NOT NULL,
        USERNAME  VARCHAR(64)  NOT NULL,
        PASSWORD  VARCHAR(64));''')
        logger.info( "[DATABASE] AUTH_REQUESTS Table created successfully")
        con.commit()

    if "SUBSCRIPTION_VALUES".lower() not in TABLES:
        cursor.execute('''CREATE TABLE SUBSCRIPTION_VALUES
        (ID VARCHAR(150)   PRIMARY KEY   NOT NULL,
        SUB1 VARCHAR(150)  NOT NULL,
        SUB2  VARCHAR(150) NOT NULL,
        E1 VARCHAR(150)  NOT NULL,
        E2  VARCHAR(150) NOT NULL);''')
        logger.info( "[DATABASE] SUBSCRIPTION_VALUES Table created successfully")
        con.commit()

    if "CLIENTS".lower() not in TABLES:
        cursor.execute('''CREATE TABLE CLIENTS
        (ID SERIAL PRIMARY KEY  NOT NULL,
        CLIENT_ID VARCHAR(150)  NOT NULL,
        REQUEST_ID  VARCHAR(150) NOT NULL,
        REQUEST_TYPE  VARCHAR(150) NOT NULL);''')
        logger.info( "[DATABASE] CLIENTS Table created successfully")
        con.commit()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

def handle_client(conn, addr):
    """
    Desc: Handling incoming Requests
    Args: Connection obj and Address bound
    Returns: None
    """
    logger.info(f'[NEW CONNECTION] {addr} connected.')

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            struct_msg = json.loads(msg)
            if struct_msg['method'] == 'mining.authorize':
                response = mining_authorize(struct_msg['client_id'],struct_msg['id'],'mining.authorize',struct_msg['params'])
                conn.send(response.encode(FORMAT))
            
            if struct_msg['method'] == 'mining.subscribe':
                response = mining_subscribe(struct_msg['client_id'],struct_msg['id'],'mining.subscribe',struct_msg['params'])
                conn.send(response.encode(FORMAT))

            if msg == DISCONNECT_MESSAGE:
                connected = False

    conn.close()

def mining_authorize(client_id,id,method,params):
    """
    Desc: Handle mining.authorize requests
    Args: client Id, request id, method type and params
    Returns: None
    """
    
    cursor = connection.cursor()
    username,password = params
    if not username or not password:
        return json.dumps({"id":id,"response":"false","error":"Invalid credentials"})

    query = f"INSERT INTO CLIENTS(CLIENT_ID, REQUEST_ID,REQUEST_TYPE) VALUES ('{client_id}', '{id}','{method}')"
    cursor.execute(query)
    connection.commit()
    
    query = f"INSERT INTO AUTH_REQUESTS(ID, USERNAME, PASSWORD) VALUES ('{id}', '{username}','{password}')"
    cursor.execute(query)
    connection.commit()

    return json.dumps({"id":id,"response":"true","error":"null"})

def mining_subscribe(client_id,id,method,params):
    """
    Desc: Handle mining.subscribe requests
    Args: client Id, request id, method type and params
    Returns: None
    """
    cursor = connection.cursor()
    subscription1,subscription2,extranonce1,extranonce2 = params

    query = f"INSERT INTO CLIENTS(CLIENT_ID, REQUEST_ID,REQUEST_TYPE) VALUES ('{client_id}', '{id}','{method}')"
    cursor.execute(query)
    connection.commit()

    query = f"INSERT INTO SUBSCRIPTION_VALUES(ID, SUB1, SUB2, E1, E2) VALUES ('{id}', '{subscription1}','{subscription2}','{extranonce1}', '{extranonce2}')"
    cursor.execute(query)
    connection.commit()
    return json.dumps({"id":id,"result":[str(uuid.uuid4)[-5:],extranonce1[-5:]],"error":"null"})

def start():
    """
    Desc: Start the server
    Args: None
    Returns: None
    """
    server.listen()
    logger.info(f"[LISTENING] Server is listening on {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        logger.info(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")


logger.info("[STARTING] server is starting...")

connection = connect_db()
if connection:
    check_tables(connection)
start()