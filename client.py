import socket
import json
import logging
import uuid

logging.basicConfig(format='%(asctime)s %(message)s')
logger=logging.getLogger()
  
#Setting the threshold of logger to INFO
logger.setLevel(logging.INFO)

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = '127.0.0.1'
ADDR = (SERVER, PORT)
CLIENT_ID = str(uuid.uuid4())

mining_authorize = {"client_id":CLIENT_ID,"id":str(uuid.uuid4()),"method":"mining.authorize","params":["uname","password"]}
mining_subscribe = {"client_id":CLIENT_ID,"id":str(uuid.uuid4()),"method":"mining.subscribe","params":[str(uuid.uuid1()),str(uuid.uuid1()),str(uuid.uuid1().hex),"4"]}

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)
logger.info(f"[CONNECTION] Connected to {SERVER}")

connected = True
authorized = False
subscribed = False

def send(msg):
    """
    Desc: Send requests
    Args: Message to send in JSON format
    Returns: None
    """
    logger.info(f"[REQUEST] Sending {msg['method']} request to {SERVER}")
    msg = json.dumps(msg)
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    response = json.loads(client.recv(2048).decode(FORMAT))
    if response['error'] == 'null':
        logger.info("[RESPONSE] " + json.dumps(response))

while connected:
    if not subscribed:
        send(mining_subscribe)
        subscribed = True
    if not authorized:
        send(mining_authorize)
        authorized = True
    
    