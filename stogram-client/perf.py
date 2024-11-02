import socket
import json 
import random
import concurrent.futures 
import uuid 

class StogramSync:
    


    def __init__(self, host, port):
        print("Address: {}:{}".format(host,port))


        self.name = str(uuid.uuid4())
        self.host = host
        self.port = port 
        self.sock = None
        self.bytes_sent = 0
        self.bytes_received = 0
        self.connect()

    def connect(self):
        if not self.sock:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host,int(self.port)))
            resp = self.call(dict(event="register", subscriber=self.name))
            
        return self

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args, **kwargs):
        self.sock.close()
        self.sock = None
        print("Conncetion closed")

    def publish(self, topic, data):
        return self.call(dict(event="publish",topic=topic,message=json.dumps(data)))

    def call(self, obj):
        obj_bytes = json.dumps(obj).encode('utf-8')
        self.sock.sendall(obj_bytes)
        self.bytes_sent += len(obj_bytes)
        bytes_ = b''
        while True:
            bytes_ += self.sock.recv(4096)
            resp = bytes_.decode('utf-8')
            
            #resp = resp.strip("\r\n")
            
            try:
                resp_obj = json.loads(resp)
                
                self.bytes_received += len(resp)
                return resp_obj
            except Exception as ex:
                continue
            
    #return json.loads(resp)




import time 

time_start = time.time()

def task():
    host = "127.0.0.1"

    port = random.choice([8889,9000,9001])
    print("Address: {}:{}".format(host,port))


    client = StogramSync(host, 8889)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    time_start = time.time()
    number = 0
    sock.connect((host,port))
    with StogramSync(host, 7001) as client:
        while True:
            client.publish(topic="chat",data=dict(message="Haaaai",reader="user",writer="bot"))
          
    #        client.publish("chat",dict(message="Hoi",reader="user",writer="bot"))
            #obj = dict(event="chat",message=dict(message="Hello",Number=number),reader="user",writer="bot")
            #client.publish("chatz",obj)
            if number % 1000 == 0:
                time_start = time.time()
                if number != 0:
                    return 
            number += 1
            print(client.port, client.name, time.time() - time_start, number)


task()
exit(0)

with concurrent.futures.ProcessPoolExecutor() as executor:
    tasks = []
    for x in range(20):
        tasks.append(executor.submit(task))
    for task in concurrent.futures.as_completed(tasks):
        print(task.result())
