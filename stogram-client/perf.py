import socket
import json 
import random
import concurrent.futures 
import uuid 
from stogram_client.sync import Client as StogramSync




import time 

time_start = time.time()

def task():
    host = "127.0.0.1"

    port = random.choice([7001])
    print("Address: {}:{}".format(host,port))


    client = StogramSync(host, 7001)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    time_start = time.time()
    number = 0
    sock.connect((host,port))
    with StogramSync(host, 7001) as client:
        while True:
            client.publish(topic="test",data=dict(message="Haaaai",reader="user",writer="bot"))
          
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
