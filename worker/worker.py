#!/usr/bin/env python
import pika
import time
from datetime import datetime
import pickle

##########################################################################
#                                                                        #
#             ESTABLISHES CONNECTION AND HANDLES THE CALLBACK            #
#                                                                        #
##########################################################################

connection = pika.BlockingConnection(pika.ConnectionParameters('distributed_computing_project-rabbitmq-1', heartbeat=0))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    time.sleep(2)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    instruction = pickle.loads(body)

    print(f" The following instructuion was recieved at {current_time} :\n{instruction}\n ")


    print(f"[x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()