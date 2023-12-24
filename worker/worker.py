#!/usr/bin/env python
import pika
import time
from datetime import datetime

connection = pika.BlockingConnection(pika.ConnectionParameters('distributed_computing_project-rabbitmq-1', heartbeat=0))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

# Purge the queue
channel.queue_purge(queue='task_queue')

def callback(ch, method, properties, body):
    time.sleep(2)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f" [x] Received {body.decode()} at {current_time}")


    print(f"[x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()





# import pika
# from datetime import datetime
# import time

# # when RabbitMQ is running on localhost
# #params = pika.ConnectionParameters('localhost')

# # when RabbitMQ broker is running on network
# #params = pika.ConnectionParameters('rabbitmq')

# # when starting services with docker compose
# params = pika.ConnectionParameters(
#     'distributed_computing_project-rabbitmq-1',
#     heartbeat=0)

# # create the connection to broker
# connection = pika.BlockingConnection(params)
# channel = connection.channel()

# # create the queue, if it doesn't already exist
# channel.queue_declare(queue='messages', durable=True, exclusive=False)

# # define a function to call when message is received
# def callback(ch, method, properties, body):
#     time.sleep(2)
#     # Get the current system time
#     current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     print(f" [x] Received {body} was recieved at {current_time}")


# # setup to listen for messages on queue 'messages'
# channel.basic_consume(queue='messages',
#                       auto_ack=True,
#                       on_message_callback=callback)

# # log message to show we've started
# print('Waiting for messages. To exit press CTRL+C')

# # start listening
# channel.start_consuming()
