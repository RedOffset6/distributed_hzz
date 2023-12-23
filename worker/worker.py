#!/usr/bin/env python
import pika
from datetime import datetime
import time

# when RabbitMQ is running on localhost
#params = pika.ConnectionParameters('localhost')

# when RabbitMQ broker is running on network
#params = pika.ConnectionParameters('rabbitmq')

# when starting services with docker compose
params = pika.ConnectionParameters(
    'distributed_computing_project-rabbitmq-1',
    heartbeat=0)

# create the connection to broker
connection = pika.BlockingConnection(params)
channel = connection.channel()

# create the queue, if it doesn't already exist
channel.queue_declare(queue='messages')

# define a function to call when message is received
def callback(ch, method, properties, body):
    # Get the current system time
    time.sleep(4)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f" [x] Received {body} was recieved at {current_time}")

# setup to listen for messages on queue 'messages'
channel.basic_consume(queue='messages',
                      auto_ack=True,
                      on_message_callback=callback)

# log message to show we've started
print('Waiting for messages. To exit press CTRL+C')

# start listening
channel.start_consuming()
