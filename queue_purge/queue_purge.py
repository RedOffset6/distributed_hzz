import pika

#This file contains a function which purges the queue before the other containers are opened

def purge_queue():
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('distributed_computing_project-rabbitmq-1', heartbeat=0))
    channel = connection.channel()

    # Purge the queue
    channel.queue_purge(queue='task_queue')

    # Close the connection
    connection.close()
    print("Queue Purged")

purge_queue()



