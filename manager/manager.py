#!/usr/bin/env python

import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters('distributed_computing_project-rabbitmq-1', heartbeat=0))

channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

for message_number in range(0,100):
    #message = ' '.join(sys.argv[1:]) or "Hello World!"
    message = f"message {message_number}"
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent
        ))
    print(f" [x] Sent {message}")


connection.close()





# import pika
# import infofile

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

# # # send a simple message
# # channel.basic_publish(exchange='',
# #                       routing_key='messages',
# #                       body='Hello!')

# # log message sending
# print("Message sent")


# #############################################################################################

# for i in range (0,99):
#     message = f"message {i}"
#     channel.basic_publish(exchange='',
#                           routing_key='messages',
#                           body=message,
#                           properties=pika.BasicProperties(
#                           delivery_mode = 2, # make message persistent
#                           ))



# #defining samples struct
# samples = {

#     'data': {
#         'list' : ['data_A','data_B','data_C','data_D'],
#     },

#     r'Background $Z,t\bar{t}$' : { # Z + ttbar
#         'list' : ['Zee','Zmumu','ttbar_lep'],
#         'color' : "#6b59d3" # purple
#     },

#     r'Background $ZZ^*$' : { # ZZ
#         'list' : ['llll'],
#         'color' : "#ff0000" # red
#     },

#     r'Signal ($m_H$ = 125 GeV)' : { # H -> ZZ -> llll
#         'list' : ['ggH125_ZZ4lep','VBFH125_ZZ4lep','WH125_ZZ4lep','ZH125_ZZ4lep'],
#         'color' : "#00cdff" # light blue
#     },

# }

# #defining the tuple path
# tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/" # web address

# #getting
# def get_data_from_files():
#     data = {} # define empty dictionary to hold awkward arrays
#     for s in samples: # loop over samples
        
#         print(f"s = {s}")
        
#         print('Processing '+s+' samples') # print which sample
#         #frames = [] # define empty list to hold data
#         for val in samples[s]['list']: # loop over each file
            
#             print(f"val = {val}")
            
#             if s == 'data': prefix = "Data/" # Data prefix
#             else: # MC prefix
#                 prefix = "MC/mc_"+str(infofile.infos[val]["DSID"])+"."
#             fileString = tuple_path+prefix+val+".4lep.root" # file name to open

#            # send a simple message
            
#             for i in range (0,4):
#                 channel.basic_publish(exchange='',
#                                     routing_key='messages',
#                                     body=fileString)
           
           
#         #     temp = read_file(fileString,val) # call the function read_file defined below
#         #     frames.append(temp) # append array returned from read_file to list of awkward arrays
#         # data[s] = ak.concatenate(frames) # dictionary entry is concatenated awkward arrays
    
#     #return data # return dictionary of awkward arrays
# get_data_from_files()
