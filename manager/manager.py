#!/usr/bin/env python

import pika
import sys
import infofile
import uproot
import uuid
import aiohttp
import requests
import pickle
import awkward as ak # to represent nested data in columnar format

##########################################################################
#                                                                        #
#             DEFINES A FUNCTION WHICH BUILDS THE WORK PLAN              #
#                                                                        #
##########################################################################

#a functions which builds a plan for all of the work which needs to be completed
def build_work_plan(batch_size = 200, fraction = 0.01):
    url_list = url_builder()
    #the url list which is returned in the following form [[url1, val1], [url2, val2], ...]
    work_plan = []
    job_completion_dict = {}
    for path in url_list:
        #note that:
        #path[0]  = the url
        #path[1] = the sample (aka val)#
        
        #finds how many events are stored under a certain URL
        with uproot.open(path[0] + ":mini") as tree:
            numevents = tree.num_entries # number of events

            #limits the total number of events calculated by multiplying the true numevents by the fraction
            numevents = int(numevents*fraction)

            # Iterate over events and create batches
            for start_index in range(0, numevents, batch_size):
                finish_index = min(start_index + batch_size -1, numevents)
                
                #uses uuid to generate a unique idintifier for each job
                unique_id = str(uuid.uuid4())  # Generate a unique identifier
                #creates a bath_info dictionary 
                batch_info = {
                    "job_id": unique_id,
                    "url": path[0],
                    "start_index": start_index,
                    "finish_index": finish_index,
                    "sample": path[1]
                }
                # print(f"Batch Info: {batch_info}")
                work_plan.append(batch_info)

                job_completion_dict[unique_id] = False

            #print(f"Under {path}\nthere were {numevents} events")
    return work_plan, job_completion_dict

##########################################################################
#                                                                        #
#            DEFINES A FUNCTION WHICH BUILDS A LIST OF URLS              #
#                                                                        #
##########################################################################

#A function which builds a list of all of the URLS which need to be iterated over
def url_builder():
    #defining samples struct
    samples = {

        'data': {
            'list' : ['data_A','data_B','data_C','data_D'],
        },

        r'Background $Z,t\bar{t}$' : { # Z + ttbar
            'list' : ['Zee','Zmumu','ttbar_lep'],
            'color' : "#6b59d3" # purple
        },

        r'Background $ZZ^*$' : { # ZZ
            'list' : ['llll'],
            'color' : "#ff0000" # red
        },

        r'Signal ($m_H$ = 125 GeV)' : { # H -> ZZ -> llll
            'list' : ['ggH125_ZZ4lep','VBFH125_ZZ4lep','WH125_ZZ4lep','ZH125_ZZ4lep'],
            'color' : "#00cdff" # light blue
        },

    }
    tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/" # web address

    url_list = []

    #loops through samples and builds the filestrings
    for s in samples: # loop over samples
        #frames = [] # define empty list to hold data
        for val in samples[s]['list']: # loop over each file
            
            if s == 'data': prefix = "Data/" # Data prefix
            else: # MC prefix
                prefix = "MC/mc_"+str(infofile.infos[val]["DSID"])+"."
            filestring = tuple_path+prefix+val+".4lep.root" # file name to open
            url_list.append([filestring, val])
    
    #returns a list of lists in the following form [[url1, val1], [url2, val2], ...]
    return url_list

##########################################################################
#                                                                        #
#            ESTABLISHES CONNECTION AND SENDS JOBS TO QUEUE              #
#                                                                        #
##########################################################################


#establishing a connection and channels
connection = pika.BlockingConnection(pika.ConnectionParameters('distributed_computing_project-rabbitmq-1', heartbeat=0))

channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=False)
# channel.queue_declare(queue='result_queue', durable=False)

#defining a function for the results callback
def result_callback(ch, method, properties, body):
    
    results = pickle.loads(body)
    
    #print(f"The results of calculation: {results['job_id']} have been received", flush = True)

    frames.append(results["result_data"])

    print(f"The computation is {(len(frames)/len(job_completion_dict)):.1%} complete" , flush = True)

    job_completion_dict[results["job_id"]] = True



    # # Print the first 20 items
    # for index, (key, value) in enumerate(job_completion_dict.items()):
    #     print(f"{key}: {value}", flush = True)
    #     if index == 19:
    #         break  # Stop after printing the first 20 items

    #checks to see if all of the jobs have been recieved
    all_received = all(job_completion_dict.values())

    if all_received:
        print("All 'recieved' values are True.")
        ch.stop_consuming()
        print("stopped consuming", flush = True)
    # else:
    #     print("Not all 'recieved' values are True.")


    ch.basic_ack(delivery_tag=method.delivery_tag)

#creates a workplan

print("WORKPLAN FUNCTION CALLED")
workplan, job_completion_dict = build_work_plan()
print(f"There are {len(workplan)} items in the workplan")

#serialises the first 20 items from the work plan and sends these to the queue
for instruction in workplan:
    serialised_instruction = pickle.dumps(instruction)

    #sends a message down the channel
    channel.basic_publish(exchange='',
                        routing_key='task_queue',
                        body=serialised_instruction,
                        properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent))
    

    
    #print(f"message {i} was sent succesfully from the manager node")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='result_queue', on_message_callback=result_callback)

print("start listening for results", flush=True)

frames = []

channel.start_consuming()

result_data = ak.concatenate(frames)

print("PRINTING RESULT DATA")

print(result_data, flush = True)

#closes the connection
connection.close()

