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
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import AutoMinorLocator # for minor ticks
import os
##########################################################################
#                                                                        #
#             DEFINES A FUNCTION WHICH BUILDS THE WORK PLAN              #
#                                                                        #
##########################################################################
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
                    "sample": path[1],
                    "s": path[2]

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
            url_list.append([filestring, val, s])
    
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

    list_of_results_dicts.append(results)

    print(f"The computation is {(len(list_of_results_dicts)/len(job_completion_dict)):.1%} complete" , flush = True)

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

fraction = 0.01

workplan, job_completion_dict = build_work_plan(fraction = fraction)
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
    

    
##########################################################################
#                                                                        #
#            LISTENING FOR THE RESULTS OF THE CALCULATIONS               #
#                                                                        #
##########################################################################


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='result_queue', on_message_callback=result_callback)

print("start listening for results", flush=True)

list_of_results_dicts = []

channel.start_consuming()


print("Printing item 1 in the list of result dicts")

print(list_of_results_dicts[0])

print("DONE")

data = {}
for s in samples:
    print(f"s = {s}")
    frames = []
    for result in list_of_results_dicts:
        
        print(f"comparing s [{s}] to the results sample tag [{result['sample']}]", flush = True)
        if s == result["sample"]:
            frames.append(result["result_data"])
    data[s] = ak.concatenate(frames)




# result_data = ak.concatenate(frames)

print("PRINTING RESULT DATA")

print(data, flush = True)

#closes the connection
connection.close()


##########################################################################
#                                                                        #
#                   DEFINES THE DATAPLOTTING FUNCTION                    #
#                                                                        #
##########################################################################


MeV = 0.001
GeV = 1.0

#lumi = 0.5 # fb-1 # data_A only
#lumi = 1.9 # fb-1 # data_B only
#lumi = 2.9 # fb-1 # data_C only
#lumi = 4.7 # fb-1 # data_D only
lumi = 10 # fb-1 # data_A,data_B,data_C,data_D

def plot_data(data):

    xmin = 80 * GeV
    xmax = 250 * GeV
    step_size = 5 * GeV

    bin_edges = np.arange(start=xmin, # The interval includes this value
                     stop=xmax+step_size, # The interval doesn't include this value
                     step=step_size ) # Spacing between values
    bin_centres = np.arange(start=xmin+step_size/2, # The interval includes this value
                            stop=xmax+step_size/2, # The interval doesn't include this value
                            step=step_size ) # Spacing between values

    data_x,_ = np.histogram(ak.to_numpy(data['data']['mllll']), 
                            bins=bin_edges ) # histogram the data
    data_x_errors = np.sqrt( data_x ) # statistical error on the data

    signal_x = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)']['mllll']) # histogram the signal
    signal_weights = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)'].totalWeight) # get the weights of the signal events
    signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color'] # get the colour for the signal bar

    mc_x = [] # define list to hold the Monte Carlo histogram entries
    mc_weights = [] # define list to hold the Monte Carlo weights
    mc_colors = [] # define list to hold the colors of the Monte Carlo bars
    mc_labels = [] # define list to hold the legend labels of the Monte Carlo bars

    for s in samples: # loop over samples
        if s not in ['data', r'Signal ($m_H$ = 125 GeV)']: # if not data nor signal
            mc_x.append( ak.to_numpy(data[s]['mllll']) ) # append to the list of Monte Carlo histogram entries
            mc_weights.append( ak.to_numpy(data[s].totalWeight) ) # append to the list of Monte Carlo weights
            mc_colors.append( samples[s]['color'] ) # append to the list of Monte Carlo bar colors
            mc_labels.append( s ) # append to the list of Monte Carlo legend labels
    


    # *************
    # Main plot 
    # *************
    main_axes = plt.gca() # get current axes
    
    # plot the data points
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                       fmt='ko', # 'k' means black and 'o' is for circles 
                       label='Data') 
    
    # plot the Monte Carlo bars
    mc_heights = main_axes.hist(mc_x, bins=bin_edges, 
                                weights=mc_weights, stacked=True, 
                                color=mc_colors, label=mc_labels )
    
    mc_x_tot = mc_heights[0][-1] # stacked background MC y-axis value
    
    # calculate MC statistical uncertainty: sqrt(sum w^2)
    mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])
    
    # plot the signal bar
    main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot, 
                   weights=signal_weights, color=signal_color,
                   label=r'Signal ($m_H$ = 125 GeV)')
    
    # plot the statistical uncertainty
    main_axes.bar(bin_centres, # x
                  2*mc_x_err, # heights
                  alpha=0.5, # half transparency
                  bottom=mc_x_tot-mc_x_err, color='none', 
                  hatch="////", width=step_size, label='Stat. Unc.' )

    # set the x-limit of the main axes
    main_axes.set_xlim( left=xmin, right=xmax ) 
    
    # separation of x axis minor ticks
    main_axes.xaxis.set_minor_locator( AutoMinorLocator() ) 
    
    # set the axis tick parameters for the main axes
    main_axes.tick_params(which='both', # ticks on both x and y axes
                          direction='in', # Put ticks inside and outside the axes
                          top=True, # draw ticks on the top axis
                          right=True ) # draw ticks on right axis
    
    # x-axis label
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                        fontsize=13, x=1, horizontalalignment='right' )
    
    # write y-axis label for main axes
    main_axes.set_ylabel('Events / '+str(step_size)+' GeV',
                         y=1, horizontalalignment='right') 
    
    # set y-axis limits for main axes
    main_axes.set_ylim( bottom=0, top=np.amax(data_x)*1.6 )
    
    # add minor ticks on y-axis for main axes
    main_axes.yaxis.set_minor_locator( AutoMinorLocator() ) 

    # Add text 'ATLAS Open Data' on plot
    plt.text(0.05, # x
             0.93, # y
             'ATLAS Open Data', # text
             transform=main_axes.transAxes, # coordinate system used is that of main_axes
             fontsize=13 ) 
    
    # Add text 'for education' on plot
    plt.text(0.05, # x
             0.88, # y
             'for education', # text
             transform=main_axes.transAxes, # coordinate system used is that of main_axes
             style='italic',
             fontsize=8 ) 
    
    # Add energy and luminosity
    lumi_used = str(lumi*fraction) # luminosity to write on the plot
    plt.text(0.05, # x
             0.82, # y
             '$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', # text
             transform=main_axes.transAxes ) # coordinate system used is that of main_axes
    
    # Add a label for the analysis carried out
    plt.text(0.05, # x
             0.76, # y
             r'$H \rightarrow ZZ^* \rightarrow 4\ell$', # text 
             transform=main_axes.transAxes ) # coordinate system used is that of main_axes

    # draw the legend
    main_axes.legend( frameon=False ) # no box around the legend
    
    # plt.savefig("hzz_plot.png", dpi=300, bbox_inches="tight")
    # Save the plot to the specified directory within the container
    os.makedirs('/app/data/static', exist_ok=True)
    plt.savefig('/app/data/static/plot.png', dpi=300, bbox_inches="tight")
    return


print("ATTEMPTING TO PLOT", flush = True)


plot_data(data)


print("PLOT SAVED", flush = True)


