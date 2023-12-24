import infofile
import uproot
import uuid

#a functions which builds a plan for all of the work which needs to be completed
def build_work_plan(url_list, batch_size = 100):
    url_list = url_builder()
    work_plan = []
    for path in url_list:
        #finds how many events are stored under a certain URL
        with uproot.open(path[0] + ":mini") as tree:
            numevents = tree.num_entries # number of events

            # Iterate over events and create batches
            for start_index in range(0, numevents, batch_size):
                finish_index = min(start_index + batch_size, numevents)
                
                #uses uuid to generate a unique idintifier for each job
                unique_id = str(uuid.uuid4())  # Generate a unique identifier
                #creates a bath_info dictionary 
                batch_info = {
                    "job_id": unique_id,
                    "url": path,
                    "start_index": start_index,
                    "finish_index": finish_index,
                    "sample": path[1]
                }
                # print(f"Batch Info: {batch_info}")
                work_plan.append(batch_info)

            print(f"Under {path}\nthere were {numevents} events")
    return work_plan

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

#lumi = 0.5 # fb-1 # data_A only
#lumi = 1.9 # fb-1 # data_B only
#lumi = 2.9 # fb-1 # data_C only
#lumi = 4.7 # fb-1 # data_D only
lumi = 10 # fb-1 # data_A,data_B,data_C,data_D

fraction = 0.5 # reduce this is if you want the code to run quicker

#getting the crossectional weight 
def get_xsec_weight(sample):
    info = infofile.infos[sample] # open infofile
    xsec_weight = (lumi*1000*info["xsec"])/(info["sumw"]*info["red_eff"]) #*1000 to go from fb-1 to pb-1
    return xsec_weight # return cross-section weight

#an attempt at iterating over the file
def iteration_attempt(url_list, start, stop):
    #picks one of the urls
    url = url_list[5][0]
    sample = url_list[5][1]
    with uproot.open(url + ":mini") as tree:
        #numevents = tree.num_entries # number of events
        if 'data' not in sample: xsec_weight = get_xsec_weight(sample) # get cross-section weight
        counter = 0
        for data in tree.iterate(['lep_pt','lep_eta','lep_phi',
                                'lep_E','lep_charge','lep_type', 
                                # add more variables here if you make cuts on them 
                                'mcWeight','scaleFactor_PILEUP',
                                'scaleFactor_ELE','scaleFactor_MUON',
                                'scaleFactor_LepTRIGGER'], # variables to calculate Monte Carlo weight
                                library="ak", # choose output type as awkward array
                                entry_start=start,
                                entry_stop=stop): # process up to numevents*fraction
            counter = counter + 1
            print(data)
        print(f"{counter} items were generated")



url_list = url_builder()

print(f"There are {len(url_list)} urls in the url list")


# build_work_plan(url_list)

work_plan = build_work_plan(url_list)

print(f"printing work plan 0: \n{work_plan[0]}")
print(f"printing work plan 200: \n{work_plan[200]}")
