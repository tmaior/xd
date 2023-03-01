# Author:      Valerio Mazzone @ syntehticus
# Description: This script takes a folder structure containing multiple CSV files  
#              and aggregates it into a single file.
# ================================================================================

import argparse
import pandas as pd
import os
import shutil, os
from pathlib import Path
import logging
from datetime import datetime, date
import pickle
import pandas as pd
import numpy as np
import random
from datetime import timedelta 
from matplotlib import pyplot as plt 
from faker import Faker
fake = Faker()

MAX_DATE = pd.Timestamp.max
MIN_DATE = pd.Timestamp.min

logger = logging.getLogger(__name__)

def filter_is_csv(item):
    """
    Returns true if the item is a csv file
    """
    return item.endswith(".csv")

def filter_is_json(item):
    """
    Returns true if the item is a csv file
    """
    return item.endswith(".json")

def filter_is_pkl(item):
    """
    Returns true if the item is a csv file
    """
    return item.endswith(".pkl")

def get_data(dir):
    """
    Get the aggregated dataframe, consisting of all data inside the folder with original data
    
    Args:
       input_folder (str): specifying the folder which stores the data
       from_date (Optional[str]): earliest start date to filter data
       to_date (Optional[str]): last date to filter data
    Returns:
       pd.DataFrame: aggregated data frame
    """

    #os.chdir(dir)
    #data_dir = 'data_orig'
    logger.info(dir)
    list_of_files = []
    
    for (dirpath, dirnames, filenames) in os.walk(dir):
        list_of_files += [os.path.join(dirpath, file) for file in filenames]
    logger.info(list_of_files)
    # Make relative paths from absolute file paths
    rel_paths = [os.path.relpath(item, dir) for item in list_of_files]
    logger.info(rel_paths)
    # Filter out csv files
    json_path = list(filter(filter_is_json, rel_paths))[0]
    # Filter csv files
    rel_paths_csv = list(filter(filter_is_csv, rel_paths))
    logger.info(rel_paths_csv)
    # Filter pkl files
    rel_paths_pkl = list(filter(filter_is_pkl, rel_paths))
    logger.info(rel_paths_pkl)

    # Concatenate all files
    dfs = {}
    for item in rel_paths_csv:
        dfs[item[:-4]] = pd.read_csv(dir + '/' + item)
        logger.info(item[:-4])
    logger.info(f"loaded CSV files")

    for item in rel_paths_pkl:
        logger.info(item)
        logger.info(dir)
        with open(dir + '/' + item, 'rb') as file:
            # Call load method to deserialze
            dfs[item[:-4]] = pickle.load(file)

    logger.info(f"loaded PKL files")
    
    return dfs, json_path

# Define exclusion list
technical_attributes = [
                'PKEY','METABO','PROCESSID','ROWCOMMENT',
                'BOID','BIZCREATED','CREATED','LASTUPDATE',
                'REPLACED','STATEFROM','STATEUPTO',
                'STATEBEGIN','STATEEND','GUELTAB','GUELTBIS',
                'TIMESEG','ARCHIVETAG','MDBID']
				
				
mlbo_attributes = [
                'INTERNALNAME',
                'BEZEICHNUNGDT','NL_BEZEICHNUNGDT',
                'BEZEICHNUNGEN','NL_BEZEICHNUNGEN',
                'BEZEICHNUNGFR','NL_BEZEICHNUNGFR',
                'BEZEICHNUNGIT','NL_BEZEICHNUNGIT',
                'BEZEICHNUNG05','NL_BEZEICHNUNG05',
                'BEZEICHNUNG06','NL_BEZEICHNUNG06',
                'BEZEICHNUNG07','NL_BEZEICHNUNG07',
                'BEZEICHNUNG08','NL_BEZEICHNUNG08',
                'BEZEICHNUNG09','NL_BEZEICHNUNG09',
                'BEZEICHNUNG10','NL_BEZEICHNUNG10',
				'KURZBEZDT',
				'KURZBEZEN',
				'KURZBEZFR',
				'KURZBEZIT'
				'KURZBEZ05',
				'KURZBEZ06',
				'KURZBEZ07',
				'KURZBEZ08',
				'KURZBEZ09',
				'KURZBEZ10'
				]

foreign_keys = [
'ITSTARIFTYP',
'ITSWAEHRUNG',
'ITSAUSZETARGRP',
'ITSANALOGIETARIF',
'ITSKOGUZEITRMENHT'
]

business_list = ['GTIN',
                'TARIFZIFFER',
                'TARIF']	

exception_list = technical_attributes + mlbo_attributes
exception_list = exception_list + foreign_keys
exception_list = exception_list + business_list

def state_begin_end_correction(tmp,idMaxRow):

    # change only for the active

    #now active is if pd.isnull(tmp.REPLACED) is True to be checked
    min_statefrom = pd.NaT if tmp[pd.isnull(tmp.REPLACED)]['STATEFROM'].isna().any() else min(tmp[pd.isnull(tmp.REPLACED)]['STATEFROM'])
    max_stateupto = pd.NaT if tmp[pd.isnull(tmp.REPLACED)]['STATEUPTO'].isna().any() else max(tmp[pd.isnull(tmp.REPLACED)]['STATEUPTO'])
            
    tmp['GUELTBIS'] = max_stateupto
    tmp['GUELTAB'] = min_statefrom

    for i in tmp.index:
        if pd.isnull(tmp.REPLACED)[i] and i <= idMaxRow: 
            
            #print('statebegin',tmp.at[i,'STATEFROM'] == min_statefrom,tmp.at[i,'STATEFROM'],min_statefrom)
            if tmp.at[i,'STATEFROM'] == min_statefrom:
                tmp.at[i,'STATEBEGIN'] = pd.NaT
            else:   
                tmp.at[i,'STATEBEGIN'] = tmp.at[i,'STATEFROM']
                
            #print('stateend', tmp.at[i,'STATEUPTO'] == max_stateupto,tmp.at[i,'STATEUPTO'],max_stateupto)
            if tmp.at[i,'STATEUPTO'] == max_stateupto:
                tmp.at[i,'STATEEND'] = pd.NaT
            else:
                tmp.at[i,'STATEEND'] = tmp.at[i,'STATEUPTO'] 
        else:
            continue
    return tmp

def correcting_dates(tmpToSave):
    '''
    This function correct the dates in the right format.
    The logic needs to be changed because to save step by step
    the way dataframes are referenciated is strange.
    '''
    #Correct date for computational porpuse 
    tmpToSave['REPLACED'] = tmpToSave['REPLACED'].replace([pd.NaT],MAX_DATE)
    tmpToSave['GUELTAB'] = tmpToSave['GUELTAB'].replace([datetime(1900,1,1,0,0,0)],MIN_DATE)
    tmpToSave['GUELTBIS'] = tmpToSave['GUELTBIS'].replace([pd.NaT],MAX_DATE)
    tmpToSave['STATEFROM'] = tmpToSave['STATEFROM'].replace([datetime(1900,1,1,0,0,0)],MIN_DATE)
    tmpToSave['STATEUPTO'] = tmpToSave['STATEUPTO'].replace([pd.NaT],MAX_DATE)
    tmpToSave['STATEBEGIN'] = tmpToSave['STATEBEGIN'].replace([pd.NaT],MIN_DATE)
    tmpToSave['STATEEND'] = tmpToSave['STATEEND'].replace([pd.NaT],MAX_DATE)

    #Trasform datetime to date

    tmpToSave['GUELTAB'] = pd.to_datetime(tmpToSave['GUELTAB']).dt.date
    tmpToSave['GUELTBIS'] = pd.to_datetime(tmpToSave['GUELTBIS']).dt.date
    tmpToSave['STATEFROM'] = pd.to_datetime(tmpToSave['STATEFROM']).dt.date
    tmpToSave['STATEUPTO'] = pd.to_datetime(tmpToSave['STATEUPTO']).dt.date
    tmpToSave['STATEBEGIN'] = pd.to_datetime(tmpToSave['STATEBEGIN']).dt.date
    tmpToSave['STATEEND'] = pd.to_datetime(tmpToSave['STATEEND']).dt.date


    tmpToSave['REPLACED'] = tmpToSave['REPLACED'].astype(str).replace(str(MAX_DATE),'3000-01-01 00:00:00')


    tmpToSave['GUELTAB'] = tmpToSave['GUELTAB'].astype(str).replace(str(pd.to_datetime(MIN_DATE).date()),'1900-01-01')
    tmpToSave['GUELTBIS'] = tmpToSave['GUELTBIS'].astype(str).replace(str(pd.to_datetime(MAX_DATE).date()),'3000-01-01')
    tmpToSave['STATEBEGIN'] = tmpToSave['STATEBEGIN'].astype(str).replace(str(pd.to_datetime(MIN_DATE).date()),'1900-01-01')
    tmpToSave['STATEEND'] = tmpToSave['STATEEND'].astype(str).replace(str(pd.to_datetime(MAX_DATE).date()),'3000-01-01')
    tmpToSave['STATEFROM'] = tmpToSave['STATEFROM'].astype(str).replace(str(pd.to_datetime(MIN_DATE).date()),'1900-01-01')
    tmpToSave['STATEUPTO'] = tmpToSave['STATEUPTO'].astype(str).replace(str(pd.to_datetime(MAX_DATE).date()),'3000-01-01')

    return tmpToSave

def findDistributions(column):
    values = column.value_counts().index#.reset_index(drop = True)
    counts = column.value_counts().values
    freq = pd.DataFrame()
    freq['values'] = values
    freq['counts'] = counts
    freq['prob'] = counts/len(column)
    c  = 1/freq['prob'].sum()
    freq['norm_prob'] = freq['prob']*c
    return freq

def substitute(tmp,n,changeColumn,valueToInsert):
    tmp = pd.concat([tmp,tmp.iloc[n].to_frame().T],ignore_index=True)
    lastIndex = tmp.last_valid_index()
    
    logger.info(tmp.at[n, "CREATED"],type(tmp.at[n, "CREATED"]))
    createdNp1 = fake.date_time_between(
        start_date= tmp.at[n, "CREATED"]+timedelta(days=1), 
        end_date=date.today()-timedelta(days=45)
        )

    tmp.at[lastIndex, "CREATED"] = createdNp1
    tmp.at[n, "REPLACED"] = tmp.at[lastIndex, "CREATED"]
    tmp.at[n, "LASTUPDATE"] = tmp.at[lastIndex, "CREATED"]
    # check if the below 2 rows can be deleted becasue i already copied the row n in to lastIndex
    ##tmp.at[lastIndex, "STATEFROM"] = tmp.at[n, "STATEFROM"]
    ##tmp.at[lastIndex, "STATEUPTO"] = tmp.at[n, "STATEUPTO"]
    tmp.at[lastIndex, "REPLACED"] = pd.NaT
    tmp.at[lastIndex, "LASTUPDATE"] = tmp.at[lastIndex, "CREATED"]
    logger.info(valueToInsert)
    tmp.at[lastIndex, changeColumn] = valueToInsert

    return tmp

def split(tmp,n,changeColumn,valueToInsert):
    # assert that tmp.at[n, "CREATED"] (got from alex) is at least 30 days from now
    tmp = pd.concat([tmp,tmp.iloc[n].to_frame().T],ignore_index=True)
    tmp = pd.concat([tmp,tmp.iloc[n].to_frame().T],ignore_index=True)
    lastIndex=tmp.last_valid_index()
    
    # if it s human action any date but office hours
    # for batch even
    # [CREATED,n] +1 TO now - 30 days but assert is higher then [Created,n].
    # have a tleast 1 day between them
    logger.info(tmp.at[n, "CREATED"],type(tmp.at[n, "CREATED"]))  
    datetime.strptime
    # Split creation timestamp
    splitCreationTS = fake.date_time_between(
        start_date= tmp.at[n, "CREATED"]+timedelta(days=1), 
        end_date=date.today()-timedelta(days=45)
        )

    # end_date now-30days 
    # assert same things as before.
    splitDateTS = fake.date_time_between(
        start_date= tmp.at[n, "STATEFROM"],
        end_date=date.today()-timedelta(days=30))
    
    # Adapting the first state of the split
    tmp.at[lastIndex-1, "CREATED"] = splitCreationTS
    tmp.at[lastIndex-1, "LASTUPDATE"] = splitCreationTS
    tmp.at[lastIndex-1, "STATEUPTO"] = splitDateTS
    tmp.at[lastIndex-1, "REPLACED"] = pd.NaT

    # Adapting the second state of the split
    tmp.at[lastIndex, "CREATED"] = splitCreationTS
    tmp.at[lastIndex, "STATEFROM"] = splitDateTS + timedelta(days=1)
    # if I am doing a copy the line below is not required
    tmp.at[lastIndex, "STATEUPTO"] = tmp.at[n, "STATEUPTO"]
    tmp.at[lastIndex, "REPLACED"] = pd.NaT    
    tmp.at[lastIndex, "LASTUPDATE"] = splitCreationTS

    # Replacing the original row
    tmp.at[n, "LASTUPDATE"] = splitCreationTS
    tmp.at[n, "REPLACED"] = splitCreationTS

    logger.info(valueToInsert)
    tmp.at[lastIndex, changeColumn] = valueToInsert

    # Check if the logic of the dates is correct
    assert tmp.at[n, "CREATED"] < date.today()-timedelta(days=30)
    assert tmp.at[lastIndex-1, "STATEFROM"] < tmp.at[lastIndex-1, "STATEUPTO"]
    assert tmp.at[lastIndex-1, "STATEUPTO"] < tmp.at[lastIndex, "STATEFROM"]
    #assert tmp.at[n+2, "STATEFROM"] < tmp.at[n+2, "STATEUPTO"] 
    # is it possible that this will always throw an error because tmp.at[n+2, "STATEUPTO"] is inifinty (or pd.NaT) 
    # add or that check if it s pd.NaT

    return tmp

def postprocessTechnical(table, orig_table):
    # Add definition of the function
    # u can optimize it keeping juist the boid column and statefrom
    originalList = table.columns.values.tolist()
    changingList = list(set(originalList).symmetric_difference(set(exception_list)))
    logger.info(changingList)
    
    orig_table.sort_values('BOID',inplace=True)
    table.sort_values('BOID',inplace=True)

    if 'STATEFROM' in originalList:
      historyType = 'histBO'
    elif 'GUELTAB' in originalList:
      historyType = 'lifeBO'
    elif 'REPLACED' in originalList:
      historyType = 'BO'
    else:
      logger.info('Cannot identify BO')

    if historyType == 'histBO':
        freqSTATEFROM = findDistributions(orig_table['STATEFROM'])

    freqBOID = findDistributions(orig_table['BOID'])
    freqBOID = findDistributions(freqBOID['counts'])
    
    # change to targetLenght
    tmpLenght = random.choices(freqBOID['values'], weights = freqBOID['norm_prob'], k = 10800)# check this because is hasrdcorded

    synthTable = pd.DataFrame()
    for i in range(0,table.shape[0]):
        row = table.iloc[i].to_frame().T
        tmp = pd.DataFrame()
        tmp = pd.concat([tmp,row],ignore_index=True)
        tmp['REPLACED'] = pd.NaT
        
        #final lenght of life cycle for the single BOID
        lenght = tmpLenght[i]
        # Calculate parameters foir single BOID
        
        if lenght == 1:
            #assert statefrom = guiltab, stateupto = guiltbis, statebegin = 1900-01-01 stateend = 3000-01-01
            synthTable = pd.concat([synthTable,tmp],ignore_index=True)
            activeRows = 1 
            pass # this seems nencessary if the final lenght is 1 
                # does not make sense to do the BOID life cycle
        elif lenght <= 3: # is it possible to have 1 ianctive and 2 active example: i have the first row and i do a split
            activeRows = 1
        elif lenght > 3:
            activeRows = random.randint(2,lenght-1)

        # Override for lifeBO and BO
        if historyType in ['BO','lifeBO']:
            activeRows = 1
        
        maxNoOfSplits = (lenght - 1) / 2
        idealNoOfSplits = activeRows - 1
        idealNoOfSplits = min(idealNoOfSplits,maxNoOfSplits)
        idealNoOfSplits = np.floor(idealNoOfSplits)
        substitutesNo = 2 * (maxNoOfSplits - idealNoOfSplits)
        totalActions = idealNoOfSplits + substitutesNo
        # Calculate probabilities of a split or substitution
        probSplit =  idealNoOfSplits / totalActions
        probSub = 1 - probSplit
        
        # set the initial values of remaining actions
        remainingSplits = idealNoOfSplits 
        remaningSubstitutions = substitutesNo
        rowNumber = 0

        if historyType in ['BO','lifeBO']:
            logger.info(f'For {historyType} the calculated probability of substitution is: {probSub}, and it should be 1 for BO and lifeBO.')
        
        if historyType in ['histBO']:
            '''
            Line below: this is an assumption that every BOID always start with 
            stateupto = 3000-01-01
            '''
            tmp.at[0,'STATEUPTO'] = pd.NaT
            tmp.at[0,'STATEFROM'] = random.choices(freqSTATEFROM['values'], weights = freqSTATEFROM['norm_prob'], k = 1)[0]
            tmp = state_begin_end_correction(tmp,0)

        print('Line and BOID:', tmp['BOID'])  
        print('Actual row Nuber and Final lenght:',rowNumber,lenght-1)
        while rowNumber < lenght-1:
            
            if random.random() < probSub: 
                action='substitute'
            else: 
                action='split'
            
            tmpActiveRows = tmp.index[pd.isnull(tmp.REPLACED)]
            print('tmpActiveRows',tmpActiveRows)
            rowWithAction = random.choice(tmpActiveRows[:])
            print('RowWithAction',rowWithAction)
            if historyType in ['BO','lifeBO']:
                logger.info(f'For BO and lifeBO {tmpActiveRows} should be only 1 value')
            
            # This is to avaoid to take into account the fact that we are not working with the full
            # dataset but we are dropping nans....
            columnToChange = ''
            while columnToChange not in originalList:
                columnToChange = random.choice(changingList) 
            else:
                value = orig_table[columnToChange].sample().reset_index(drop = True)[0]
            
            if action == 'substitute' and remaningSubstitutions > 0:
                print('Action being applied',action)

                logger.info(columnToChange)
                logger.info(value)
                print('Column to be changed',columnToChange)
                tmp = substitute(tmp,rowWithAction,columnToChange,value)
                remaningSubstitutions = remaningSubstitutions - 1
                rowNumber = rowNumber + 1
                #change businees data with the function (last row, (list of fields i can change))
                # sample from the chosen column of the orignal table
                
            elif remainingSplits > 0:
                #### IMPORTANT override the action if it falls in this case. otherwise it will print the wrong action
                # chose randomly the last one or the penultimate to apply the businees change
                # call the function here for the change
                # the solution to override does not seems very elegant because it overrides even when the action is the correct ones
                assert historyType in ['histBO']
                action = "split" 
                print('Action being applied',action)
                logger.info(columnToChange)
                logger.info(value)
                print('Column to be changed',columnToChange)
                tmp = split(tmp,rowWithAction,columnToChange,value)
                remainingSplits = remainingSplits - 1
                rowNumber = rowNumber + 2
                
            # else check if the logic falls in or not
            else:
                print('FUCK') #should I just pass here?
                pass
            #display('tmp after each action')
            #display(tmp[['BOID','CREATED','LASTUPDATE','REPLACED','STATEFROM','STATEUPTO','STATEBEGIN','GUELTAB','GUELTBIS']])
            
        tmp = state_begin_end_correction(tmp,rowNumber)
        tmpToSave = tmp.copy()
        tmpToSave = correcting_dates(tmpToSave)
        logger.info(tmpToSave)

        synthTable = pd.concat([synthTable,tmpToSave],ignore_index=True)

    return synthTable

def postprocess_data(**context):
    """
    - Read the orignal data and aggregate them in a dataframe
    - Preprocess the data
    - Save the preprocessed version of the data
    
    Args:
        conf json passed to the API requests
    
    Returns:
        pd.DataFrame: aggregated data frame
    """

    main_data_dir = "{}".format(context["dag_run"].conf["main_data_dir"])
    project_dir = main_data_dir + '/' "{}".format(context["dag_run"].conf["project_dir"])
    
    # Load the original data
    dataOrig = project_dir + '/airflow_data/data_synth'
    dfsOrig, metadata_path_orig = get_data(dataOrig)

    # Load Data to be preprocessed
    dataToBePost = project_dir + '/airflow_data/data_synth'
    logger.info(dataToBePost)
    dfs, metadata_path = get_data(dataToBePost)

    MAX_DATE = pd.Timestamp.max
    MIN_DATE = pd.Timestamp.min
    
    for item in dfs.keys():
        logger.info(f"Postprocessing table:{item}")
        logger.info(dfs[item].info())
        synth_data = postprocessTechnical(dfs[item],dfsOrig[item])
        logger.info(synth_data)

        file_path = dataToBePost + '/' +str(item)+'.pkl'
        synth_data.to_csv(dataToBePost + '/' +str(item)+'.csv')
        with open(file_path, 'wb') as file:
        # Save dataframe in pickle format
            pickle.dump(synth_data, file)
            logger.info(f"Successfully saved pickle {file_path}")
