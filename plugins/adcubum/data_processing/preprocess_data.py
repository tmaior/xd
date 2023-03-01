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
        
def get_data(input_folder):
    """
    Get the aggregated dataframe, consisting of all data inside 'input_folder'
    between 'from_date' and 'to_date'
    
    Args:
       input_folder (str): specifying the folder which stores the data
       from_date (Optional[str]): earliest start date to filter data
       to_date (Optional[str]): last date to filter data
    Returns:
       pd.DataFrame: aggregated data frame
    """
    
    list_of_files = []

    logger.info(f"reads csv files from nested folder {input_folder}")

    # for root, dirs, files in os.walk("../data/data_orig/", topdown=False):
    #     for name in files:
    #         print(os.path.join(root, name))
    #     for name in dirs:
    #         print(os.path.join(root, name))

    for (dirpath, dirnames, filenames) in os.walk(input_folder):
        list_of_files += [os.path.join(dirpath, file) for file in filenames]

    logger.info(f"loaded {len(list_of_files)} files")

    # Make relative paths from absolute file paths
    rel_paths = [os.path.relpath(item, input_folder) for item in list_of_files]
    logger.info(rel_paths)
    # Filter out csv files
    json_path = list(filter(filter_is_json, rel_paths))[0]
    # Filter out non-csv files
    rel_paths = list(filter(filter_is_csv, rel_paths))
    logger.info(rel_paths)
    # Get absolute paths
    abs_paths = [os.path.join(input_folder, item) for item in rel_paths]
    # Concatenate all files
    dfs = {}
    for item,file in zip(rel_paths,abs_paths):
        dfs[item[:-4]] = pd.read_csv(file, encoding = 'ISO-8859-1')
    logger.info(dfs.keys())
    return dfs,json_path
    
    
def preprocess_data(**context):
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

    # Load Data to be preprocessed
    dataToBePrep = project_dir + '/airflow_data/data_orig'
    dfs, metadata_path = get_data(dataToBePrep)

    MAX_DATE = pd.Timestamp.max
    MIN_DATE = pd.Timestamp.min
    
    # Preprocess data
    for item in dfs.keys():
        logger.info(item)
        dfs[item] = dfs[item].sort_values('CREATED').drop_duplicates(subset=['BOID'])

        dfs[item] = dfs[item].dropna(axis = 'columns', how = 'all')
        # if "BOID" in dfs[item]:
        #     dfs[item]["BOID"] = dfs[item]["BOID"].astype('str')
        if "ITSTARIFTYP" in dfs[item]:
            dfs[item]["ITSTARIFTYP"] = dfs[item]["ITSTARIFTYP"].astype('str')

        # if "CREATED" in dfs[item]:
        #     dfs[item]['CREATED'] = pd.to_datetime(dfs[item]['CREATED'],errors='coerce')
        # if "REPLACED" in dfs[item]:
        #     dfs[item]['REPLACED'] = pd.to_datetime(dfs[item]['REPLACED'],errors='coerce').fillna(MAX_DATE)
        # if "LASTUPDATE" in dfs[item]:
        #     dfs[item]['LASTUPDATE'] = pd.to_datetime(dfs[item]['LASTUPDATE'],errors='coerce')
        # if "GUELTAB" in dfs[item]:
        #     dfs[item]['GUELTAB'] = pd.to_datetime(dfs[item]['GUELTAB'],errors='coerce').fillna(MIN_DATE)
        # if "GUELTBIS" in dfs[item]:
        #     dfs[item]['GUELTBIS'] = pd.to_datetime(dfs[item]['GUELTBIS'],errors='coerce').fillna(MAX_DATE)

        # if "STATEFROM" in dfs[item]:
        #     dfs[item]['STATEFROM'] = pd.to_datetime(dfs[item]['STATEFROM'],errors='coerce').fillna(MIN_DATE)
        #     dfs[item]['STATEFROM'] = dfs[item]['STATEFROM'].replace([datetime(1900,1,1,00,00,00)],pd.NaT)
        # if "STATEUPTO" in dfs[item]: 
        #     dfs[item]['STATEUPTO'] = pd.to_datetime(dfs[item]['STATEUPTO'],errors='coerce').fillna(MAX_DATE)
        # if "STATEBEGIN" in dfs[item]:
        #     dfs[item]['STATEBEGIN'] = pd.to_datetime(dfs[item]['STATEBEGIN'],errors='coerce').fillna(MIN_DATE)
        # if "STATEEND" in dfs[item]:
        #     dfs[item]['STATEEND'] = pd.to_datetime(dfs[item]['STATEEND'],errors='coerce').fillna(MAX_DATE)


        dfs[item]["BOID"] = dfs[item]["BOID"].astype('str')

        #dfs[item]["ITSTARIFTYP"] = dfs[item]["ITSTARIFTYP"].astype('str')

        dfs[item]['CREATED'] = pd.to_datetime(dfs[item]['CREATED'],errors='coerce')

        dfs[item]['REPLACED'] = pd.to_datetime(dfs[item]['REPLACED'],errors='coerce').fillna(MAX_DATE)

        dfs[item]['LASTUPDATE'] = pd.to_datetime(dfs[item]['LASTUPDATE'],errors='coerce')

        dfs[item]['GUELTAB'] = pd.to_datetime(dfs[item]['GUELTAB'],errors='coerce').fillna(MIN_DATE)

        dfs[item]['GUELTBIS'] = pd.to_datetime(dfs[item]['GUELTBIS'],errors='coerce').fillna(MAX_DATE)


        dfs[item]['STATEFROM'] = pd.to_datetime(dfs[item]['STATEFROM'],errors='coerce').fillna(MIN_DATE)
        dfs[item]['STATEFROM'] = dfs[item]['STATEFROM'].replace([datetime(1900,1,1,00,00,00)],pd.NaT)

        dfs[item]['STATEUPTO'] = pd.to_datetime(dfs[item]['STATEUPTO'],errors='coerce').fillna(MAX_DATE)

        dfs[item]['STATEBEGIN'] = pd.to_datetime(dfs[item]['STATEBEGIN'],errors='coerce').fillna(MIN_DATE)

        dfs[item]['STATEEND'] = pd.to_datetime(dfs[item]['STATEEND'],errors='coerce').fillna(MAX_DATE)

        dfs[item] = dfs[item].sort_values(by=['BOID', 'CREATED'], ascending=[True, True])

        # remove duplicates based on column 'A'
        dfs[item] = dfs[item].drop_duplicates(subset='BOID')



    # Save files in the project folder
    for item in dfs.keys():
        file_path = project_dir+'/airflow_data/data_orig/'+str(item)+'.pkl'
        logger.info(dfs[item].info())
        # Open a file and use dump()
        with open(file_path, 'wb') as file:
        # Save dataframe in pickle format
            pickle.dump(dfs[item], file)
            logger.info(dfs[item].info())
            file_path_rm = project_dir+'/airflow_data/data_orig/'+str(item)+'.csv'
            os.remove(file_path_rm)
            logger.info(f"Successfully saved pickle {file_path}")