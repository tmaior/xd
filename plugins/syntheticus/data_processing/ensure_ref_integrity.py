# Author:      CD4ML Working Group @ D ONE
# Description: Use this script to train a new ML model from scratch. The algorithm
#              is defined in 'get_model'. The trained model will be tracked in
#              MLflow and is available for further steps in the pipeline via model 
#              uri
# ================================================================================

import os
from sys import version_info
import cloudpickle
import pickle
import time
from pathlib import Path
import logging
import numpy as np
import pandas as pd
import os
from sdv import Metadata
import json
from itertools import groupby
from sklearn import preprocessing
from datetime import datetime, date

MAX_DATE = pd.Timestamp.max
MIN_DATE = pd.Timestamp.min

PYTHON_VERSION = "{major}.{minor}.{micro}".format(major=version_info.major,
                                                  minor=version_info.minor,
                                                  micro=version_info.micro)

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

def get_data(dir,data_dir):
    """
    Get the aggregated dataframe, consisting of all data inside the folder with original data
    
    Args:
       input_folder (str): specifying the folder which stores the data
       from_date (Optional[str]): earliest start date to filter data
       to_date (Optional[str]): last date to filter data
    Returns:
       pd.DataFrame: aggregated data frame
    """

    os.chdir(dir)
    logger.info(data_dir)
    list_of_files = []
    
    for (dirpath, dirnames, filenames) in os.walk(data_dir):
        list_of_files += [os.path.join(dirpath, file) for file in filenames]
    logger.info(list_of_files)
    # Make relative paths from absolute file paths
    rel_paths = [os.path.relpath(item, data_dir) for item in list_of_files]
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
        dfs[item[:-4]] = pd.read_csv(data_dir + '/' + item)
        logger.info(item[:-4])
    logger.info(f"loaded CSV files")

    for item in rel_paths_pkl:
        with open(data_dir + '/' + item, 'rb') as file:
            # Call load method to deserialze
            dfs[item[:-4]] = pickle.load(file)

    logger.info(f"loaded PKL files")
    
    return dfs, json_path

def ensure_refint(**context):
    """
    Loads single or multitable data set from project_name/orig_data, trains a model and tracks
    it with MLflow

    Args:
        main_data_dir
        project_name
        type of model
    """
    # Create setup variable
    
    main_data_dir = "{}".format(context["dag_run"].conf["main_data_dir"])
    project_dir = main_data_dir + '/' +"{}".format(context["dag_run"].conf["project_dir"])
    airflow_data_folder = project_dir + "/airflow_data"

    start = time.time()
     
    orig_tables, metadata_file = get_data(airflow_data_folder,'data_orig')
    synth_tables, metadata_file_synth = get_data(airflow_data_folder,'data_synth')

    with open(airflow_data_folder + '/data_orig/' + metadata_file) as meta_file:
        metadata = json.load(meta_file)
    logger.info(metadata)
    
    # build the relationald atabase configuration 
    pks = {}
    rel_list = []
    rel_grouped = []
    for item in metadata['tables']:
        pks[item] = metadata['tables'][item]['primary_key']
        for field in metadata['tables'][item]['fields']:
            if 'ref' in metadata['tables'][item]['fields'][field]:
                refTable = metadata['tables'][item]['fields'][field]['ref']['table']
                refField = metadata['tables'][item]['fields'][field]['ref']['field']
                childTable = item
                childField = field
                if [refTable,refField] not in rel_list:
                    rel_list.append([refTable,refField])
                rel_list.append([childTable,childField])

    # Key function
    key_func = lambda x: x[1]
    
    for key, group in groupby(rel_list, key_func):
        l = list(group)
        rel_tmp = []
        for i in range(len(l)):
            rel_tmp.append((l[i][0],l[i][1]))
        rel_grouped.append(rel_tmp)

    rdb_config = {}
    rdb_config['primary_keys'] = pks
    rdb_config['relationships'] = rel_grouped
    rdb_config['table_data'] = orig_tables
    logger.info(rdb_config)

    primary_keys_processed = [] 

    def transform_keys(key_set):

        # Get array of unique values from each table, can use dfs in transformed_tables   
        first = True
        field_values = set()
        for table_field_pair in key_set:
            table, field = table_field_pair
            # The first pair is a primary key
            if first:
                primary_keys_processed.append(table)
                first = False
            field_values = field_values.union(set(rdb_config["table_data"][table][field]))
            
        # Train a label encoder
        field_values_list = list(set(field_values))
        le = preprocessing.LabelEncoder()
        le.fit(field_values_list)
        # Run the label encoder on dfs in transformed_tables
        for table_field_pair in key_set:
            table, field = table_field_pair
            synth_tables[table][field] = le.transform(rdb_config["table_data"][table][field])

        # Run our transform_keys function on each key set
    for key_set in rdb_config["relationships"]:
        transform_keys(key_set)

    for item in list(synth_tables.keys()):
        file_path = airflow_data_folder+'/'+'data_synth/'+str(item)+'.pkl'
        with open(file_path, 'wb') as file:
        # Save dataframe in pickle format
            pickle.dump(synth_tables[item], file)
            logger.info(f"Successfully saved pickle {file_path}") 
            #file_path_rm = project_dir+'/airflow_data/data_synth/'+str(item)+'.csv'
            #os.remove(file_path_rm)
    return 