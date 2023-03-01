# Author:      Valerio Mazzone @ syntehticus
# Description: This task takes a folder containing single or multiple  CSV files  
#              and a metadata json file moving them in the airflow_data folder 
#              inside the specified project.
# ================================================================================

import pandas as pd
import os
import shutil, os
from pathlib import Path
import logging

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
    
    Args:
       input_folder (str): specifying the folder which stores the data
    Returns:
       pd.DataFrame: aggregated data frame in a dictionary and list
       with the name of the matadata json file (can be an empty list) 
    """
    
    list_of_files = []

    logger.info(f"Reads csv files from nested folder {input_folder}")

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
    # Concatenate all files in a dictionary
    dfs = {}
    for item,file in zip(rel_paths,abs_paths):
        dfs[item[:-4]] = pd.read_csv(file, encoding = 'ISO-8859-1')
    logger.info(dfs.keys())
    return dfs,json_path
    
def ingest_data(**context):
    """
    - Read the orignal data and aggregate them in a dataframe
    - Create project folders
    - Save the file in the appropiate project_folders
    
    Args:
        conf json passed to the API requests
    Returns:
        None
    Results:
        Copy the content of the specified dataset folder in the airflow_data/data_orig folder
        that will be tracked in the track_data task
    """
    
    main_data_dir = "{}".format(context["dag_run"].conf["main_data_dir"])
    project_dir = main_data_dir + '/' +"{}".format(context["dag_run"].conf["project_dir"])
    dataset_dir = "{}".format(context["dag_run"].conf["dataset_dir"])
    input_folder = project_dir + "/uploaded_data/" + dataset_dir
    
    dfs, metadata_path = get_data(input_folder)

    # Check if the project airflow_data folder exists, if not it
    airflow_data_folder = project_dir + "/airflow_data"  
    if os.path.exists(airflow_data_folder):  
        logger.info(f"{str(airflow_data_folder)} exists")
    else:
        logger.info(f"I am creating {str(airflow_data_folder)} and the project sub-directory")
        os.mkdir(airflow_data_folder)
        os.mkdir(airflow_data_folder+'/data_orig')
        os.mkdir(airflow_data_folder+'/data_synth')
        os.mkdir(airflow_data_folder+'/report')
        os.mkdir(airflow_data_folder+'/models')

    # Save files in the project folder
    for item in dfs.keys():
        file_path = airflow_data_folder +'/data_orig/'+str(item)+'.csv'
        dfs[item].to_csv(file_path, index=False)
        logger.info(f"Successfully saved csv {file_path}")
    
    # Copy metadata file in the data orig folder
    if metadata_path:
        # TODO: Permission error
        shutil.copyfile(input_folder + '/' + metadata_path, airflow_data_folder+'/data_orig/demo_metadata.json')
        shutil.copyfile(input_folder + '/' + metadata_path, airflow_data_folder+'/data_synth/demo_metadata.json')

    else:
        logger.info('None matadata file has been provided.')