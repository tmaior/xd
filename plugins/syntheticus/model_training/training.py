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

import mlflow
import mlflow.pytorch
import mlflow.pyfunc
import time
import pandas as pd
import os
from pathlib import Path
import logging
import sdv
from sdv.tabular import GaussianCopula 
from sdv.relational import HMA1
from sdv import Metadata
from datetime import datetime, date

MAX_DATE = pd.Timestamp.max
MIN_DATE = pd.Timestamp.min

PYTHON_VERSION = "{major}.{minor}.{micro}".format(major=version_info.major,
                                                  minor=version_info.minor,
                                                  micro=version_info.micro)

logger = logging.getLogger(__name__)

# Create a Conda environment for the new MLflow Model that contains all necessary dependencies.
conda_env = {
    'channels': ['defaults'],
    'dependencies': [
      'python={}'.format(PYTHON_VERSION),
      'pip',
      {
        'pip': [
          'mlflow',
          'sdv=={}'.format(sdv.__version__),
          'cloudpickle=={}'.format(cloudpickle.__version__),
        ],
      },
    ],
    'name': 'sdv_env'
}

# Gaussian Copula wrapper
class synthGCopula(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        from sdv.tabular import GaussianCopula
        self.model = GaussianCopula.load(context.artifacts["gc_model"])

    def predict(self, context, model_input):
        return self.model.sample(model_input)

# HMA1 wrapper
class synthHMA1(mlflow.pyfunc.PythonModel):

     def load_context(self, context):
         from sdv.relational import HMA1
         self.model = HMA1.load(context.artifacts["hma1_model"])

     def predict(self, context, model_input):
         return self.model.sample(num_rows = model_input)

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

    os.chdir(dir)
    data_dir = 'data_orig'
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

def train(**context):
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

    dataset_type = "{}".format(context["dag_run"].conf["dataset_type"])
    synth_num_rows = "{}".format(context["dag_run"].conf["synth_num_rows"])

    start = time.time()
     
    tables, metadata_path = get_data(airflow_data_folder)

    tableNames = list(tables.keys())
    
    logger.info(tableNames)

    if dataset_type == 'singleTable':
        logger.info(dataset_type)
        if synth_num_rows == 'Default':
            numOfRows = tables[tableNames[0]].shape[0]
        else:
            numOfRows = int(synth_num_rows)
        with mlflow.start_run() as active_run:
            run_id = active_run.info.run_id
            # add the git commit hash as tag to the experiment run
            git_hash = os.popen("git rev-parse --verify HEAD").read()[:-2]
            mlflow.set_tag("git_hash", git_hash)
    
            # Run model Fit and Log model
            mlflow_pyfunc_model_path = "gc_model"
            #metadata = Metadata(project_dir+'/data_orig/'+ metadata_path)
            model = GaussianCopula()
            logger.info(tables[tableNames[0]])
            model.fit(tables[tableNames[0]])
            model_path = airflow_data_folder + '/models/model.pkl'
            model.save(model_path)

            artifacts = {
                "gc_model": model_path
            }
            
            mlflow.pyfunc.log_model(
                artifact_path=mlflow_pyfunc_model_path, 
                python_model=synthGCopula(), 
                artifacts=artifacts,
                conda_env=conda_env
                )
            model_uri = mlflow.get_artifact_uri(mlflow_pyfunc_model_path)

    elif dataset_type == 'multiTable':
        logger.info(dataset_type)
        if synth_num_rows == 'Default':
            numOfRows = tables[tableNames[0]].shape[0]
        else:
            numOfRows = int(synth_num_rows)
        with mlflow.start_run() as active_run:
            run_id = active_run.info.run_id
            # add the git commit hash as tag to the experiment run
            git_hash = os.popen("git rev-parse --verify HEAD").read()[:-2]
            mlflow.set_tag("git_hash", git_hash)
    
            # Run model Fit and Log model
            mlflow_pyfunc_model_path = "hma1_model"
            metadata = Metadata(airflow_data_folder + '/data_orig/' + metadata_path)
            model = HMA1(metadata)
            
            model.fit(tables)
            model_path = airflow_data_folder + '/models/model.pkl'
            model.save(model_path)

            artifacts = {
                 "hma1_model": model_path
            }

            mlflow.pyfunc.log_model(
                artifact_path=mlflow_pyfunc_model_path, 
                python_model=synthHMA1(), 
                artifacts=artifacts,
                conda_env=conda_env
                )

            model_uri = mlflow.get_artifact_uri(mlflow_pyfunc_model_path)
    else:        
        logger.info('The dataset is not supported.') 
        
    return run_id, model_uri, tableNames, numOfRows