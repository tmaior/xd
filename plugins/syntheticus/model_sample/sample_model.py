# Author:      Valerio Mazzone @ Syntheticus.ai
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
import logging
import sdv
from sdv.relational import HMA1
from sdv.tabular import GaussianCopula,CTGAN
import numpy as np
from datetime import datetime, date
import os
from mlflow.tracking.client import MlflowClient
import pandas as pd
import logging
import subprocess as sp

logger = logging.getLogger(__name__)

MAX_DATE = pd.Timestamp.max
MIN_DATE = pd.Timestamp.min

PYTHON_VERSION = "{major}.{minor}.{micro}".format(major=version_info.major,
                                                  minor=version_info.minor,
                                                  micro=version_info.micro)

# test ml-flow custom models
logger = logging.getLogger(__name__)

# Gaussian Copula wrapper
class synthGCopula(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        from sdv.tabular import GaussianCopula
        self.model = GaussianCopula.load(context.artifacts["gc_model"])

    def predict(self, context, model_input):
        return self.model.sample(num_rows = model_input)

# HMA1 wrapper
class synthHMA1(mlflow.pyfunc.PythonModel):

     def load_context(self, context):
         from sdv.relational import HMA1
         self.model = HMA1.load(context.artifacts["hma1_model"])

     def predict(self, context, model_input):
         return self.model.sample(num_rows = model_input)

def sample_model(**context):
    # to rewrite
    """
    Load the trained model from MLflow and sample synthetic data

    Args:
        from context:
        project_dir
        model_name
        model_experiment_name
        metadata_file
    """
    main_data_dir = "{}".format(context["dag_run"].conf["main_data_dir"])
    project_dir = main_data_dir + '/' +"{}".format(context["dag_run"].conf["project_dir"])
    airflow_data_folder = project_dir + "/airflow_data"

    dataset_type = "{}".format(context["dag_run"].conf["dataset_type"])
    synth_num_rows = "{}".format(context["dag_run"].conf["synth_num_rows"])

    task_instance = context.get('task_instance')
    
    if task_instance is None:
       ValueError(
           "task_instance is required, ensure you are calling this function from an airflow task and after a training run.")

    _, latest_model_uri, latest_table_names, latestNumOfRows = task_instance.xcom_pull(task_ids='model_training')

    logger.info(f"Loading trained model {latest_model_uri}")
    loaded_model = mlflow.pyfunc.load_model(latest_model_uri)
    # Load the model in `python_function` format
    
    logger.info(loaded_model)
    if synth_num_rows == 'Default':
        synth_data = loaded_model.predict(latestNumOfRows)
    else:
        synth_data = loaded_model.predict(int(synth_num_rows))

    logger.info(synth_data) # to be deprecated
    
    if dataset_type == 'singleTable':
        logger.info(dataset_type)
    
        file_path = airflow_data_folder+'/'+'data_synth/'+str(latest_table_names[0])+'.pkl'
        with open(file_path, 'wb') as file:
            # Save dataframe in pickle format
            pickle.dump(synth_data, file)
            logger.info(f"Successfully saved pickle {file_path}")
        
        file_path = airflow_data_folder+'/'+'data_synth/'+str(latest_table_names[0])+'.csv'
        synth_data.to_csv(file_path, index=False)

    elif dataset_type == 'multiTable':
        for item in latest_table_names:
            file_path = airflow_data_folder+'/'+'data_synth/'+str(item)+'.csv'
            synth_data[item].to_csv(file_path, index=False)
            logger.info(f"Successfully saved synth csv {file_path}")     
    else:
        logger.info('The dataset is not supported.')

    return
    
    