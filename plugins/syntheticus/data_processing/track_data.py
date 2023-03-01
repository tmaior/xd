# Author:      vlrmzz @ Syntheticus.ai
# Description: Use this script to track the current version of the dataset 
#              located at using dvc.
# ================================================================================

import os
import subprocess as sp
from datetime import datetime

import logging

logger = logging.getLogger(__name__)

def _initialize_dvc(input_dir):
    """initialize .dvc stored in 'proj_dir/airflow_data' with data in 'orig_data'"""
    os.chdir(input_dir)
    logger.info("initializing DVC repository in")

    os.chdir(input_dir)
    sp.Popen("git init", shell=True).wait()
    # With the git config lines the commit seems to not work
    sp.Popen('git config --global user.email "example@syntheticus.ai"', shell=True).wait()
    sp.Popen('git config --global user.name "syntheticus"', shell=True).wait()
    sp.Popen("dvc init", shell=True).wait()
    sp.Popen(f"dvc remote add -d dvc_remote {os.path.join(input_dir, 'dvc_remote')}", shell=True).wait()
    sp.Popen("git commit -m 'dvc setup'", shell=True).wait()  
    logger.info("DVC setup committed to Git")

def track_data(**context):
    """
    Track project folder and the folder inside (data_orig,data_synth,report,models)

    Args:
        conf json passed to the API requests
    """
    main_data_dir = "{}".format(context["dag_run"].conf["main_data_dir"])
    project_dir = main_data_dir + '/' +"{}".format(context["dag_run"].conf["project_dir"])
    dataset_dir = "{}".format(context["dag_run"].conf["dataset_dir"])
    airflow_data_folder = project_dir + "/airflow_data"  

    os.chdir(airflow_data_folder)
    # Check if DVC was already initialized
    if not os.path.exists(os.path.join(airflow_data_folder, ".dvc")):
        logger.info("DVC not yet initialized")
        _initialize_dvc(airflow_data_folder)
    
    if not os.popen("dvc status").read() == "Data and pipelines are up to date.\n":
        logger.info("Original data update detected")
        logger.info(os.popen("dvc status").read())
        
        # Track current version of original dataset
        current_time = datetime.now()
        timestamp = current_time.strftime("%Y/%m/%d-%H:%M:%S")
            # Intialize tracking of models, data_synth, report
        sp.Popen(f"dvc add {airflow_data_folder+'/data_orig'}", shell=True).wait()
        sp.Popen(f"git add {airflow_data_folder+'/data_orig'}.dvc", shell=True).wait()

        sp.Popen(f"dvc add {airflow_data_folder+'/models'}", shell=True).wait()
        sp.Popen(f"git add {airflow_data_folder+'/models'}.dvc", shell=True).wait()

        sp.Popen(f"dvc add {airflow_data_folder+'/data_synth'}", shell=True).wait()
        sp.Popen(f"git add {airflow_data_folder+'/data_synth'}.dvc", shell=True).wait()

        sp.Popen(f"dvc add {airflow_data_folder+'/report'}", shell=True).wait()
        sp.Popen(f"git add {airflow_data_folder+'/report'}.dvc", shell=True).wait()

        commit_msg = ' '.join([f"Adding original dataset version",dataset_dir,timestamp])
        sp.Popen(f"git commit -m '{commit_msg}'", shell=True).wait()
        logger.info(f"Committed new original dataset version {dataset_dir},{timestamp}")
        
        sp.Popen("dvc push", shell=True).wait()
        logger.info("Pushed data to remote")
    else:
        logger.info("Orignal data did not change. Nothing to track.")