# Author:      vlrmzz @ Syntheticus.ai
# Description: Use this script to track the current version of the dataset 
#              located at using dvc.
# ================================================================================

import os
import subprocess as sp
from datetime import datetime

import logging

logger = logging.getLogger(__name__)

def track_report(**context):
    """
    Track project folder and the folder inside (data_orig,data_synth,report,models)

    Args:
        conf json passed to the API requests
    """
    main_data_dir = "{}".format(context["dag_run"].conf["main_data_dir"])
    project_dir = main_data_dir + '/' +"{}".format(context["dag_run"].conf["project_dir"])
    airflow_data_folder = project_dir + "/airflow_data"  
    dataset_dir = "{}".format(context["dag_run"].conf["dataset_dir"])


    os.chdir(airflow_data_folder)
    # Check if DVC was already initialized
    if not os.path.exists(os.path.join(airflow_data_folder, ".dvc")):
        logger.info("DVC not yet initialized")
    
    if not os.popen("dvc status").read() == "Data and pipelines are up to date.\n":
        logger.info("Report data update detected")
        logger.info(os.popen("dvc status").read())
        
        # Track current version of original dataset
        current_time = datetime.now()
        timestamp = current_time.strftime("%Y/%m/%d-%H:%M:%S")

        # track current version of synthetic dataset
        sp.Popen(f"dvc add {airflow_data_folder+'/report'}", shell=True).wait()
        sp.Popen(f"git add {airflow_data_folder+'/report'}.dvc", shell=True).wait()
        commit_msg = ' '.join(["Adding report data version",dataset_dir,timestamp])
        sp.Popen(f"git commit -m '{commit_msg}'", shell=True).wait()
        logger.info(f"Committed new report dataversion {timestamp}")
        
        sp.Popen("dvc push", shell=True).wait()
        logger.info("Pushed data to remote")
    else:
        logger.info("Report data did not change. Nothing to track.")