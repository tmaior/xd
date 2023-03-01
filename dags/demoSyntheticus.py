# Author:      Valerio Mazzone and Alex Manai @ adcubumeticus.ai
# Description: Script that defines and creates the CI Airflow DAG (the MLOps 
#              training pipeline). If the Airflow scheduler and webserver are
#              running, you can open the notebooks folder and run testAPI.ipynb
# ================================================================================

import os
from datetime import timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
# Tasks defined for Syntheticus pipeline
from syntheticus.data_processing import ingest_data
from syntheticus.data_processing import track_data
from syntheticus.model_training import train
from syntheticus.model_sample import sample_model
from syntheticus.data_processing import track_synth_data
from syntheticus.data_processing import track_model
from syntheticus.model_metrics import get_metrics
from syntheticus.data_processing import track_report

logger = logging.getLogger(__name__)

# directories
_root_dir = "/"
if not _root_dir:
    raise ValueError('PROJECT_PATH environment variable not set')


default_args = {
    'owner': 'syntheticus',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'demoSyntheticus',
    default_args=default_args,
    description='Synthetcus Hub Pipeline 4 demo',
    schedule_interval=None
)

with dag:
    pass

    data_ingestion = PythonOperator( 
        provide_context=True, 
        task_id='data_ingestion',
        python_callable=ingest_data,
        dag=dag
    )

    data_track = PythonOperator(
        provide_context=True,
        task_id='track_data',
        python_callable=track_data,
        dag=dag      
      )
    
    model_training = PythonOperator(
        provide_context=True,
        task_id='model_training',
        python_callable=train,
        dag=dag
     )

    model_track = PythonOperator(
        provide_context=True,
        task_id='track_model',
        python_callable=track_model,
        dag=dag      
      ) 

    model_sampling = PythonOperator(
        provide_context=True,
        task_id='model_sampling',
        python_callable=sample_model,
        dag=dag
      )

    data_synth_track = PythonOperator(
        provide_context=True,
        task_id='track_synth_data',
        python_callable=track_synth_data,
        dag=dag      
      )  

    model_metrics = PythonOperator(
        provide_context=True,
        task_id='model_metrics',
        python_callable=get_metrics,
        dag=dag
      )
    
    report_track = PythonOperator(
        provide_context=True,
        task_id='track_report',
        python_callable=track_report,
        dag=dag      
      )  

    # stop = DummyOperator(
    #       task_id='keep_old_model',
    #       dag=dag,
    #       trigger_rule="all_done",
    #   )

    data_ingestion >> data_track >> model_training >> model_track >>model_sampling >> data_synth_track >> model_metrics >> report_track