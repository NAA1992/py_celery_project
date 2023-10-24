import os
import sys
import math
import logging
import datetime
import psutil
import requests
import json
import csv
import pandas
import pytz

from etl_tasks.etl_source.api_source import API_SOURCE

from time import perf_counter, sleep

from celery.exceptions import MaxRetriesExceededError
from celery.result import AsyncResult , allow_join_result
from celery import Celery, uuid, current_app, shared_task, chain


from celery_admin.project_config import ETLConfig
from celery_admin.project_config import CeleryConfig

from celery_admin.project_admin import ETLTask

# CFG ETL, Extracts
current_config = ETLConfig('proj_conf.yaml')
current_config.cfgs_extract()
api_action = API_SOURCE(def_url=current_config.settings.source_api.url)
etl_task = ETLTask()

# Celery
celeryclass = CeleryConfig()
celeryclass.celeryconfig(current_config)
app = celeryclass.celeryapp()

logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

@shared_task(bind=True, name = 'from_api_to_database')
def from_api_to_database(self):
    etl_task.init_db(current_config)
    data_from_source = api_action.call_api(def_method='GET')

    for i in data_from_source:
        etl_task.upsert_data(i)


if __name__ == "__main__":


    celeryclass.celery_set_config()
    celeryclass.apply_schedules()

    current_directory = os.getcwd()
    files_in_current_directory = os.listdir(current_directory)

    for item in files_in_current_directory:
        if item.startswith("celery.beat.PersistentScheduler") or item.startswith("celerybeat-schedule"):
            os.remove(os.path.join(current_directory, item))


    # Если количество потоков не указано, то количество потоков = количеству CPU
    app.worker_main(argv=['worker', f'--concurrency={celeryclass.celery_cfg_threads}', f'--loglevel={celeryclass.cloglevel}', '-S', 'celery.beat.PersistentScheduler','-E', '-B', '--without-heartbeat'])





    """

    ### БЛОК CELERY ###
    celeryclass.celery_set_config()
    celeryclass.apply_schedules()

    csv_file_to_upload="./etl_extract/mw_personal_file_record.csv"
    if os.path.isfile(csv_file_to_upload) == True:
        # pandas_csv = pandas.read_csv(csv_file_to_upload, delimiter=';')
        rows_from_source = []
        with open(csv_file_to_upload, 'r') as file:
            csvreader = csv.reader(file, delimiter=";")
            header = next(csvreader)
            for row_from_source in csvreader:
                rows_from_source.append(row_from_source)

        string_to_api = ";".join(header)
        for row in rows_from_source:
            string_to_api += "\n"+ ";".join(row)

    # Если количество потоков не указано, то количество потоков = количеству CPU
    # app.worker_main(argv=['worker', f'--concurrency={celeryclass.celery_cfg_threads}', f'--loglevel={celeryclass.cloglevel}', '-S', 'celery.beat.PersistentScheduler','-E', '-B', '--without-heartbeat'])
    # sys.exit('WE HAVE FINISHED ETL')
    """
