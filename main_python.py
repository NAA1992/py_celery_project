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

from time import perf_counter
from time import sleep

from celery.exceptions import MaxRetriesExceededError
from celery.result import AsyncResult
from celery.result import allow_join_result
from celery import Celery
from celery import uuid
from celery import current_app 
from celery import shared_task
from celery import chain

from celery_admin.project_config import ETLConfig
from celery_admin.project_config import CeleryConfig
from celery_admin.project_config import celery_internal_tasks

from common_celery_tasks.api_tasks import Prostore
from common_celery_tasks.api_tasks import csv_api

# CFG ETL, Extracts
current_config = ETLConfig('proj_conf.yaml')
current_config.cfgs_extract()

# Celery
celeryclass = CeleryConfig()
celeryclass.celeryconfig(current_config)
app = celeryclass.celeryapp()

""" # Присвоить из словаря ключи и значения себе
dict_app = {k: v for k, v in vars(current_config).items() if k.startswith('app')}
for key,val in dict_app.items():
        exec(key + '=val')
"""



logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

# url = "http://localhost:8080/api/csv/mz_frmo_frmr.mo_license"

 



####################################################################











    
@shared_task(bind=True)
def chain_csv(self, source_file):
    """
    for i in range(3):
        task_id_uuid = uuid()
        send_api_csv_file.apply_async(kwargs={
                                        'source_file': source_file 
                                        , 'table_name': 'mo_license'
                                        }, add_to_parent =False, task_id = task_id_uuid, countdown=3)
        res_tasks_async = AsyncResult(task_id_uuid)
        while res_tasks_async.ready() == False:
            res_tasks_async = AsyncResult(task_id_uuid)
    """
            
    """for i in range(2):
        send_api_csv_file.apply_async(kwargs={
                                'source_file': source_file
                                , 'table_name': 'mo_license'
                                }, countdown=1)"""
    #hello_get = hello.get()
    
    #pus = chain(
    #    hello.si()
    #    , world.si()
    #    ).delay()
    
    print(type(self.request.children))
    
    for i in self.request.children:
        task_obj = AsyncResult(i)
        print(i)
        print(type(i))
        print(type(task_obj))
        if task_obj.ready():
            with allow_join_result():
                res = task_obj.get()
                print(res)
    
    """chain(
        send_api_csv_file.si(source_file=source_file, table_name='mw_personal_file_record')
        , send_api_csv_file.si(source_file=source_file, table_name='mw_personal_file_record')
        ).delay()"""
    #ret = chain(send_api_csv_file.s('POST', source_file, 'mz_frmo_frmr', 'mo_license'),send_api_csv_file.s('POST', source_file, 'mz_frmo_frmr', 'mo_license')).apply_async()
    #chain = send_api_csv_file.s('POST', source_file, 'mz_frmo_frmr', 'mo_license') | send_api_csv_file.s('POST', source_file, 'mz_frmo_frmr', 'mo_license')
    #chain()

if __name__ == "__main__":
    ### БЛОК CELERY ###
    celeryclass.celery_set_config()
    celeryclass.apply_schedules()
    
    """csv_file_to_upload="./etl_extract/mw_personal_file_record.csv"
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
            string_to_api += "\n"+ ";".join(row)"""
    
    
        
    # Запускаем задачки
    
    app.send_task(name = 'prostore_send_query', kwargs={
        'def_query': 'select count(*) from medical_worker'
        , 'def_url': 'http://localhost:9090/api/v1/datamarts/mz_frmo_frmr/query?format=json'
    })
    
    #(send_api_csv_file.s('POST', csv_file_to_upload, 'mz_frmo_frmr', 'mo_license').set(countdown=1) | send_api_csv_file.s('POST', csv_file_to_upload, 'mz_frmo_frmr', 'mo_license').set(countdown=1)).delay()
    """chain_csv.apply_async(kwargs={
                                'source_file': csv_file_to_upload 
                                }, countdown=1)"""
    """
    for cnt_tasks in range (2):
        send_api_csv_file.apply_async(kwargs={
                                        'source_file': csv_file_to_upload 
                                        , 'table_name': 'mo_license'
                                        }, countdown=3)
    
    
    for cnt_tasks in range (1):
        task_for_celery.apply_async(kwargs={
                                        'sleep_seconds': 3
                                        , 'cnt_values': 900
                                        }, countdown=3)
    for cnt_tasks in range (1):
            task_sleep_and_generate_child.apply_async(kwargs={
                                            'sleep_seconds': 900
                                            , 'cnt_subtasks': 10
                                            }, countdown=3)
    """
    
    
    # Если количество потоков не указано, то количество потоков = количеству CPU
    app.worker_main(argv=['worker', f'--concurrency={celeryclass.celery_cfg_threads}', f'--loglevel={celeryclass.cloglevel}', '-S', 'celery.beat.PersistentScheduler','-E', '-B', '--without-heartbeat'])
    sys.exit('WE HAVE FINISHED ETL')