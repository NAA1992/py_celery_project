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
url_csv_uploader_base = "http://localhost:8080/api/csv/"
 



####################################################################

@shared_task(bind=True, max_retries=5)
def send_api_csv_file(self, method='POST', source_file='', scheme='mz_frmo_frmr', table_name=''):
    try:    
        try:
            # method: POST - upsert, DELETE - delete
            # table_name: mo_license OR medical_worker OR ...
            # source_file: ./etl_extract/mo_license.csv

            headers_binary = {
                        'Content-Type': 'application/octet-stream'
                        }
            url_binary = url_csv_uploader_base + scheme + '.' + table_name
            
            with open(source_file, 'rb') as f:
                binary_data = f.read()
            
            response_binary = requests.request(
                                    method
                                    , url_binary
                                    , data = binary_data
                                    , headers = headers_binary)
            print(f"""
                {method} {table_name} {source_file}, КОД СТАТУСА: {response_binary.status_code}
                ОТВЕТ: {response_binary.text}
                """, )
            if response_binary.status_code != 200:
                pass
            return('OK')
        except Exception as e:
                pass
                #logger.error(f'retry: {self.request.retries}/{self.max_retries}, {self.request.task} ERROR: '+ str(e))
                #raise self.retry(countdown=10)
    except MaxRetriesExceededError:
        pass
        # kill_celery.apply_async(priority=1, countdown=3)
        # app.send_task(name="simple_celery_python.kill_celery", priority=1, countdown=0)

@shared_task(bind=True, max_retries=5)
def send_api_csv_text(self, method='POST', string_to_upload='', scheme='mz_frmo_frmr', table_name=''):
    try:    
        try:
            # method: POST - upsert, DELETE - delete
            # table_name: mo_license OR medical_worker OR ...
            # source_file: ./etl_extract/mo_license.csv
            headers_binary = {
                        'Content-Type': 'text/plain'
                        }
            url_binary = url_csv_uploader_base + scheme + '.' + table_name
            
            response_binary = requests.request(
                                    method
                                    , url_binary
                                    , data = string_to_upload
                                    , headers = headers_binary)
            print(f"""
                {method} {table_name} ТЕКСТ, КОД СТАТУСА: {response_binary.status_code}
                ОТВЕТ: {response_binary.text}
                """, )
            if response_binary.status_code != 200:
                pass
            return('OK')
        except Exception as e:
                logger.error(f'retry: {self.request.retries}/{self.max_retries}, {self.request.task} ERROR: '+ str(e))
                raise self.retry(countdown=10)
    except MaxRetriesExceededError:
        pass
        # kill_celery.apply_async(priority=1, countdown=3)
        # app.send_task(name="simple_celery_python.kill_celery", priority=1, countdown=0)


@shared_task(bind=True)
def kill_celery(self):
    print('Поступила команда на убийство всех процессов')
    cntrl_celery = app.control.inspect()
    active_tasks_by_host = cntrl_celery.active()
    # list_active_tasks = list(active_tasks.values())
    # res_tasks_async = AsyncResult(self.request.id)
    # print(type(active_tasks))
    # print(type(active_tasks.values()))
    # print (active_tasks[0])
    #app.control.shutdown(destination=[self.request.hostname])
    for hostname in active_tasks_by_host:
        active_tasks_all = active_tasks_by_host[hostname]  # Количество активных задач
    for num_act in active_tasks_all:
        print (num_act.get('id'))
        if num_act.get('id') != self.request.id:
            app.control.revoke(num_act.get('id'), terminate=True, signal='SIGKILL')
    print('Убийство процессов произошло. Теперь идет команда на Warm Termination')
    # app.control.revoke(self.request.id) # prevent this task from being executed again
    app.control.shutdown(destination=[self.request.hostname])
    print('Полный процесс убийства и теплого завершения закончен. Выходим')
    sys.exit(1)  



@shared_task(bind=True)
def task_for_celery(self, sleep_seconds, cnt_values):
    list_values = []
    print (f"""
        Идентификатор задачи запущенной: {self.request.id}
        Номер процесса внутри кампуктера: {os.getpid()}
        Наименование задачи: {self.request.task}
            """
            )
    for num in range (cnt_values):
        uuid_value = uuid()
        list_values.append(uuid_value)
    print(f"""
        Идентификатор задачи запущенной: {self.request.id}
        Номер процесса внутри кампуктера: {os.getpid()}
        Наименование задачи: {self.request.task}
        Мы нагенерировали {cnt_values} значений
        И щас заснем на {sleep_seconds} секунд
          """)
    sleep(sleep_seconds)
    print(f"""
        Я ЗАВЕРШАЮСЬ
        Идентификатор задачи запущенной: {self.request.id}
        Номер процесса внутри кампуктера: {os.getpid()}
        Наименование задачи: {self.request.task}
        """
        )
    
    
@shared_task(bind=True)
def task_sleep_and_generate_child(self, sleep_seconds, cnt_subtasks):
    print (f"""
        Идентификатор задачи запущенной: {self.request.id}
        Номер процесса внутри кампуктера: {os.getpid()}
        Наименование задачи: {self.request.task}
        Здесь я буду создавать {cnt_subtasks} подзадач
        Они и я будем засыпать на {sleep_seconds} секунд            
            """
            )
    for subtask in range(cnt_subtasks):
        subtask_just_sleep.apply_async(kwargs={
                                            'sleep_seconds': sleep_seconds
                                            }, add_to_parent=False, countdown=3)
    sleep(sleep_seconds)
    print(f"""
        Я ЗАВЕРШАЮСЬ
        Идентификатор задачи запущенной: {self.request.id}
        Номер процесса внутри кампуктера: {os.getpid()}
        Наименование задачи: {self.request.task}
        """
        )
    
@shared_task(bind=True)
def subtask_just_sleep(self, sleep_seconds):
    print (f"""
        Я - ДОЧЕРНЯЯ ЗАДАЧА. Я ПРОСТО ЗАСЫПАЮ НА {sleep_seconds} секунд 
        Идентификатор задачи запущенной: {self.request.id}
        Номер процесса внутри кампуктера: {os.getpid()}      
            """
            )
    sleep(sleep_seconds)
    print (f"""
        Я - ДОЧЕРНЯЯ ЗАДАЧА. Я ПРОСНУЛАСЬ И ЗАВЕРШАЮСЬ
        Идентификатор задачи запущенной: {self.request.id}
        Номер процесса внутри кампуктера: {os.getpid()}
        Наименование задачи: {self.request.task}    
        """
        )
    
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
    
    chain(
        send_api_csv_file.si(source_file=source_file, table_name='mw_personal_file_record')
        , send_api_csv_file.si(source_file=source_file, table_name='mw_personal_file_record')
        ).delay()
    #ret = chain(send_api_csv_file.s('POST', source_file, 'mz_frmo_frmr', 'mo_license'),send_api_csv_file.s('POST', source_file, 'mz_frmo_frmr', 'mo_license')).apply_async()
    #chain = send_api_csv_file.s('POST', source_file, 'mz_frmo_frmr', 'mo_license') | send_api_csv_file.s('POST', source_file, 'mz_frmo_frmr', 'mo_license')
    #chain()

if __name__ == "__main__":
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
    
    
        
    # Запускаем задачки
    
    
    
    #(send_api_csv_file.s('POST', csv_file_to_upload, 'mz_frmo_frmr', 'mo_license').set(countdown=1) | send_api_csv_file.s('POST', csv_file_to_upload, 'mz_frmo_frmr', 'mo_license').set(countdown=1)).delay()
    chain_csv.apply_async(kwargs={
                                'source_file': csv_file_to_upload 
                                }, countdown=1)
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