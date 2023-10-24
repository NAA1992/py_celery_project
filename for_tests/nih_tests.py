import csv
import os
import math
import datetime
import ntpath
import time

from itertools import (takewhile,repeat)

import datetime as dt

start_test = time.time()
time.sleep(0.1)
end_test = time.time()
delta_test = end_test - start_test
print(start_test, end_test, delta_test)
string_test = './mw_medical_worker.csv'
string_search = 'medical_worker'
string_path = './etl_extract/KAKASHKA.csv'
string_snizu = 'pizdez_'
number_for_delit = 4/3

print(string_snizu[-1:] == '_')
print(string_snizu[-1:])

print(number_for_delit)
print(round(number_for_delit, 2))
print((ntpath.basename(string_path)[:-4]))
print (string_search in string_test)


begin_etl = dt.datetime(2023,9,18,15,43,13)
current_time = dt.datetime(2023,9,18,15,55,50)
date_prognoz_vnutri_etl = dt.datetime(2023,9,21,17,41,6)

raznica_begin = (date_prognoz_vnutri_etl-begin_etl).total_seconds()

second_page = 160.502
ost_pages = 1659

second_and_pages = ost_pages * second_page

prognoz_begin = begin_etl + dt.timedelta(seconds=second_and_pages)
prognoz_current = current_time + dt.timedelta(seconds=second_and_pages)
prognoz_vnutri_etl = date_prognoz_vnutri_etl

print('*'*50)
print(prognoz_begin)
print('*'*50)
print(prognoz_current)
print('*'*50)
print(prognoz_vnutri_etl)
print('*'*50)




def rawincount(filename):
    f = open(filename, 'rb')
    bufgen = takewhile(lambda x: x, (f.raw.read(1024*1024) for _ in repeat(None)))
    return sum( buf.count(b'\n') for buf in bufgen )


with open('./etl_extract/KAKASHKA.csv', 'w', newline='') as file:
                    writer = csv.writer(file, delimiter =';')
                    writer.writerow([
                        'mw_id'
                        , 'mw_oid'
                        , 'last_name'
                        , 'first_name'
                        , 'middle_name'
                        , 'work_experience'
                        ])
                    file.close()
string1 = '""'
string2 = "''str2"
string3 = string1+string2

LISTNAH = []
LISTNAH.append(string3)
LISTNAH.append(string2)
LISTNAH.append(string1)
print('list')
print(LISTNAH)


hztype = ''
print(type(hztype))
print(hztype)
print(str(hztype))
if hztype is None:
    print('DETECTED')
else:
    print('fuck')

file_data = [string3,string3,3,4,5,6]


for i in range(len(file_data)):
    file_data[i] = str(file_data[i]).replace('"','""').replace("'","''")
print(file_data[0])
print(file_data[1])
"""with open('./etl_extract/KAKASHKA.csv', 'a') as file:
                writer = csv.writer(file, delimiter =';', quoting=csv.QUOTE_ALL)
                writer.writerow(file_data)
                file.close()"""
memory_total = 14236
workers = 5
print (memory_total / workers)
print (min(memory_total / 100000, workers, 2))

hui = 1

while hui < 5:
    print(hui)
    hui += 1

cifra_begin = 1
cifra_delitel = 2

cifra_itog = cifra_begin / cifra_delitel
print (f'Цифра = {cifra_itog}')
print (f'Цифра вверх = {math.floor(cifra_itog)}')
print (f'Цифра вниз = {math.ceil(cifra_itog)}')




print(datetime.datetime.now())
print('Начало подсчетов')
cwd = os.getcwd()
file_path = "/home/alex/datamodels-check/etl_extract/bigfile.csv"

print(file_path[:-4])

file_size = os.path.getsize(file_path)
print(f'Размер файла в байтах = {file_size}')
file_size_mb = file_size /1000 / 1000
print(f'Размер файла в МЕГАбайтах = {file_size_mb}')
numlines = rawincount(file_path)
print(f'Количество строк в файле = {numlines}')
size_one_line = math.ceil(file_size / numlines)
print(f'Размер одной строки в файле примерно = {size_one_line}' )
lines_in_one_mb = math.ceil( (1024*1024) / size_one_line)
print(f'Количество строк в 1 МБ примерно = {lines_in_one_mb}')

rezat_na_mb = math.ceil(200 * lines_in_one_mb)

print(datetime.datetime.now())
print('Конец подсчетов')

import pandas as pd

infile = file_path

n=0
"""for chunk in pd.read_csv(infile, sep = ';', dtype='unicode', chunksize=rezat_na_mb):
    data = chunk
    oPath = file_path[:-4] +'_' +str(n)+'.csv'
    data.to_csv(oPath, sep=';',index=False, header=True)
    n +=1"""
print(datetime.datetime.now())
print('Конец Разреза')


"""

def control_celery(self):
    # Inspect all nodes.
    cntrl_celery = app.control.inspect()

    print('='*50)
    print('SCHEDULED')
    print(cntrl_celery.scheduled())
    print('='*50)

    print('-'*50)
    #scheduled_tasks = cntrl_celery.scheduled().values()
    scheduled_tasks = cntrl_celery.scheduled()
    print(type(scheduled_tasks))
    #print(scheduled_tasks)
    for hostname in scheduled_tasks:
        print (hostname)
        print (len(scheduled_tasks[hostname]))  # Количество запланированных задач
        for a in scheduled_tasks[hostname]:
            print ('*'*50)
            print(a)
            print ('*'*50)
        #print (scheduled_tasks[hostname], sep = '\n', end='\n')
    print('-'*50)
    #v = list(scheduled_tasks.items())
    #print(len(v))

    print('='*50)
    print('active')
    # Show tasks that are currently active.
    print(cntrl_celery.active())
    print('='*50)

    print('='*50)
    print('reserved')
    # Show tasks that have been claimed by workers
    print(cntrl_celery.reserved())
    print('='*50)

    # app.conf.task_default_queue = 'celery_default_key'       ## По умолчанию routing_key (наименование очереди) = celery. Здесь мы меняем значение по умолчанию

@shared_task(bind=True)
def task_shutdown_workers(self, flag_error=1):
    print('='*50,'='*50,"ЗАВЕРШАЕМ все WORKER'ы и ВЫХОДИМ из ETL",'='*50,'='*50, sep = '\n')

    #app.control.revoke(self.request.id) # prevent this task from being executed again
    # app.control.shutdown() # send shutdown signal to all workers

    # В качестве exception можно юзать
    try:
        pass
        # some init logic...
    except Exception:
        # this log will include traceback
        logger.exception('function_will_exit failed with exception')
        # this log will just include content in sys.exit
        logger.error(str('EXIT ERROR'))
        raise Celery.exceptions.WorkerShutdown()

    # Запускаем задачки



    #(send_api_csv_file.s('POST', csv_file_to_upload, 'mz_frmo_frmr', 'mo_license').set(countdown=1) | send_api_csv_file.s('POST', csv_file_to_upload, 'mz_frmo_frmr', 'mo_license').set(countdown=1)).delay()
    chain_csv.apply_async(kwargs={
                                'source_file': csv_file_to_upload
                                }, countdown=1)

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


""" # Присвоить из словаря ключи и значения себе
dict_app = {k: v for k, v in vars(current_config).items() if k.startswith('app')}
for key,val in dict_app.items():
        exec(key + '=val')
"""
