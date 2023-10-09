import uuid
import requests
import json

from celery import shared_task
from celery.exceptions import MaxRetriesExceededError


class Prostore():
    
    def __init__(self):
        self.url_prostore = 'http://localhost:9090/api/v1/datamarts/mz_frmo_frmr/query?format=json'
    
    @shared_task(bind=True, name='prostore_send_query', max_retries=5)
    def send_query(self, def_url = '', method='POST', scheme='mz_frmo_frmr', def_query = ''):
        try:    
            try:
                request_data = {"query": def_query}
                json_data_string = json.dumps(request_data, ensure_ascii=False)
                
                if def_url == '':
                    def_url = self.url_prostore
                _uuid = uuid.uuid4()               
                headers = {
                            'Content-Type': 'application/json'
                            , 'x-request-id': str(_uuid)
                            }
                
                response = requests.request(
                                        method
                                        , def_url
                                        , data = json_data_string.encode('utf-8')
                                        , headers = headers)
                print(f"""
                    {method} ТЕКСТ, КОД СТАТУСА: {response.status_code}
                    ОТВЕТ: {response.json()}
                    """, )
                if response.status_code != 200:
                    pass
                return(response.json())
            except Exception as e:
                    print(f'retry: {self.request.retries}/{self.max_retries}, {self.request.task} ERROR: '+ '\n' + str(e))
                    raise self.retry(countdown=10)
        except MaxRetriesExceededError:
            pass
            # kill_celery.apply_async(priority=1, countdown=3)
            # app.send_task(name="simple_celery_python.kill_celery", priority=1, countdown=0)
            






class csv_api():
    
    def __init__(self):
        self.url_csv_uploader_base = "http://localhost:8080/api/csv/"
    
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
                url_binary = self.url_csv_uploader_base + scheme + '.' + table_name
                
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
                    print(f'retry: {self.request.retries}/{self.max_retries}, {self.request.task} ERROR: '+ str(e))
                    raise self.retry(countdown=10)
        except MaxRetriesExceededError:
            pass
            # kill_celery.apply_async(priority=1, countdown=3)
            # app.send_task(name="simple_celery_python.kill_celery", priority=1, countdown=0)
            
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
                url_binary = self.url_csv_uploader_base + scheme + '.' + table_name
                
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