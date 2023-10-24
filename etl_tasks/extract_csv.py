import requests

from celery import current_app, shared_task
from celery.exceptions import MaxRetriesExceededError

shared_task(bind=True, max_retries=5)
def send_api_csv_file(self, method='POST', source_file='', scheme='mz_frmo_frmr', table_name='', url_csv_uploader_base = ''):
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
def send_api_csv_text(self, method='POST', string_to_upload='', scheme='mz_frmo_frmr', table_name='', url_csv_uploader_base = ''):
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
                # logger.error(f'retry: {self.request.retries}/{self.max_retries}, {self.request.task} ERROR: '+ str(e))
                raise self.retry(countdown=10)
    except MaxRetriesExceededError:
        pass
        # kill_celery.apply_async(priority=1, countdown=3)
