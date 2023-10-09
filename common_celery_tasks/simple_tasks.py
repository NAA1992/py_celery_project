import os

from time import sleep

from celery import current_app
from celery import shared_task
from celery import uuid

@shared_task(name = 'simple_celery.hello', max_retries=5)
def hello():
    print ("Hello")
    return "result_Hello"

@shared_task(name = 'simple_celery.world', max_retries=5)
def world():
    print ("World")
    return "result_World"


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