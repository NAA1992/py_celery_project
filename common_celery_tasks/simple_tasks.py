from celery import current_app
from celery import shared_task

@shared_task(name = 'simple_celery.hello', max_retries=5)
def hello():
    print ("Hello")
    return "result_Hello"

@shared_task(name = 'simple_celery.world', max_retries=5)
def world():
    print ("World")
    return "result_World"