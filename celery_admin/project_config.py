import os
import yaml
import psutil

from dotmap import DotMap
from celery import Celery

class ETLConfig():

    settings = ""
    filename = ""

    def __init__(self, yaml_file_config):
        self.filename = yaml_file_config
        if os.path.exists(yaml_file_config):
            with open(self.filename, 'r') as yaml_file:
                try:
                    config_d = yaml.safe_load(yaml_file)
                    self.settings = DotMap(config_d)
                except yaml.YAMLError as exc:
                    raise exc
        else:
            raise Exception(f"File with config {yaml_file_config} not found localy")
        
    def cfgs_extract (self):
        # enable debugging SQL in logs
        self.sql_debug = self.settings.sql_debug
        
        
    def create_example(self):
        self.settings.datamart.etlsstats.data.host = "localhost"
        self.settings.datamart.etlsstats.data.port = 6565
        self.settings.datamart.etlsstats.data.user = "postgres"
        self.settings.datamart.etlsstats.data.password = "postgrespassword"
        self.settings.datamart.etlsstats.dbname = "frmofrmrdatamart_v5"

        self.settings.celery.broker_url = "redis://localhost:6379/0" 
        self.settings.celery.result_backend = "redis://localhost:6379/0"
        self.settings.celery.loglevel = "warning"
        self.settings.sql_debug = 0  

        with open(self.filename, 'w') as yaml_file:
           yaml.dump(self.settings.toDict(), yaml_file, default_flow_style=False)

    def get_variable(self,varname):
        print(getattr(self,varname))
        return getattr(self,varname)

######## ПРИОРИТЕТЫ ПО CELERY ОГРАНИЧЕНИЯМ #######
# Если CFG количество потоков > 0, то запускается количество потоков, указанное в CFG, иначе - потоков = количеству CPU

# Если включено процент от общего объема памяти - высчитываем это значения с учетом потоков
## Проверяется, чтоб процент был больше 0. Если 0 и меньше - деактивируется CFG ограничение по проценту
## Если больше ноля, то берем в расчет общий объем памяти (ex. 10 Gb), процент (ex. 5), количество потоков (ex. 2) 
## 5% от 10 ГБ = 500 МБ. Так как потоков 2, то 500 МБ \ 2 = каждому потоку ставится ограничение 250 МБ

# Если включен указанный объем общий:
## Если значение 0 и меньше - кфг деактивируется, иначе:
## Проверяется включен ли еще и процент общего объема памяти. Если да - в конфиг ограничений попадает попадает наименьшее значение.
## Например, четкое значение указано 200 МБ. 200 МБ меньше, чем выше рассчитанный 500 МБ, следовательно на каждый поток попадет 200 \ 2 (потока) = 100 МБ

# Если включен количество памяти на каждый воркер, то 
## Если значение 0 и меньше - кфг деактивируется, иначе:
## проверяются вышеуказанные параметры и применяется наименьшее. 
## Например, на каждый воркер указано 10 МБ. Расчеты выше показали 250 МБ и 100 МБ, 10 МБ - наименьшее это и будет применено

# Если ничего из ограничений памяти применено - выводим в лог "!!!WARNING!!! MEMORY USAGE INFINITE" и Celery запускается без ограничения по памяти

# Если указано количество потоков больше 0 > Применяем количество из CFG, 
# иначе - количество CPU и выводим в лог "!!!WARNING!!! Count of workers (threads) = ", количество

# Если указано количество задач на Worker и оно больше 0, то выводим в лог "!!!WARNING!!! Count tasks per worker = ", количество
# Если НЕ указано или указано 0 и меньше - выводим в лог "!!!WARNING!!! Count tasks per worker - INFINITE"

# Если мы вычсчитали все-таки сколько нужно ограничиться по памяти для каждого Worker (потока), то проверяем
# Сколько доступной памяти? Если она меньше, чем количество потоков УМНОЖИТЬ на ограничение памяти, то выводим в лог
# !!!WARNING!!! Total memory = X, available = Y, max_memory_usage_ETL = Z 
# !!!WARNING!!! We have Risk take OutOfMemory

# Если включен признак очищения очереди задач, то очищаем и выводим в лог "!!!WARNING!!! We clearing celery queue"
# Если выключен признак очищения очереди задач, то просто выводим в лог "!!!WARNING!!! We NOT clearing celery queue"
class CeleryConfig():
    def celeryapp(self):
        self.app = Celery('py_celery_app', 
                broker = self.broker,
                backend= self.backend ,
                timezone='Europe/Moscow',
                result_expires=10800,    # value in seconds
                broker_connection_retry_on_startup=True,
                fixups=[])
        return self.app
        
    
    def celeryconfig(self, current_config):
        self.broker = current_config.settings.celery.broker_url
        self.backend = current_config.settings.celery.result_backend
        
        sys_cpu_cnt = os.cpu_count()
        sys_memory_available = psutil.virtual_memory().available
        sys_memory_total = psutil.virtual_memory().total
        
        ### TTL результатов внутри REDIS. По дефолту 86400 (сутки)
        self.result_expires = current_config.settings.celery.result_expires_minutes * 60
        if self.result_expires < 1:
            self.result_expires = 86400
        
        ### Время в секундах для очищения задач с истекшим TTL. По дефолту 04:00, но мы делаем дефолт 86400 (сутки)
        self.schedule_backend_cleanup = current_config.settings.celery.schedule_backend_cleanup_minutes * 60
        if self.schedule_backend_cleanup < 1:
            self.schedule_backend_cleanup = 86400
        
        #### Флаги активации ограничений Celery
        # Ограничение по проценту от общего объема памяти
        celery_enable_total_percent_memory = current_config.settings.celery.limits_enable.total_percent_memory
        # Ограничение по четко заданному общему объему памяти
        celery_enable_total_size_memory = current_config.settings.celery.limits_enable.total_size_kb_memory
        # Ограничение по объему памяти на 1 поток (worker)
        celery_enable_perworker_size_memory = current_config.settings.celery.limits_enable.perworker_size_kb_memory
        # Ограничение по количеству задач на 1 поток (worker)
        celery_enable_perworker_cnt_tasks = current_config.settings.celery.limits_enable.perworker_cnt_tasks
        # Ограничение по количеству ЗАРЕЗЕРВИРОВАННЫХ задач на 1 поток (worker)
        celery_enable_perworker_reserved_tasks = current_config.settings.celery.limits_enable.perworker_reserved_tasks
        # Ограничение по количеству потоков (workers)
        celery_enable_threads = current_config.settings.celery.limits_enable.threads


        #### Значения ограничений Celery
        # Ограничение по проценту от общего объема памяти
        celery_value_total_percent_memory = current_config.settings.celery.limits_values.total_percent_memory
        # Ограничение по четко заданному общему объему памяти
        celery_value_total_size_memory = current_config.settings.celery.limits_values.total_size_kb_memory
        # Ограничение по объему памяти на 1 поток (worker)
        celery_value_perworker_size_memory = current_config.settings.celery.limits_values.perworker_size_kb_memory
        # Ограничение по количеству задач на 1 поток (worker)
        celery_value_perworker_cnt_tasks = current_config.settings.celery.limits_values.perworker_cnt_tasks
        # Ограничение по количеству ЗАРЕЗЕРВИРОВАННЫХ задач на 1 поток (worker)
        celery_value_perworker_reserved_tasks = current_config.settings.celery.limits_values.perworker_reserved_tasks
        # Ограничение по количеству потоков (workers)
        celery_value_threads = current_config.settings.celery.limits_values.threads

        #### Признак очищения очереди задач
        celery_clear_queue = current_config.settings.celery.clear_queue
        
        #### Уровень логирования
        cloglevel = current_config.settings.celery.loglevel

        #### Переменные значения для КФГ Celery
        celery_cfg_perworker_size_memory = 0
        
        # Максимальное количество приоритетов и приоритет по умолчанию
        task_queue_max_priority = 10
        task_default_priority = 5

        # Количество потоков
        if celery_enable_threads > 0 and celery_value_threads > 0:
            celery_cfg_threads = celery_value_threads
        else:
            celery_enable_threads = 0
            celery_cfg_threads = sys_cpu_cnt

        # Считаем процент от общего объема памяти
        if celery_enable_total_percent_memory == 1 and celery_value_total_percent_memory > 0:
            celery_cfg_perworker_size_memory = (sys_memory_total / 100 * celery_value_total_percent_memory) / celery_cfg_threads
        else:
            celery_enable_total_percent_memory = 0
                
        # Смотрим что меньше - четко указанный объем или процент от него
        if celery_enable_total_size_memory == 1 and celery_value_total_size_memory > 0:
            if celery_cfg_perworker_size_memory > 0: # Признак, что уже раннее считали для ограничений объем
                celery_cfg_perworker_size_memory = min(celery_value_total_size_memory / celery_cfg_threads, celery_cfg_perworker_size_memory)
            else:
                celery_cfg_perworker_size_memory = celery_value_total_size_memory / celery_cfg_threads
        else:
            celery_enable_total_size_memory = 0

        # Смотрим что меньше - что-то из выше или четко указанный размер на каждый поток:
        if celery_enable_perworker_size_memory == 1 and celery_value_perworker_size_memory > 0:
            if celery_cfg_perworker_size_memory > 0: # Признак, что уже раннее считали для ограничений объем
                celery_cfg_perworker_size_memory = min(celery_value_perworker_size_memory, celery_cfg_perworker_size_memory)
            else:
                celery_cfg_perworker_size_memory = celery_value_perworker_size_memory
        else:
            celery_enable_perworker_size_memory = 0

        # Количество задач на worker, после которого идет перезапуск
        if celery_enable_perworker_cnt_tasks == 1 and celery_value_perworker_cnt_tasks > 0:
            celery_cfg_perworker_cnt_tasks = celery_value_perworker_cnt_tasks
        else:
            celery_cfg_perworker_cnt_tasks = 0
            
        # Максимальное число зарезервированных задач под 1 worker. По умолчанию 4
        if celery_enable_perworker_reserved_tasks > 0 and celery_value_perworker_reserved_tasks >= 0:
            celery_cfg_perworker_reserved_tasks = celery_value_perworker_reserved_tasks
        else:
            celery_enable_perworker_reserved_tasks = 0
            celery_cfg_perworker_reserved_tasks = 4
            
        
            
        ### Возвращаем все КФГ-шки
        self.sys_cpu_cnt = sys_cpu_cnt
        self.sys_memory_available = sys_memory_available
        self.sys_memory_total = sys_memory_total
        self.celery_clear_queue = celery_clear_queue
        self.celery_enable_threads = celery_enable_threads
        self.celery_cfg_threads = celery_cfg_threads
        self.celery_cfg_perworker_size_memory = celery_cfg_perworker_size_memory
        self.celery_cfg_perworker_cnt_tasks = celery_cfg_perworker_cnt_tasks
        self.celery_enable_perworker_reserved_tasks = celery_enable_perworker_reserved_tasks
        self.celery_cfg_perworker_reserved_tasks = celery_cfg_perworker_reserved_tasks
        self.cloglevel = cloglevel
        self.task_queue_max_priority = task_queue_max_priority
        self.task_default_priority = task_default_priority

    def celery_set_config(self):
        # Выводим в лог то что получилось по памяти
        if self.celery_cfg_perworker_size_memory > 0:
            print(f'!!!WARNING!!! У каждого из потоков ограничения по памяти = {self.celery_cfg_perworker_size_memory} КБ. Всего потоков = {self.celery_cfg_threads}')
            if self.celery_cfg_perworker_size_memory > self.sys_memory_available:
                print(f'!!!WARNING!!! Ограничение по памяти превышает доступное в системе памяти. Доступное в системе памяти = {self.sys_memory_available}')
        else:
            print(f'!!!WARNING!!! У потоков нет ограничения по используемой памяти. Всего потоков = {self.celery_cfg_threads}')
        
        # Количество задач на поток
        if self.celery_cfg_perworker_cnt_tasks > 0:
            print(f'!!!WARNING!!! Количество задач на поток, после которого поток перезапустится = {self.celery_cfg_perworker_cnt_tasks}')
        else:
            print('!!!WARNING!!! Нет ограничений по количеству задач на поток')
            
            
        # Максимальное число зарезервированных задач под 1 worker. По умолчанию 4
        if self.celery_enable_perworker_reserved_tasks > 0:
            if self.celery_cfg_perworker_reserved_tasks > 0:
                print (f'!!!WARNING!!! Количество зарезервированных задач на worker = {self.celery_cfg_perworker_reserved_tasks}')    
            elif self.celery_cfg_perworker_reserved_tasks == 0:
                print ('!!!WARNING!!! Количество зарезервированных задач UNLIMITED!!! Есть риск Out of Memory')
        else:
            print (f'!!!WARNING!!! Количество зарезервированных задач на worker ПО УМОЛЧАНИЮ = {self.celery_cfg_perworker_reserved_tasks}')
            
        
        # В связи с многопоточностью - часто тасков больше, чем потоков, в связи с чем таски в очереди выполняются вместе с перезапуском ETL
        # Приходится во время перезапуска очищать всю очередь
        if self.celery_clear_queue > 0:
            print('!!!WARNING!!! Очищаем у брокера очередь сообщений')
            self.app.control.purge()
        else:
            print('!!!WARNING!!! Очередь сообщений у брокера остается той же')
        
        # Максимальное количество приоритетов и приоритет по умолчанию
        self.app.conf.task_queue_max_priority = 10
        self.app.conf.task_default_priority = 5
        

    
        # Ограничение по памяти
        if self.celery_cfg_perworker_size_memory > 0:
            self.app.conf.worker_max_memory_per_child = self.celery_cfg_perworker_size_memory
            
        # Ограничение по количеству тасков на поток
        if self.celery_cfg_perworker_cnt_tasks > 0:
            self.app.conf.worker_max_tasks_per_child = self.celery_cfg_perworker_cnt_tasks
        
        # Подтвердим задачу после выполнения, чтоб была возможность перекинуть на другой worker     
        self.app.conf.task_acks_late = True
        
        # Максимальное число зарезервированных задач под 1 worker. По умолчанию 4
        self.app.conf.worker_prefetch_multiplier = self.celery_cfg_perworker_reserved_tasks
        
        # Игнорирование результатов вообще всех тасков
        # self.app.conf.task_ignore_result = True
        
    def apply_schedules(self):
        # Расписание очищалки для тех ключей, у которых истекло TTL (time-to-live)
        self.app.conf.beat_schedule = {
            'backend_cleanup': {
                'task': 'celery.backend_cleanup',
                'schedule': self.schedule_backend_cleanup, # value in seconds
            },
        }
        
        
    def get_variable(self,varname):
        return getattr(self,varname)