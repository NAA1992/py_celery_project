import sys

from celery import current_app, shared_task

@shared_task(bind=True)
def kill_celery(self):
    print('Поступила команда на убийство всех процессов')
    cntrl_celery = current_app.control.inspect()
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
            current_app.control.revoke(num_act.get('id'), terminate=True, signal='SIGKILL')
    print('Убийство процессов произошло. Теперь идет команда на Warm Termination')
    # app.control.revoke(self.request.id) # prevent this task from being executed again
    current_app.control.shutdown(destination=[self.request.hostname])
    print('Полный процесс убийства и теплого завершения закончен. Выходим')
    sys.exit(1)

"""
@shared_task(bind=True)
def chain_csv(self, source_file):

    for i in range(3):
        task_id_uuid = uuid()
        send_api_csv_file.apply_async(kwargs={
                                        'source_file': source_file
                                        , 'table_name': 'mo_license'
                                        }, add_to_parent =False, task_id = task_id_uuid, countdown=3)
        res_tasks_async = AsyncResult(task_id_uuid)
        while res_tasks_async.ready() == False:
            res_tasks_async = AsyncResult(task_id_uuid)


    for i in range(2):
        send_api_csv_file.apply_async(kwargs={
                                'source_file': source_file
                                , 'table_name': 'mo_license'
                                }, countdown=1)
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
"""
