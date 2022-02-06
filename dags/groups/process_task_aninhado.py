from airflow.decorators import task
from airflow.utils.task_group import TaskGroup


@task.python
def process_a(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_b(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_c(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def check_A():
    print("Check A done!")

@task.python
def check_B():
    print("Check B done!")

@task.python
def check_C():
    print("Check C done!")

def process_tasks_aninhadas(partner_settings):
    with TaskGroup(group_id="process_tasks") as process_tasks:
        with TaskGroup(group_id="check_group") as check_group:
            check_A()
            check_B()
            check_C()
        process_a(partner_settings['partner_name'], partner_settings['partner_path']) >> check_group
        process_b(partner_settings['partner_name'], partner_settings['partner_path']) >> check_group
        process_c(partner_settings['partner_name'], partner_settings['partner_path']) >> check_group
    return process_tasks_aninhadas