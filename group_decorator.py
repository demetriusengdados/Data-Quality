@task_group(group_id='tasks')

def my_independent_tasks():
    task_a()
    task_b()
    task_c()
    
@task_group(group_id='tasks')
def my_dependent_tasks():
    return task_a(task_b(task_c()))

