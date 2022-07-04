for g_id in range(1,50):
    with TaskGroup(group_id=f'(g_id)') as tg1:
        t1 = DummyOperator(task_id='task1')
        t2 = DummyOperator(task_id='task2')
        
        t1 >> t2