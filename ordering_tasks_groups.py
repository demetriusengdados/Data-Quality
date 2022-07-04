groups = []
for g_id in range(1,400):
    tg_id = f'group{g_id}'
    with TaskGroup(group_id=tg_id) as tg1:
        t1 = DummyOperator(task_id='task1')
        t2 = DummyOperator(task_id='task2')

        t1 >> t2

        if tg_id == 'group1':
            t3 = DummyOperator(task_id='task3')
            t1 >> t3
                
        groups.append(tg1)

[groups[0] , groups[1]] >> groups[2]