groups = []
for g_id in range(1,300):
    with TaskGroup(group_id=f'group{g_id}') as tg1:
        t1 = DummyOperator(task_id='task1')
        t2 = DummyOperator(task_id='task2')

        sub_groups = []
        for s_id in range(1,3):
            with TaskGroup(group_id=f'sub_group{s_id}') as tg2:
                st1 = DummyOperator(task_id='task1')
                st2 = DummyOperator(task_id='task2')

                st1 >> st2
                sub_groups.append(tg2)

        t1 >> sub_groups >> t2
        groups.append(tg1)

groups[0] >> groups[1]