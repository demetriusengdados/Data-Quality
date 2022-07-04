t0 = DummyOperator(task_id='start')

#start no grupo de tarefas

with TaskGroup(group_id='group1') as tg1:
    t1 = DummyOperator(task_id='tarefa1')
    t2 = DummyOperator(task_id='tarefa2')
    
    t1 >> t2
    
    #Fim da definição do grupo de tarefas
    
t3 = DummyOperator(task_id='end')

#setando os grupos de tasks para as dependencias

t0 >> tg1 >> t3 

"""
Na interface do ar, os grupos de tarefas parecem tarefas com sombreamento azul. Quando expandimos clicando nele, 
vemos círculos azuis onde as dependências do Grupo de Tarefas foram aplicadas às tarefas agrupadas. 
A tarefa(s) imediatamente à direita do primeiro círculo azul () obter as dependências a montante do grupo e as tarefas(s) 
imediatamente à esquerda () do último círculo azul obter as dependências a jusante do grupo.group1t1t2
"""