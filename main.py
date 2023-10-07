import cassandra

# Conectando ao cluster Cassandra
cluster = cassandra.Cluster()
session = cluster.connect()

# Cria o keyspace e a tabela
session.execute("""
CREATE KEYSPACE tasks
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE tasks (
    id uuid PRIMARY KEY,
    description text
);
""")

def add_task():
    # Obtendo um novo ID único para a tarefa
    task_id = uuid.uuid4()

    # Obtendo a descrição da tarefa do usuário
    task_description = input("Digite a descrição da tarefa: ")

    # Armazenando a tarefa no Cassandra
    session.execute("""
    INSERT INTO tasks (id, description)
    VALUES (%s, %s);
    """, (task_id, task_description))
    print("Tarefa adicionada com sucesso.")

def list_tasks():
    # Obtendo todas as tarefas
    tasks = session.execute("SELECT * FROM tasks;")

    # Exibindo as tarefas
    for task in tasks:
        task_id = task[0]
        task_description = task[1]
        print(f"Tarefa ID: {task_id}, Descrição: {task_description}")

def remove_task():
    # Obtendo o ID da tarefa que o usuário deseja remover
    task_id = input("Digite o ID da tarefa a ser removida: ")

    # Removendo a tarefa do Cassandra
    session.execute("DELETE FROM tasks WHERE id = %s;", (task_id,))
    print("Tarefa removida com sucesso.")

# Menu de opções
while True:
    print("Opções:")
    print("1. Adicionar Tarefa")
    print("2. Listar Tarefas")
    print("3. Remover Tarefa")
    print("0. Sair")

    choice = input("Escolha uma opção: ")

    if choice == '1':
        add_task()
    elif choice == '2':
        list_tasks()
    elif choice == '3':
        remove_task()
    elif choice == '0':
        print("Encerrando o programa.")
        break
    else:
        print("Opção inválida. Escolha uma opção válida.")
