import pandas as pd
import psycopg2 as ps 
from psycopg2 import sql
import datetime
import psycopg2 as ps
from os import getenv
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


# Parâmetros de conexão com o banco de dados
conn_params_source = {
    "dbname": getenv("DBNAME_INSERCAO"),
    "user": getenv("USER_INSERCAO"),
    "host": getenv("HOST_INSERCAO"),
    "password": getenv("PASSWORD_INSERCAO"),
    "port": getenv("PORT_INSERCAO")
}

user_table = "app_user"
streak_table = "streak"

try:
    # Conectar ao banco de dados
    conn = ps.connect(**conn_params_source)
    cur = conn.cursor()

    # Carregar os dados da tabela de usuários
    day_last_log = pd.read_sql(f"SELECT id, updated_at FROM {user_table}", conn)
    for i in range(len(day_last_log)):
        last_log_date = day_last_log['updated_at'][i]
        user_id = day_last_log['id'][i]

        # Verifica se o usuário não fez login nas últimas 24 horas
        if last_log_date < datetime.datetime.now() - datetime.timedelta(days=1):
            update_query = f"""
            UPDATE {streak_table} 
            SET final_date = %s 
            WHERE fk_app_user_id = %s 
            AND final_date IS NULL
            """
            # Executa a query de atualização
            cur.execute(update_query, (last_log_date, user_id))

    # Commit das alterações
    conn.commit()

except Exception as e:
    print(f"Erro ao carregar dados da tabela {user_table}: {e}")

finally:
    # Fechar a conexão com o banco de dados
    if conn:
        conn.close()
