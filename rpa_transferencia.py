import time
import pandas as pd
import psycopg2 as ps 
import psycopg2
from psycopg2 import sql
import numpy as np
from os import getenv
from dotenv import load_dotenv

load_dotenv()

#Funções

def carregar_dados(tabela, conn_source):
    try:
        df = pd.read_sql(f"SELECT * FROM {tabela}", conn_source)
        print(f"Dados carregados com sucesso da tabela: {tabela}.")
        return df
    except Exception as e:
        print(f"Erro ao carregar dados da tabela: {tabela}: {e}")
        return None

def sync_table(conn_dest, conn_source, df_all, df_changes, table_name_dest, table_name_source):
    cur_dest = conn_dest.cursor()
    cur_source = conn_source.cursor()

    # Query para verificar se um registro existe
    check_query = sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {} = %s)").format(
        sql.Identifier(table_name_dest),
        sql.Identifier('id')  # Usando a coluna ID da tabela de destino
    )

    # Query de inserção
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name_dest),
        sql.SQL(', ').join(map(sql.Identifier, df_all.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_all.columns))
    )

    # Query de atualização
    update_query = sql.SQL("UPDATE {} SET ({}) = ({}) WHERE {} = %s").format(
        sql.Identifier(table_name_dest),
        sql.SQL(', ').join(map(sql.Identifier, df_all.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_all.columns)),
        sql.Identifier('id')
    )

    # Query para deletar registros no destino
    delete_query = sql.SQL("DELETE FROM {} WHERE {} = %s").format(
        sql.Identifier(table_name_dest), 
        sql.Identifier('id')
    )

    # Sincronização dos dados
    for idx, row in df_changes.iterrows():
        try:
            record = df_all.iloc[idx]

            record_values = [record[col] if not isinstance(record[col], (np.integer, np.floating)) else int(record[col]) for col in df_all.columns]

            cur_dest.execute(check_query, (int(record['id']),))  # Converte para int
            record_exists = cur_dest.fetchone()[0]

            if row['is_deleted']:
                if record_exists:
                    cur_dest.execute(delete_query, (int(record['id']),))  # Converte para int
                    print(f"Registro com ID {record['id']} deletado da tabela {table_name_dest}.")
            elif row['is_updated']:
                if record_exists:
                    cur_dest.execute(update_query, (*record_values, int(record['id'])))  # Usando valores convertidos
                    print(f"Registro com ID {record['id']} atualizado na tabela {table_name_dest}.")
                else:
                    cur_dest.execute(insert_query, tuple(record_values))  # Usando valores convertidos
                    print(f"Registro com ID {record['id']} criado (update) na tabela {table_name_dest}.")
            else:  
                if not record_exists:
                    cur_dest.execute(insert_query, tuple(record_values))  # Usando valores convertidos
                    print(f"Registro com ID {record['id']} criado (insert) na tabela {table_name_dest}.")

        except Exception as e:
            print(f"Erro ao processar registro com ID {record['id']}: {e}")
            conn_dest.rollback()  
            return 

    conn_dest.commit()
    
    delete_query_source = sql.SQL("DELETE FROM {} WHERE is_deleted = TRUE").format(
        sql.Identifier(table_name_source)
    )
    cur_source.execute(delete_query_source)
    print(f"Registros deletados da tabela {table_name_source} onde is_deleted é True.")

    conn_source.commit()
    cur_dest.close()
    cur_source.close()


#Banco do primeiro ano
dbname_extracao = getenv("DBNAME_EXTRACAO")
user_extracao = getenv("USER_EXTRACAO")
host_extracao = getenv("HOST_EXTRACAO")
password_extracao = getenv("PASSWORD_EXTRACAO")
port_extracao = getenv("PORT_EXTRACAO")
    
conn_params_source = ps.connect(
    dbname=dbname_extracao,
    user=user_extracao,
    host=host_extracao,
    password=password_extracao,
    port=port_extracao
)
time.sleep(1)

#Banco do segundo ano
dbname_insercao = getenv("DBNAME_INSERCAO")
user_insercao = getenv("USER_INSERCAO")
host_insercao = getenv("HOST_INSERCAO")
password_insercao = getenv("PASSWORD_INSERCAO")
port_insercao = getenv("PORT_INSERCAO")

conn_params_dest = ps.connect(
    dbname= dbname_insercao,
    user= user_insercao,
    host= host_insercao,
    password= password_insercao,
    port= port_insercao
)

time.sleep(1)

tabela_origem_1 = "tema"
tabela_origem_2 = "plano"
tabela_origem_3 = "powerup"
tabela_origem_4 = "role_usuario"

tabela_dest_1 = 'theme'
tabela_dest_2 = 'plan'
tabela_dest_3 = 'powerup'
tabela_dest_4 = 'user_role'

conn_source = conn_params_source
conn_dest = conn_params_dest
time.sleep(1)

# Chamando a função para cada tabela
df_tabela1 = carregar_dados(tabela_origem_1, conn_source)
df_tabela2 = carregar_dados(tabela_origem_2, conn_source)
df_tabela3 = carregar_dados(tabela_origem_3, conn_source)
df_tabela4 = carregar_dados(tabela_origem_4, conn_source)

#Tratamento de tablas
df_tabela1.columns = ['id' if col == 'id_tema' else col for col in df_tabela1.columns]
df_tabela2.columns = ['id' if col == 'id_plano' else col for col in df_tabela2.columns]
df_tabela3.columns = ['id' if col == 'id_powerup' else col for col in df_tabela3.columns]
df_tabela4.columns = ['id' if col == 'id_role_usuario' else col for col in df_tabela4.columns]
df_tabela1 = df_tabela1.drop('ativo_atual', axis=1)
df_tabela1.columns = ['name' if col == 'nome' else col for col in df_tabela1.columns]
df_tabela1.columns = ['price' if col == 'preco_moedas' else col for col in df_tabela1.columns]
df_tabela1.columns = ['store_picture_url' if col == 'imagem_loja_url' else col for col in df_tabela1.columns]
df_up_1 = pd.DataFrame({
    'is_updated': df_tabela1['is_updated'],
    'is_deleted': df_tabela1['is_deleted']
})
df_tabela1 = df_tabela1.drop('is_deleted', axis=1)
df_tabela1 = df_tabela1.drop('is_updated', axis=1)

time.sleep(1)

#Chamada de funções
sync_table(conn_dest, conn_source, df_tabela1, df_up_1, tabela_dest_1, tabela_origem_1)
# sync_table(conn_dest, df_tabela2, tabela_dest_2)
# sync_table(conn_dest, df_tabela3, tabela_dest_3)
# sync_table(conn_dest, df_tabela4, tabela_dest_4)

# Fechando as conexões
conn_dest.close()
conn_source.close()
