import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv


def cria_conexao_db(config):
    try:
        conn = mysql.connector.connect(**config)
        print('Conectado ao banco de dados')
        return conn
    except mysql.connector.Error as e:
        print('Erro ao conectar ao banco de dados:', e)
        raise e


def insere_registro_unico(cursor, tabela, campo_unico, tf, valor):
    # Verifica se um registro com o campo_unico e valor fornecidos existem na tabela.
    select_statement = f"SELECT COUNT(*) FROM {tabela} WHERE {campo_unico} = %s"
    cursor.execute(select_statement, (valor,))
    result = cursor.fetchone()

    if result[0] == 0:
        # Registro não existe, insere.
        insert_statement = f"INSERT INTO {tabela} (tf_{campo_unico},{campo_unico}) VALUES ({tf}, \'{valor}\')"
        print(insert_statement)
        cursor.execute(insert_statement)
        return True
    else:
        print(f"Registro com {campo_unico} = '{valor}' já existe na tabela {tabela}.")
        return False


def processa_csv_e_insere_dados(conn, cursor, dados_curso, dados_ies, ano):
    try:
        dados = pd.read_csv(dados_curso, sep=';', encoding='ISO-8859-1', low_memory=False)
        dados = dados.fillna('')
        dados_ies = pd.read_csv(dados_ies, sep=';', encoding='ISO-8859-1', low_memory=False)
        dados_ies = dados_ies[['CO_IES', 'NO_IES']]

        # ===== UF =====
        dados_uf = pd.DataFrame(dados['NO_UF'].unique(), columns=['UF'])
        for i, r in dados_uf.iterrows():
            uf = r['UF']
            insere_registro_unico(cursor, 'dim_uf', 'uf', i, uf)

        # ===== MUNICIPIO =====
        dados_municipio = pd.DataFrame(dados['NO_MUNICIPIO'].unique(), columns=['MUNICIPIO'])
        for i, r in dados_municipio.iterrows():
            municipio = r['MUNICIPIO'].replace("'", " ")
            insere_registro_unico(cursor, 'dim_municipio', 'municipio', i, municipio)

        # ===== IES ======
        dados_ies_curso = pd.DataFrame(dados['CO_IES'].unique(), columns=['IES'])
        for i, r in dados_ies_curso.iterrows():
            dados_ies_filtrado = dados_ies[dados_ies['CO_IES'] == r['IES']]
            dados_filrados = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
            insere_registro_unico(cursor, 'dim_ies', 'ies', i, dados_filrados)

        # ===== MODALIDADE =====
        dados_modalidade = pd.DataFrame(dados['TP_MODALIDADE_ENSINO'].unique(), columns=['MODALIDADE'])
        for i, r in dados_modalidade.iterrows():
            if r['MODALIDADE'] == 1:
                modalidade = 'Presencial'
            elif r['MODALIDADE'] == 2:
                modalidade = 'EAD'
            insere_registro_unico(cursor, 'dim_modalidade', 'modalidade', r['MODALIDADE'], modalidade)

        # ===== CURSO =====
        dados_curso = pd.DataFrame(dados['NO_CURSO'].unique(), columns=['CURSO'])
        for i, r in dados_curso.iterrows():
            curso = r['CURSO'].replace("'", " ")
            insere_registro_unico(cursor, 'dim_curso', 'curso', i, curso)

        # ===== ANO =====
        insere_registro_unico(cursor, 'dim_ano', 'ano', ano, ano)

        # ===== TABELA FATO =====
        for i, r in dados.iterrows():
            modalidade = 'Presencial' if r['TP_MODALIDADE_ENSINO'] == 1 else 'EAD'
            dados_ies_filtrado = dados_ies[dados_ies['CO_IES'] == r['CO_IES']]
            no_ies = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
            municipio = str(r['NO_MUNICIPIO']).replace("'", " ")

            subquery_matriculas = f"(SELECT {r['QT_INSCRITO_TOTAL']}) as matriculas"
            subquery_tf_ano = f"(SELECT tf_ano from dim_ano where ano = {r['NU_ANO_CENSO']}) AS tf_ano"
            subquery_tf_modalidade = f"(SELECT tf_modalidade from dim_modalidade where modalidade = '{modalidade}') AS tf_modalidade"
            subquery_tf_municipio = \
                f"(SELECT tf_municipio from dim_municipio where municipio = '{municipio}') AS tf_municipio" if r['NO_MUNICIPIO'] \
                else f"(SELECT Null from dim_municipio where municipio = '{r['NO_MUNICIPIO']}') AS tf_municipio"
            subquery_tf_uf = f"(SELECT tf_uf from dim_uf where uf = '{r['NO_UF']}') AS tf_uf" if r['NO_UF'] \
                else f"(SELECT Null from dim_uf where uf = '{r['NO_UF']}') AS tf_uf"
            subquery_tf_ies = f"(SELECT tf_ies from dim_ies where ies = '{no_ies}') AS tf_ies"
            subquery_tf_curso = f"(SELECT tf_curso from dim_curso where curso = '{r['NO_CURSO']}') AS tf_curso"

            insert_statement = f"""INSERT INTO fact_matriculas (matriculas, tf_curso, tf_ano, tf_modalidade, tf_municipio, tf_uf, tf_ies)
                                    SELECT DISTINCT * from
                                    {subquery_matriculas},
                                    {subquery_tf_curso},
                                    {subquery_tf_ano},
                                    {subquery_tf_modalidade},
                                    {subquery_tf_municipio},
                                    {subquery_tf_uf},
                                    {subquery_tf_ies};
                                """

            print(insert_statement)
            cursor.execute(insert_statement)

    except FileNotFoundError:
        print('Arquivo CSV não encontrado.')

    except pd.errors.EmptyDataError:
        print('O arquivo CSV está vazio.')


def main():
    load_dotenv()

    config = {
        "user": os.environ.get('DATABASE_USER'),
        "password": os.environ.get('DATABASE_PASSWORD'),
        "host": os.environ.get('DATABASE_HOST'),
        "database": os.environ.get('DATABASE_NAME'),
        "port": os.environ.get('DATABASE_PORT')
    }

    dados_curso_2020 = os.environ.get('DADOS_CURSO_2020')
    dados_ies_2020 = os.environ.get('DADOS_IES_2020')
    dados_curso_2021 = os.environ.get('DADOS_CURSO_2021')
    dados_ies_2021 = os.environ.get('DADOS_IES_2021')

    try:

        # Estabelece uma conexão com o banco de dados MySQL
        conn = cria_conexao_db(config)
        cursor = conn.cursor()

        processa_csv_e_insere_dados(conn, cursor, dados_curso_2020, dados_ies_2020, 2020)
        processa_csv_e_insere_dados(conn, cursor, dados_curso_2021, dados_ies_2021, 2021)

        cursor.close()
        conn.commit()
        conn.close()
        print('Conexão com o banco de dados finalizada!')

    except mysql.connector.Error as e:
        print('Erro ao conectar ao banco de dados:', e)
    except Exception as e:
        print('Ocorreu um erro inesperado:', e)


if __name__ == "__main__":
    main()