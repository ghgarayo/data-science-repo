import mysql.connector
import pandas as pd

config = {
    "user": "root",
    "password": "root",
    "host": "localhost",
    "database": "dw_inep",
    "port": "3306"
}

dados_curso_2020 = "X:/Faculdade/2023.2/Data Science/docs/MICRODADOS_CADASTRO_CURSOS_2020.CSV"
dados_ies_2020 = "X:/Faculdade/2023.2/Data Science/docs/MICRODADOS_CADASTRO_IES_2020.CSV"
dados_curso_2021 = "X:/Faculdade/2023.2/Data Science/docs/MICRODADOS_CADASTRO_CURSOS_2021.CSV"
dados_ies_2021 = "X:/Faculdade/2023.2/Data Science/docs/MICRODADOS_CADASTRO_IES_2021.CSV"

try:

    # Estabelece uma conexão com o banco de dados MySQL
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print('Conectado ao banco de dados')

    # Lê o arquivo CSV
    curso_2020 = pd.read_csv(dados_curso_2020, sep=';', encoding='ISO-8859-1', low_memory=False)
    curso_2020 = curso_2020.fillna('')
    curso_2021 = pd.read_csv(dados_curso_2021, sep=';', encoding='ISO-8859-1', low_memory=False)
    curso_2021 = curso_2021.fillna('')
    ies_2020 = pd.read_csv(dados_ies_2020, sep=';', encoding='ISO-8859-1', low_memory=False)
    ies_2020 = ies_2020[['CO_IES', 'NO_IES']]
    ies_2021 = pd.read_csv(dados_ies_2021, sep=';', encoding='ISO-8859-1', low_memory=False)
    ies_2021 = ies_2021[['CO_IES', 'NO_IES']]

    # ===== UF =====
    dados_uf = pd.DataFrame(curso_2020['NO_UF'].unique(), columns=['UF'])
    for i, r in dados_uf.iterrows():
        insert_statement = 'insert into dim_uf_2020 (tf_uf, uf) values (' \
                           + str(i) + ',\'' \
                           + str(r['UF']) + '\')'
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    dados_uf_2021 = pd.DataFrame(curso_2021['NO_UF'].unique(), columns=['UF'])
    for i, r in dados_uf_2021.iterrows():
        insert_statement = 'insert into dim_uf_2021 (tf_uf, uf) values (' \
                           + str(i) + ',\'' \
                           + str(r['UF']) + '\')'
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== MUNICIPIO =====
    dados_municipio = pd.DataFrame(curso_2020['NO_MUNICIPIO'].unique(), columns=['MUNICIPIO'])
    for i, r in dados_municipio.iterrows():
        municipio = r['MUNICIPIO'].replace("'", " ")
        insert_statement = f"INSERT INTO dim_municipio_2020 (tf_municipio, municipio) VALUES ({i} , '{municipio}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    dados_municipio_2021 = pd.DataFrame(curso_2021['NO_MUNICIPIO'].unique(), columns=['MUNICIPIO'])
    for i, r in dados_municipio_2021.iterrows():
        municipio = r['MUNICIPIO'].replace("'", " ")
        insert_statement = f"INSERT INTO dim_municipio_2021 (tf_municipio, municipio) VALUES ({i} , '{municipio}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== IES ======
    dados_ies_curso = pd.DataFrame(curso_2020['CO_IES'].unique(), columns=['IES'])
    for i, r in dados_ies_curso.iterrows():
        dados_ies_filtrado = ies_2020[ies_2020['CO_IES'] == r['IES']]
        dados_filrados = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
        insert_statement = f"INSERT INTO dim_ies_2020 (tf_ies, ies) values ({i} , '{dados_filrados}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    dados_ies_curso_2021 = pd.DataFrame(curso_2021['CO_IES'].unique(), columns=['IES'])
    for i, r in dados_ies_curso_2021.iterrows():
        dados_ies_filtrado = ies_2021[ies_2021['CO_IES'] == r['IES']]
        dados_filrados = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
        insert_statement = f"INSERT INTO dim_ies_2021 (tf_ies, ies) values ({i} , '{dados_filrados}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== MODALIDADE =====
    dados_modalidade = pd.DataFrame(curso_2020['TP_MODALIDADE_ENSINO'].unique(), columns=['MODALIDADE'])
    for i, r in dados_modalidade.iterrows():
        if r['MODALIDADE'] == 1:
            insert_statement = f"INSERT INTO dim_modalidade_2020 (tf_modalidade, modalidade) VALUES ('{r['MODALIDADE']}', 'Presencial')"
        elif r['MODALIDADE'] == 2:
            insert_statement = f"INSERT INTO dim_modalidade_2020 (tf_modalidade, modalidade) VALUES ('{r['MODALIDADE']}', 'EAD')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    dados_modalidade_2021 = pd.DataFrame(curso_2021['TP_MODALIDADE_ENSINO'].unique(), columns=['MODALIDADE'])
    for i, r in dados_modalidade_2021.iterrows():
        if r['MODALIDADE'] == 1:
            insert_statement = f"INSERT INTO dim_modalidade_2021 (tf_modalidade, modalidade) VALUES ('{r['MODALIDADE']}', 'Presencial')"
        elif r['MODALIDADE'] == 2:
            insert_statement = f"INSERT INTO dim_modalidade_2021 (tf_modalidade, modalidade) VALUES ('{r['MODALIDADE']}', 'EAD')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== CURSO =====
    dados_curso = pd.DataFrame(curso_2020['NO_CURSO'].unique(), columns=['CURSO'])
    for i, r in dados_curso.iterrows():
        insert_statement = f"INSERT INTO dim_curso_2020 (tf_curso, curso) VALUES ({i}, '{r['CURSO']}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    dados_curso_2021 = pd.DataFrame(curso_2021['NO_CURSO'].unique(), columns=['CURSO'])
    for i, r in dados_curso_2021.iterrows():
        insert_statement = f"INSERT INTO dim_curso_2021 (tf_curso, curso) VALUES ({i}, '{r['CURSO']}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== ANO =====
    dados_ano = pd.DataFrame(curso_2020['NU_ANO_CENSO'].unique(), columns=['ANO'])
    for i, r in dados_ano.iterrows():
        insert_statement = f"INSERT INTO dim_ano_2020 (tf_ano, ano) VALUES ({i}, {r['ANO']})"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    dados_ano_2021 = pd.DataFrame(curso_2021['NU_ANO_CENSO'].unique(), columns=['ANO'])
    for i, r in dados_ano_2021.iterrows():
        insert_statement = f"INSERT INTO dim_ano_2021 (tf_ano, ano) VALUES ({i + 1}, {r['ANO']})"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== TABELA FATO =====
    for i, r in curso_2020.iterrows():
        modalidade = 'Presencial' if r['TP_MODALIDADE_ENSINO'] == 1 else 'EAD'
        dados_ies_filtrado = ies_2020[ies_2020['CO_IES'] == r['CO_IES']]
        no_ies = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
        municipio = str(r['NO_MUNICIPIO']).replace("'", " ")

        subquery_matriculas = f"(SELECT {r['QT_INSCRITO_TOTAL']}) as matriculas"
        subquery_tf_ano = f"(SELECT tf_ano from dim_ano_2020 where ano = {r['NU_ANO_CENSO']}) AS tf_ano"
        subquery_tf_modalidade = f"(SELECT tf_modalidade from dim_modalidade_2020 where modalidade = '{modalidade}') AS tf_modalidade"
        subquery_tf_municipio = f"(SELECT tf_municipio from dim_municipio_2020 where municipio = '{municipio}') AS tf_municipio" \
            if r[
            'NO_MUNICIPIO'] else f"(SELECT Null from dim_municipio_2020 where municipio = '{r['NO_MUNICIPIO']}') AS tf_municipio"
        subquery_tf_uf = f"(SELECT tf_uf from dim_uf_2020 where uf = '{r['NO_UF']}') AS tf_uf" if r[
            'NO_UF'] else f"(SELECT Null from dim_uf_2020 where uf = '{r['NO_UF']}') AS tf_uf"
        subquery_tf_ies = f"(SELECT tf_ies from dim_ies_2020 where ies = '{no_ies}') AS tf_ies"
        subquery_tf_curso = f"(SELECT tf_curso from dim_curso_2020 where curso = '{r['NO_CURSO']}') AS tf_curso"

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
        conn.commit()

    for i, r in curso_2021.iterrows():
        modalidade = 'Presencial' if r['TP_MODALIDADE_ENSINO'] == 1 else 'EAD'
        dados_ies_filtrado = ies_2021[ies_2021['CO_IES'] == r['CO_IES']]
        no_ies = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
        municipio = str(r['NO_MUNICIPIO']).replace("'", " ")

        subquery_matriculas = f"(SELECT {r['QT_INSCRITO_TOTAL']}) as matriculas"
        subquery_tf_ano = f"(SELECT tf_ano from dim_ano_2021 where ano = {r['NU_ANO_CENSO']}) AS tf_ano"
        subquery_tf_modalidade = f"(SELECT tf_modalidade from dim_modalidade_2021 where modalidade = '{modalidade}') AS tf_modalidade"
        subquery_tf_municipio = f"(SELECT tf_municipio from dim_municipio_2021 where municipio = '{municipio}') AS tf_municipio" \
            if r['NO_MUNICIPIO'] else f"(SELECT Null from dim_municipio_2021 where municipio = '{r['NO_MUNICIPIO']}') AS tf_municipio"
        subquery_tf_uf = f"(SELECT tf_uf from dim_uf_2021 where uf = '{r['NO_UF']}') AS tf_uf" if r[
            'NO_UF'] else f"(SELECT Null from dim_uf_2021 where uf = '{r['NO_UF']}') AS tf_uf"
        subquery_tf_ies = f"(SELECT tf_ies from dim_ies_2021 where ies = '{no_ies}') AS tf_ies"
        subquery_tf_curso = f"(SELECT tf_curso from dim_curso_2021 where curso = '{r['NO_CURSO']}') AS tf_curso"

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
        conn.commit()


        print(f"Row {i + 1} inserted.")


except mysql.connector.Error as e:
    print('Erro ao conectar ao banco de dados:', e)
except FileNotFoundError:
    print('Arquivo CSV não encontrado.')
except pd.errors.EmptyDataError:
    print('O arquivo CSV está vazio.')
except Exception as e:
    print('Ocorreu um erro inesperado:', e)
# finally:
#     if conn is not None and conn.is_connected():
#         conn.close()
#         print('Conexão fechada.')