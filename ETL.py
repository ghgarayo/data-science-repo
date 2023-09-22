import mysql.connector
import pandas as pd

config = {
    "user": "root",
    "password": "root",
    "host": "localhost",
    "database": "dw_inep",
    "port": "3306"
}

dados_curso_2020 = "E:/Faculdade/02.23/Data_Science/docs/MICRODADOS_CADASTRO_CURSOS_2020.CSV"
dados_ies_2020 = "E:/Faculdade/02.23/Data_Science/docs/MICRODADOS_CADASTRO_IES_2020.CSV"
dados_curso_2021 = "E:/Faculdade/02.23/Data_Science/docs/MICRODADOS_CADASTRO_CURSOS_2021.CSV"
dados_ies_2021 = "E:/Faculdade/02.23/Data_Science/docs/MICRODADOS_CADASTRO_IES_2021.CSV"

conn = None

try:

    # Estabelece uma conexão com o banco de dados MySQL
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print('Conectado ao banco de dados')

    # Lê o arquivo CSV
    dados = pd.read_csv(dados_curso_2020, sep=';', encoding='ISO-8859-1', low_memory=False)
    dados = dados.fillna('')
    dados_ies = pd.read_csv(dados_ies_2020, sep=';', encoding='ISO-8859-1', low_memory=False)
    dados_ies = dados_ies[['CO_IES', 'NO_IES']]

    # ============== 2020 =================

    # ===== UF =====

    dados_uf = pd.DataFrame(dados['NO_UF'].unique(), columns=['UF'])
    for i, r in dados_uf.iterrows():
        uf = r['UF']
        insert_statement = f'INSERT INTO dim_uf (tf_uf, uf) VALUES ({i}, \'{uf}\')'
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== MUNICIPIO =====

    dados_municipio = pd.DataFrame(dados['NO_MUNICIPIO'].unique(), columns=['MUNICIPIO'])
    for i, r in dados_municipio.iterrows():
        municipio = r['MUNICIPIO'].replace("'", " ")
        insert_statement = f"INSERT INTO dim_municipio (tf_municipio, municipio) VALUES ({i} , '{municipio}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== IES ======

    dados_ies_curso = pd.DataFrame(dados['CO_IES'].unique(), columns=['IES'])
    for i, r in dados_ies_curso.iterrows():
        dados_ies_filtrado = dados_ies[dados_ies['CO_IES'] == r['IES']]
        dados_filrados = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
        insert_statement = f"INSERT INTO dim_ies (tf_ies, ies) values ({i} , '{dados_filrados}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== MODALIDADE =====

    dados_modalidade = pd.DataFrame(dados['TP_MODALIDADE_ENSINO'].unique(), columns=['MODALIDADE'])
    for i, r in dados_modalidade.iterrows():
        if r['MODALIDADE'] == 1:
            insert_statement = f"INSERT INTO dim_modalidade (tf_modalidade, modalidade) VALUES ('{r['MODALIDADE']}', 'Presencial')"
        elif r['MODALIDADE'] == 2:
            insert_statement = f"INSERT INTO dim_modalidade (tf_modalidade, modalidade) VALUES ('{r['MODALIDADE']}', 'EAD')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== CURSO =====

    dados_curso = pd.DataFrame(dados['NO_CURSO'].unique(), columns=['CURSO'])
    for i, r in dados_curso.iterrows():
        insert_statement = f"INSERT INTO dim_curso (tf_curso, curso) VALUES ({i}, '{r['CURSO']}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== ANO =====

    dados_ano = pd.DataFrame(dados['NU_ANO_CENSO'].unique(), columns=['ANO'])
    for i, r in dados_ano.iterrows():
        insert_statement = f"INSERT INTO dim_ano (tf_ano, ano) VALUES ({i}, {r['ANO']})"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    # ===== TABELA FATO =====

    for i, r in dados.iterrows():
        modalidade = 'Presencial' if r['TP_MODALIDADE_ENSINO'] == 1 else 'EAD'
        dados_ies_filtrado = dados_ies[dados_ies['CO_IES'] == r['CO_IES']]
        no_ies = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
        municipio = str(r['NO_MUNICIPIO']).replace("'", " ")

        subquery_matriculas = f"(SELECT {r['QT_INSCRITO_TOTAL']}) as matriculas"
        subquery_tf_ano = f"(SELECT tf_ano from dim_ano where ano = {r['NU_ANO_CENSO']}) AS tf_ano"
        subquery_tf_modalidade = f"(SELECT tf_modalidade from dim_modalidade where modalidade = '{modalidade}') AS tf_modalidade"
        subquery_tf_municipio = f"(SELECT tf_municipio from dim_municipio where municipio = '{municipio}') AS tf_municipio" \
            if r[
            'NO_MUNICIPIO'] else f"(SELECT Null from dim_municipio where municipio = '{r['NO_MUNICIPIO']}') AS tf_municipio"
        subquery_tf_uf = f"(SELECT tf_uf from dim_uf where uf = '{r['NO_UF']}') AS tf_uf" if r[
            'NO_UF'] else f"(SELECT Null from dim_uf where uf = '{r['NO_UF']}') AS tf_uf"
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
        conn.commit()

    # ============== 2021 =================

    dados = pd.read_csv(dados_curso_2021, sep=';', encoding='ISO-8859-1', low_memory=False)
    dados = dados.fillna('')
    dados_ies = pd.read_csv(dados_ies_2021, sep=';', encoding='ISO-8859-1', low_memory=False)
    dados_ies = dados_ies[['CO_IES', 'NO_IES']]

    # ===== UF =====

    dados_uf = pd.DataFrame(dados['NO_UF'].unique(), columns=['UF'])
    for i, r in dados_uf.iterrows():
        uf = r['UF']
        select_statement = f'SELECT COUNT(*) FROM dim_uf WHERE uf = \'{uf}\''
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            insert_statement = f'INSERT INTO dim_uf (tf_uf, uf) VALUES ({i}, \'{uf}\')'
            print(insert_statement)
            cursor.execute(insert_statement)
            conn.commit()
        else:
            print(f'UF \'{uf}\' já existe na tabela dim_uf.')

    # ===== MUNICIPIO =====

    dados_municipio = pd.DataFrame(dados['NO_MUNICIPIO'].unique(), columns=['MUNICIPIO'])
    for i, r in dados_municipio.iterrows():
        municipio = r['MUNICIPIO'].replace("'", " ")
        select_statement = f'SELECT COUNT(*) FROM dim_municipio WHERE municipio = \'{municipio}\''
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            insert_statement = f"INSERT INTO dim_municipio (tf_municipio, municipio) VALUES ({i} , '{municipio}')"
            print(insert_statement)
            cursor.execute(insert_statement)
            conn.commit()
        else:
            print(f'Municipio \'{municipio}\' já existe na tabela dim_municipio.')

    # ===== IES ======

    dados_ies_curso = pd.DataFrame(dados['CO_IES'].unique(), columns=['IES'])
    for i, r in dados_ies_curso.iterrows():
        dados_ies_filtrado = dados_ies[dados_ies['CO_IES'] == r['IES']]
        dados_filrados = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
        select_statement = f'SELECT COUNT(*) FROM dim_ies WHERE ies = \'{dados_filrados}\''
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            insert_statement = f"INSERT INTO dim_ies (tf_ies, ies) values ({i} , '{dados_filrados}')"
            print(insert_statement)
            cursor.execute(insert_statement)
            conn.commit()
        else:
            print(f'Instituição de ensino \'{dados_filrados}\' já existe na tabela dim_ies.')

    # ===== MODALIDADE =====

    dados_modalidade = pd.DataFrame(dados['TP_MODALIDADE_ENSINO'].unique(), columns=['MODALIDADE'])
    for i, r in dados_modalidade.iterrows():
        modalidade = r['MODALIDADE']

        if modalidade == 1:
            select_statement = f"SELECT COUNT(*) FROM dim_modalidade WHERE modalidade = 'Presencial'"
            cursor.execute(select_statement)
            result = cursor.fetchone()

            if result[0] == 0:
                insert_statement = f"INSERT INTO dim_modalidade (tf_modalidade, modalidade) VALUES ('{modalidade}', 'Presencial')"
            else:
                print(f'Modalidade Presencial já existe na tabela dim_modalidade.')

        elif modalidade == 2:
            select_statement = f"SELECT COUNT(*) FROM dim_modalidade WHERE modalidade = 'EAD'"
            cursor.execute(select_statement)
            result = cursor.fetchone()

            if result[0] == 0:
                insert_statement = f"INSERT INTO dim_modalidade (tf_modalidade, modalidade) VALUES (\'{modalidade}\', 'EAD')"
            else:
                print(f'Modalidade EAD já existe na tabela dim_modalidade.')

        cursor.execute(insert_statement)
        conn.commit()

    # ===== CURSO =====

    dados_curso = pd.DataFrame(dados['NO_CURSO'].unique(), columns=['CURSO'])
    for i, r in dados_curso.iterrows():
        curso = r['CURSO'].replace("'", " ")
        select_statement = f'SELECT COUNT(*) FROM dim_curso WHERE curso = \'{curso}\''
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            insert_statement = f"INSERT INTO dim_curso (tf_curso, curso) VALUES ({i}, '{curso}')"
            print(insert_statement)
            cursor.execute(insert_statement)
            conn.commit()

        else:
            print(f'Curso \'{curso}\' já existe na tabela dim_curso.')

    # ===== ANO =====

    dados_ano = pd.DataFrame(dados['NU_ANO_CENSO'].unique(), columns=['ANO'])
    for i, r in dados_ano.iterrows():
        ano = r['ANO']
        select_statement = f'SELECT COUNT(*) FROM dim_ano WHERE ano = {ano}'
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            insert_statement = f"INSERT INTO dim_ano (tf_ano, ano) VALUES ({i + 1}, {ano})"
            print(insert_statement)
            cursor.execute(insert_statement)
            conn.commit()

        else:
            print(f'Ano \'{ano}\' já existe na tabela dim_ano.')

    # ===== TABELA FATO =====

    for i, r in dados.iterrows():
        modalidade = 'Presencial' if r['TP_MODALIDADE_ENSINO'] == 1 else 'EAD'
        dados_ies_filtrado = dados_ies[dados_ies['CO_IES'] == r['CO_IES']]
        no_ies = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
        municipio = str(r['NO_MUNICIPIO']).replace("'", " ")

        subquery_matriculas = f"(SELECT {r['QT_INSCRITO_TOTAL']}) as matriculas"
        subquery_tf_ano = f"(SELECT tf_ano from dim_ano where ano = {r['NU_ANO_CENSO']}) AS tf_ano"
        subquery_tf_modalidade = f"(SELECT tf_modalidade from dim_modalidade where modalidade = '{modalidade}') AS tf_modalidade"
        subquery_tf_municipio = f"(SELECT tf_municipio from dim_municipio where municipio = '{municipio}') AS tf_municipio" \
            if r['NO_MUNICIPIO'] else f"(SELECT Null from dim_municipio where municipio = '{r['NO_MUNICIPIO']}') AS tf_municipio"
        subquery_tf_uf = f"(SELECT tf_uf from dim_uf where uf = '{r['NO_UF']}') AS tf_uf" if r[
            'NO_UF'] else f"(SELECT Null from dim_uf where uf = '{r['NO_UF']}') AS tf_uf"
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
        conn.commit()

except mysql.connector.Error as e:
    print('Erro ao conectar ao banco de dados:', e)
except FileNotFoundError:
    print('Arquivo CSV não encontrado.')
except pd.errors.EmptyDataError:
    print('O arquivo CSV está vazio.')
except Exception as e:
    print('Ocorreu um erro inesperado:', e)
finally:
    if conn is not None and conn.is_connected():
        conn.close()
        print('Conexão fechada.')
