# PROJETO GLOGAL SOLUTION: POSEIDON DEVELOPERS
# LUIS HENRIQUE 552692 / LUCCAS DOS ANJOS 552890 / IGOR GABRIEL 553544


import json
import pandas as pd
import oracledb
from sqlalchemy import create_engine, types
import os

# Validação de credenciais
def validar_credenciais(user, password, dsn):
    try:
        connection = oracledb.connect(user=user, password=password, dsn=dsn)
        connection.close()
        return True
    except oracledb.Error:
        return False

# Salva as credenciais no credentials.txt como a conexão em um banco de dados Oracle, usando o dsn.
# Iriamos implementar a biblioteca do keyring para armazenamento seguro, mas optamos por deixar mais visivel "Simulação de Login"
def escrever_credenciais():
    print("BEM VINDO!\n")
    while True:
        user = input("Digite o usuário: ")
        password = input("Digite a senha: ")
        dsn = "oracle.fiap.com.br:1521/orcl"

        if validar_credenciais(user, password, dsn):
            credenciais = {
                "user": user,
                "password": password,
                "dsn": dsn
            }
            with open('credentials.txt', 'w') as file:
                json.dump(credenciais, file)
            print("ACESSO PERMITIDO\n")
            break
        else:
            print("Acesso negado!!")
            print("Tente novamente.\n")

# Função para limpar as credenciais removendo o arquivo 'credentials.txt'
def limpar_credenciais():
    try:
        os.remove('credentials.txt')
        print("Credenciais removidas com sucesso.\n")
    except FileNotFoundError:
        print("Arquivo de credenciais não encontrado.\n")

# Cria a conexão com o banco de dados
# Abre o arquivo para ler as credenciais        
def conectar_banco_de_dados(secret_file):
    with open(secret_file) as file:
        secret = json.load(file)
    
    usuario = secret['user']
    senha = secret['password']
    oracle = secret['dsn']

    # Retorna a conexão e a engine/conexão.
    connection = oracledb.connect(user=usuario, password=senha, dsn=oracle)
    engine = create_engine('oracle+oracledb://', creator=lambda: connection)
    
    return connection, engine

# Função para inserir os dados do CSV no banco de dados, lendo o arquivo e carregando os dados no DataFrame
def inserir_dados_csv_no_banco(csv_file, table_name, engine):
    data = pd.read_csv(csv_file)
    
    # Mapeamento de tipos de dados do pandas, garante que as colunas do DataFrame seja corretamente mapeadas
    dtype_mapping = {
        'float64': types.Numeric(precision=38, scale=10),
        'int64': types.Integer,
        'object': types.String(255)
    }
    
    # Gera o dicionario que foi implementado na coluna
    dtype = {col: dtype_mapping[str(data[col].dtype)] for col in data.columns}
    
    data.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype)

# Retorna o nome das colunas da tabela
def nomes_colunas(table_name, engine):
    query = f"SELECT * FROM {table_name} WHERE ROWNUM = 1"
    data = pd.read_sql(query, engine)
    return data.columns.tolist()

# Obtem o retorno dos dados da tabela do banco e faz o seu 
def ler_dados(table_name, engine):
    query = f"SELECT * FROM {table_name}"
    data = pd.read_sql(query, engine)
    return data

#---------------------------------------------------------------#
#--------------- DADOS: PRODUÇÃO DE PLÁSTICO -------------------#
#---------------------------------------------------------------#

def dados_producao_plastico(engine, relatorio, colunas):
    coluna_ano = colunas[1]
    coluna_producao = colunas[2]

    if relatorio == '1':
        query = f'''
        SELECT "{coluna_ano}", "{coluna_producao}"
        FROM producao_plastico_global
        ORDER BY "{coluna_producao}" DESC
        FETCH FIRST 5 ROWS ONLY
        '''
    elif relatorio == '2':
        query = f'''
        SELECT "{coluna_ano}",
               "{coluna_producao}",
               LAG("{coluna_producao}", 1) OVER (ORDER BY "{coluna_ano}") AS Produção_Anterior,
               ((("{coluna_producao}" - LAG("{coluna_producao}", 1) OVER (ORDER BY "{coluna_ano}")) / 
               LAG("{coluna_producao}", 1) OVER (ORDER BY "{coluna_ano}")) * 100) AS Crescimento_Percentual
        FROM producao_plastico_global
        ORDER BY "{coluna_ano}"
        '''
    elif relatorio == '3':
        query = f'''
        SELECT "{coluna_ano}", SUM("{coluna_producao}") AS Produção_Anual_Total
        FROM producao_plastico_global
        GROUP BY "{coluna_ano}"
        ORDER BY "{coluna_ano}"
        '''
    elif relatorio == '4':
        query = f'''
        SELECT SUM("{coluna_producao}") AS Produção_Total
        FROM producao_plastico_global
        WHERE "{coluna_ano}" BETWEEN 2000 AND 2010
        '''
    elif relatorio == '5':
        query = f'''
        SELECT TRUNC("{coluna_ano}", -1) AS Década, AVG("{coluna_producao}") AS Produção_Média_Década
        FROM producao_plastico_global
        GROUP BY TRUNC("{coluna_ano}", -1)
        ORDER BY TRUNC("{coluna_ano}", -1)
        '''
    elif relatorio == '6':
        ano_inicio = int(input("Digite o ano de início: "))
        ano_fim = int(input("Digite o ano de fim: "))
        query = f'''
        SELECT AVG("{coluna_producao}") AS Produção_Média_Anual
        FROM producao_plastico_global
        WHERE "{coluna_ano}" BETWEEN {ano_inicio} AND {ano_fim}
        '''
    elif relatorio == '7':
        ano_limite = int(input("Digite o ano limite: "))
        query = f'''
        SELECT SUM("{coluna_producao}") AS Produção_Total_Acumulada
        FROM producao_plastico_global
        WHERE "{coluna_ano}" <= {ano_limite}
        '''
    else:
        print("Opção de relatório inválida.")
        return
    
    data = pd.read_sql(query, engine)
    print(data)

#---------------------------------------------------------------#
#----------------- DADOS: DESPEJO DE RESÍDUO PLÁSTICO ----------#
#---------------------------------------------------------------#
    
def dados_despejo_plastico(engine, relatorio, colunas):
    coluna_entidade = colunas[0]
    coluna_ano = colunas[2] 
    coluna_participacao = colunas[3]

    if relatorio == '1':
        query = f'''
        SELECT "{coluna_entidade}", "{coluna_participacao}"
        FROM despejo_residuo_plastico
        WHERE "{coluna_ano}" = '2019'
        ORDER BY "{coluna_participacao}" DESC
        '''
    elif relatorio == '2':
        query = f'''
        SELECT "{coluna_entidade}", "{coluna_ano}", "{coluna_participacao}"
        FROM despejo_residuo_plastico
        ORDER BY "{coluna_ano}", "{coluna_entidade}"
        '''
    elif relatorio == '3':
        query = f'''
        SELECT "{coluna_entidade}", AVG("{coluna_participacao}") AS Participacao_Media
        FROM despejo_residuo_plastico
        GROUP BY "{coluna_entidade}"
        ORDER BY Participacao_Media DESC
        '''
    elif relatorio == '4':
        query = f'''
        SELECT "{coluna_entidade}", SUM("{coluna_participacao}") AS Participacao_Total
        FROM despejo_residuo_plastico
        WHERE "{coluna_entidade}" IN ('Africa', 'Asia', 'Europe', 'North America', 'Oceania', 'South America')
        GROUP BY "{coluna_entidade}"
        ORDER BY Participacao_Total DESC
        '''
    elif relatorio == '5':
        N = int(input("Digite o número de países que deseja consultar: "))
        query = f'''
        SELECT "{coluna_entidade}", SUM("{coluna_participacao}") AS Participacao_Total
        FROM despejo_residuo_plastico
        GROUP BY "{coluna_entidade}"
        ORDER BY Participacao_Total DESC
        LIMIT {N}
        '''
    else:
        print("Opção de relatório inválida.")
        return
    
    data = pd.read_sql(query, engine)
    print(data)

#---------------------------------------------------------------#
#------------------ DADOS: DESTINO DO PLÁSTICO -----------------#
#---------------------------------------------------------------#
       
def dados_destino_plastico(engine, relatorio, colunas):
    coluna_entidade = colunas[0]
    coluna_ano = colunas[2]
    coluna_reciclagem = colunas[3]
    coluna_queima = colunas[4]
    coluna_lixo_mal_gerido = colunas[5]
    coluna_aterro = colunas[6]

    if relatorio == '1':
        query = f'''
        SELECT "{coluna_entidade}", MAX("{coluna_lixo_mal_gerido}") AS max_lixo_mal_gerido
        FROM destino_plastico
        GROUP BY "{coluna_entidade}"
        ORDER BY max_lixo_mal_gerido DESC
        FETCH FIRST 10 ROWS ONLY
        '''
    elif relatorio == '2':
        query = f'''
        SELECT "{coluna_entidade}", SUM("{coluna_reciclagem}") AS total_reciclagem
        FROM destino_plastico
        GROUP BY "{coluna_entidade}"
        ORDER BY total_reciclagem DESC
        '''
    elif relatorio == '3':
        query = f'''
        SELECT "{coluna_entidade}", AVG("{coluna_queima}") AS media_queima
        FROM destino_plastico
        GROUP BY "{coluna_entidade}"
        ORDER BY media_queima DESC
        '''
    elif relatorio == '4':
        query = f'''
        SELECT "{coluna_ano}", "{coluna_aterro}"
        FROM destino_plastico
        WHERE "{coluna_entidade}" = 'Americas (excl. USA)'
        ORDER BY "{coluna_ano}"
        '''
    elif relatorio == '5':
        query = f'''
        SELECT "{coluna_ano}", AVG("{coluna_reciclagem}") AS media_reciclagem
        FROM destino_plastico
        GROUP BY "{coluna_ano}"
        ORDER BY "{coluna_ano}"
        '''
    elif relatorio == '6':
        query = f'''
        SELECT "{coluna_entidade}", AVG("{coluna_lixo_mal_gerido}") AS media_lixo_mal_gerido
        FROM destino_plastico
        GROUP BY "{coluna_entidade}"
        ORDER BY media_lixo_mal_gerido DESC
        '''
    else:
        print("Opção de relatório inválida.")
        return
    
    data = pd.read_sql(query, engine)
    print(data)

#---------------------------------------------------------------#
#--------------- DADOS: DESPERDÍCIO DE PLÁSTICO ----------------#
#---------------------------------------------------------------#
    
def dados_desperdicio_plastico(engine, relatorio, colunas):
    coluna_entidade = colunas[0]
    coluna_ano = colunas[2]
    coluna_lixo = colunas[3]

    if relatorio == '1':
        query = f'''
        SELECT "{coluna_entidade}", "{coluna_lixo}"
        FROM desperdicio_plastico_per_capita
        WHERE "{coluna_ano}" = 2019
        ORDER BY "{coluna_lixo}" DESC
        FETCH FIRST 10 ROWS ONLY
        '''
    elif relatorio == '2':
        query = f'''
        SELECT "{coluna_entidade}", SUM("{coluna_lixo}") AS total_lixo_mal_gerenciado
        FROM desperdicio_plastico_per_capita
        WHERE "{coluna_ano}" = 2019
        GROUP BY "{coluna_entidade}"
        ORDER BY total_lixo_mal_gerenciado DESC
        '''
    elif relatorio == '3':
        query = f'''
        SELECT "{coluna_ano}", AVG("{coluna_lixo}") AS media_lixo_mal_gerenciado
        FROM desperdicio_plastico_per_capita
        GROUP BY "{coluna_ano}"
        ORDER BY "{coluna_ano}"
        '''
    elif relatorio == '4':
        query = f'''
        SELECT "{coluna_entidade}", "{coluna_lixo}"
        FROM desperdicio_plastico_per_capita
        WHERE "{coluna_ano}" = 2019
        ORDER BY "{coluna_lixo}" ASC
        FETCH FIRST 10 ROWS ONLY
        '''
    elif relatorio == '5':
        query = f'''
        SELECT "{coluna_ano}", "{coluna_lixo}"
        FROM desperdicio_plastico_per_capita
        WHERE "{coluna_entidade}" = 'Brazil'
        ORDER BY "{coluna_ano}"
        '''
    elif relatorio == '6':
        query = f'''
        SELECT "{coluna_entidade}", SUM("{coluna_lixo}") AS total_desperdicio_plastico
        FROM desperdicio_plastico_per_capita
        GROUP BY "{coluna_entidade}"
        ORDER BY total_desperdicio_plastico DESC
        '''
    else:
        print("Opção de relatório inválida.")
        return
    
    data = pd.read_sql(query, engine)
    print(data)
    
#---------------------------------------------------------------#
#-------------- DADOS: POLUIÇÃO DA ÁGUA NAS CIDADES ------------#
#---------------------------------------------------------------#
    
def dados_poluicao_agua(engine, relatorio, colunas):
    coluna_cidade = colunas[0]
    coluna_regiao = colunas[1]
    coluna_entidade = colunas[2]
    coluna_qualidade_ar = colunas[3]
    coluna_poluicao_agua = colunas[4]

    if relatorio == '1':
        limite_qualidade_ar = float(input("Digite o limite de Qualidade do Ar: "))
        query = f'''
        SELECT "{coluna_cidade}", "{coluna_entidade}", "{coluna_qualidade_ar}"
        FROM poluicao_agua_cidades
        WHERE "{coluna_qualidade_ar}" > {limite_qualidade_ar}
        ORDER BY "{coluna_qualidade_ar}" DESC
        '''
    elif relatorio == '2':
        query = f'''
        SELECT "{coluna_cidade}", "{coluna_entidade}", "{coluna_poluicao_agua}"
        FROM poluicao_agua_cidades
        WHERE "{coluna_poluicao_agua}" < 30
        ORDER BY "{coluna_poluicao_agua}" ASC
        '''
    elif relatorio == '3':
        query = f'''
        SELECT "{coluna_cidade}", "{coluna_entidade}", "{coluna_poluicao_agua}"
        FROM poluicao_agua_cidades
        ORDER BY "{coluna_poluicao_agua}" DESC
        FETCH FIRST 10 ROWS ONLY
        '''
    elif relatorio == '4':
        query = f'''
        SELECT "{coluna_regiao}", AVG("{coluna_poluicao_agua}") AS media_poluicao_agua
        FROM poluicao_agua_cidades
        GROUP BY "{coluna_regiao}"
        ORDER BY media_poluicao_agua DESC
        '''
    elif relatorio == '5':
        query = f'''
        SELECT "{coluna_regiao}", AVG("{coluna_qualidade_ar}") AS media_qualidade_ar
        FROM poluicao_agua_cidades
        GROUP BY "{coluna_regiao}"
        ORDER BY media_qualidade_ar DESC
        '''
    elif relatorio == '6':
        query = f'''
        SELECT "{coluna_cidade}", "{coluna_entidade}", "{coluna_qualidade_ar}"
        FROM poluicao_agua_cidades
        ORDER BY "{coluna_qualidade_ar}" DESC
        FETCH FIRST 10 ROWS ONLY
        '''
    elif relatorio == '7':
        query = f'''
        SELECT "{coluna_cidade}", "{coluna_regiao}", "{coluna_qualidade_ar}", "{coluna_poluicao_agua}"
        FROM poluicao_agua_cidades
        WHERE "{coluna_cidade}" IN ('New York City', 'Berlin', 'Los Angeles')
        ORDER BY "{coluna_cidade}"
        '''
    else:
        print("Opção de relatório inválida.")
        return
    
    data = pd.read_sql(query, engine)
    print(data)

#---------------------------------------------------------------#
#-------------- MENU: PRODUÇÃO DE PLÁSTICO ---------------------#
#---------------------------------------------------------------#

def menu_relatorios_producao(engine, colunas):
    while True:
        print("\nSelecione o tipo de relatório:\n")
        print("1. Consultar os 5 anos com a maior produção de plástico")
        print("2. Consultar o crescimento percentual da produção de plástico ano a ano")
        print("3. Consultar a produção total de plástico por ano")
        print("4. Consultar a produção total de plástico para um determinado período")
        print("5. Consultar a produção média de plástico por década")
        print("6. Consultar a produção média anual de plástico para um intervalo de anos")
        print("7. Consultar a produção total acumulada de plástico até um determinado ano")
        print("8. Voltar")

        escolha_opcao = input("Escolha uma opção: ")

        if escolha_opcao == '8':
            break
        else:
            dados_producao_plastico(engine, escolha_opcao, colunas)

#---------------------------------------------------------------#
#------------- MENU: DESPEJO DE RESÍDUO PLÁSTICO ---------------#
#---------------------------------------------------------------#

def menu_despejo_plastico(engine, colunas):
    while True:
        print("\nSelecione o tipo de relatório:\n")
        print("1. Consultar a participação na emissão global de plásticos para o oceano por entidade mais recente")
        print("2. Consultar a participação na emissão global de plásticos para o oceano por entidade")
        print("3. Consultar a participação média anual na emissão global de plásticos para o oceano por entidade")
        print("4. Consultar a participação total na emissão global de plásticos para o oceano por continente")
        print("5. Consultar os top N países com maior participação na emissão global de plásticos para o oceano")
        print("6. Voltar ")

        escolha_opcao = input("Escolha uma opção: ")

        if escolha_opcao == '6':
            break
        else:
            dados_despejo_plastico(engine, escolha_opcao, colunas)

#---------------------------------------------------------------#
#------------- MENU: DESTINO DO PLÁSTICO -----------------------#
#---------------------------------------------------------------#

def menu_destino_plastico(engine, colunas):
    while True:
        print("\nSelecione o tipo de relatório:\n")
        print("1. Entidades com Maior Proporção de Lixo Mal Gerido")
        print("2. Total de Reciclagem por Entidade")
        print("3. Média de Queima por Entidade")
        print("4. Participação do Lixo Encaminhado para Aterros ao Longo dos Anos")
        print("5. Média de Reciclagem por Ano")
        print("6. Média de Lixo Mal Gerido por Entidade")
        print("7. Voltar")

        escolha_opcao = input("Escolha uma opção: ")

        if escolha_opcao == '7':
            break
        else:
            dados_destino_plastico(engine, escolha_opcao, colunas)

#---------------------------------------------------------------#
#------------- MENU: DESPERDÍCIO DE PLÁSTICO -------------------#
#---------------------------------------------------------------#

def menu_desperdicio_plastico(engine, colunas):
    while True:
        print("\nSelecione o tipo de relatório:\n")
        print("1. Top 10 Países com Maior Desperdício Plástico Per Capita em um Ano Específico")
        print("2. Total de Lixo Plástico Mal Gerenciado por Região em um Ano Específico")
        print("3. Média de Lixo Plástico Mal Gerenciado por Pessoa por Ano")
        print("4. Países com Menor Desperdício Plástico Per Capita em um Ano Específico")
        print("5. Variação Anual do Lixo Plástico Mal Gerenciado por País")
        print("6. Total de Desperdício Plástico Per Capita por Região")
        print("7. Voltar")

        escolha_opcao = input("Escolha uma opção: ")

        if escolha_opcao == '7':
            break
        else:
            dados_desperdicio_plastico(engine, escolha_opcao, colunas)
      
#---------------------------------------------------------------#
#------------- MENU: POLUIÇÃO DA ÁGUA NAS CIDADES --------------#
#---------------------------------------------------------------#
    
def menu_poluicao_agua(engine, colunas):
    while True:
        print("\nSelecione o tipo de relatório:\n")
        print("1. Cidades com Qualidade do Ar Acima de um Limite")
        print("2. Cidades com Poluição da Água Abaixo de um Limite")
        print("3. Top 10 Cidades com Maior Poluição da Água")
        print("4. Média de Poluição da Água por Região")
        print("5. Média de Qualidade do Ar por Região")
        print("6. Top 10 Cidades com Melhor Qualidade do Ar")
        print("7. Comparação da Qualidade do Ar e Poluição da Água")
        print("8. Voltar")

        escolha_opcao = input("Escolha uma opção: ")

        if escolha_opcao == '8':
            break
        else:
            dados_poluicao_agua(engine, escolha_opcao, colunas)

#---------------------------------------------------------------#
#-------------- PRÉVIA: PRODUÇÃO DE PLÁSTICO ------------------#
#---------------------------------------------------------------#

def previa_producao_plastico_global(engine):
    colunas = nomes_colunas('producao_plastico_global', engine)
    print("\n------------------------------------------------")
    print("PRÉVIA DOS DADOS: \n")

    while True:
        data = ler_dados('producao_plastico_global', engine)
        print("5 PRIMEIROS DADOS:")
        print(data.head())
        print("------------------------------------------------")
        print("5 ULTIMOS DADOS:")
        print(data.tail())
        print("------------------------------------------------")
        print("\nMAIS OPÇÕES SOBRE: PRODUÇÃO DE PLÁSTICO?")
        print("1. Sim")
        print("2. Voltar")

        escolha = input("Escolha uma opção: ")

        if escolha == '1':
            menu_relatorios_producao(engine, colunas)
        elif escolha == '2':
            break
        else:
            print("Opção inválida. Tente novamente.")

#---------------------------------------------------------------#
#------------- PRÉVIA: DESPEJO DE RESÍDUO PLÁSTICO ------------#
#---------------------------------------------------------------#

def previa_despejo_residuo_plastico(engine):
    colunas = nomes_colunas('despejo_residuo_plastico', engine)
    print("\n------------------------------------------------")
    print("PRÉVIA DOS DADOS: \n")

    while True:
        data = ler_dados('despejo_residuo_plastico', engine)
        print("5 PRIMEIROS DADOS:")
        print(data.head())
        print("------------------------------------------------")
        print("5 ULTIMOS DADOS:")
        print(data.tail())
        print("------------------------------------------------")
        print("\nMAIS OPÇÕES SOBRE: RESIDUO PLÁSTICO?")
        print("1. Sim")
        print("2. Voltar ")

        escolha = input("Escolha uma opção: ")

        if escolha == '1':
            menu_despejo_plastico(engine, colunas)
        elif escolha == '2':
            break
        else:
            print("Opção inválida. Tente novamente.")

#---------------------------------------------------------------#
#------------- PRÉVIA: DESTINO DO PLÁSTICO --------------------#
#---------------------------------------------------------------#

def previa_destino_plastico(engine):
    colunas = nomes_colunas('destino_plastico', engine)
    print("\n------------------------------------------------")
    print("Colunas disponíveis:")

    while True:
        data = ler_dados('destino_plastico', engine)
        print("5 PRIMEIROS DADOS:")
        print(data.head())
        print("------------------------------------------------")
        print("5 ULTIMOS DADOS:")
        print(data.tail())
        print("------------------------------------------------")
        print("\nMAIS OPÇÕES SOBRE: DESTINO DO PLÁSTICO?")
        print("1. Sim")
        print("2. Voltar ")

        escolha = input("Escolha uma opção: ")

        if escolha == '1':
            menu_destino_plastico(engine, colunas)
        elif escolha == '2':
            break
        else:
            print("Opção inválida. Tente novamente.")

#---------------------------------------------------------------#
#------------ PRÉVIA: DESPERDÍCIO DE PLÁSTICO -----------------#
#---------------------------------------------------------------#
            
def previa_desperdicio_plastico(engine):
    colunas = nomes_colunas('desperdicio_plastico_per_capita', engine)
    print("\n------------------------------------------------")
    print("PRÉVIA DOS DADOS")

    while True:
        data = ler_dados('desperdicio_plastico_per_capita', engine)
        print("5 PRIMEIROS DADOS:")
        print(data.head())
        print("------------------------------------------------")
        print("5 ULTIMOS DADOS:")
        print(data.tail())
        print("------------------------------------------------")
        print("\nDeseja buscar informações específicas?")
        print("1. Sim")
        print("2. Voltar ")

        escolha = input("Escolha uma opção: ")

        if escolha == '1':
            menu_desperdicio_plastico(engine, colunas)
        elif escolha == '2':
            break
        else:
            print("Opção inválida. Tente novamente.")


#---------------------------------------------------------------#
#----------- PRÉVIA: POLUIÇÃO DA ÁGUA NAS CIDADES -------------#
#---------------------------------------------------------------#
            
def previa_poluicao_agua(engine):
    colunas = nomes_colunas('poluicao_agua_cidades', engine)
    print("\n------------------------------------------------")
    print("PRÉVIA DOS DADOS")

    while True:
        data = ler_dados('poluicao_agua_cidades', engine)
        print("5 PRIMEIROS DADOS:")
        print(data.head())
        print("------------------------------------------------")
        print("5 ULTIMOS DADOS:")
        print(data.tail())
        print("------------------------------------------------")
        print("\nMAIS OPÇÕES SOBRE: POLUIÇÃO DA ÁGUA")
        print("1. Sim")
        print("2. Voltar ")

        escolha = input("Escolha uma opção: ")

        if escolha == '1':
            menu_poluicao_agua(engine, colunas)
        elif escolha == '2':
            break
        else:
            print("Opção inválida. Tente novamente.")

#---------------------------------------------------------------#
#---------------------- MENU PRINCIPAL -------------------------#
#---------------------------------------------------------------#
            
def exibir_menu(engine):

    while True:
        print("\nMenu Poseidon Developers")
        print("Digite uma opção para continuar:\n")
        print("1- Ver produção de plástico global")
        print("2- Ver destino do plástico")
        print("3- Ver desperdício de plástico per capita")
        print("4- Ver participação de despejo de resíduo plástico")
        print("5- Ver poluição da água nas cidades")
        print("6- Sair")

        escolha = input("\nEscolha uma opção: ")

        if escolha == '1':
            previa_producao_plastico_global(engine)
        elif escolha == '2':
            previa_destino_plastico(engine)
        elif escolha == '3':
            previa_desperdicio_plastico(engine)
        elif escolha == '4':
            previa_despejo_residuo_plastico(engine)
        elif escolha == '5':
            previa_poluicao_agua(engine)
        elif escolha == '6':
            print("Volte sempre!")
            break
        else:
            print("Opção inválida. Tente novamente.")

#---------------------------------------------------------------#
#---------------------------- MAIN -----------------------------#
#---------------------------------------------------------------#

def main():
    escrever_credenciais()

    # ARQUIVOS .CSV DO PROJETO
    arquivos_csv = [
        ('1- producao-de-plastico-global.csv', 'producao_plastico_global'),
        ('2- participacao-despejo-residuo-plastico.csv', 'despejo_residuo_plastico'),
        ('3- destino-plastico.csv', 'destino_plastico'),
        ('4- desperdicio-plastico-per-capita.csv', 'desperdicio_plastico_per_capita'),
        ('5- poluicao-agua-cidades.csv', 'poluicao_agua_cidades')
    ]

    connection, engine = conectar_banco_de_dados('credentials.txt')

    try:
        # INSERÇÃO DO CSV NO DB
        for csv_file, table_name in arquivos_csv:
            inserir_dados_csv_no_banco(csv_file, table_name, engine)

        exibir_menu(engine)
    finally:
        connection.close()
        limpar_credenciais()

if __name__ == "__main__":
    main()