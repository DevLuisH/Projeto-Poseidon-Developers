# Projeto Global Solution: Poseidon Developers

## Descrição

Este projeto tem como objetivo analisar dados relacionados à produção de plástico, despejo de resíduos plásticos, destino do plástico, desperdício de plástico per capita e poluição da água nas cidades. Os dados são armazenados em um banco de dados Oracle e podem ser consultados por meio de relatórios gerados pelo programa.

## Autores

- Luis Henrique - 552692
- Luccas dos Anjos - 552890
- Igor Gabriel - 553544

## Pré-requisitos

Certifique-se de ter os seguintes pacotes instalados antes de executar o projeto:

- `pandas`
- `oracledb`
- `sqlalchemy`

Você pode instalar os pacotes necessários usando o seguinte comando:

bash
pip install pandas oracledb sqlalchemy


Execute o script principal:
python main.py

Funcionamento
O programa solicitará as credenciais do banco de dados Oracle.
As credenciais serão validadas e armazenadas em um arquivo credentials.txt.
Os arquivos CSV serão inseridos no banco de dados Oracle.
O menu principal será exibido, permitindo a geração de relatórios sobre os dados de plástico e poluição da água.

Estrutura do Projeto
main.py: Script principal que executa o programa.
credentials.txt: Arquivo onde as credenciais do banco de dados são armazenadas temporariamente.
Vários arquivos CSV contendo dados a serem inseridos no banco de dados.

Notas
As credenciais são armazenadas temporariamente no arquivo credentials.txt para fins de simulação de login. Para armazenamento seguro, considere usar uma biblioteca como keyring.
O banco de dados Oracle deve estar acessível no DSN oracle.fiap.com.br:1521/orcl.

Limpeza
Para remover as credenciais armazenadas, basta excluir o arquivo credentials.txt ou sair do programa finalizando no proprio menu.

