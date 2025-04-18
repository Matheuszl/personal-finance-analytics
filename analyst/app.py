from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import assistente 

app = Flask(__name__)
CORS(app) # Habilita CORS para permitir requisições do front-end

contexto_tabela = """
    Contexto da tabela 'view_operacoes_financeiras':
    A tabela armazena informações sobre operações financeiras. Cada linha representa um movimento financeiro individual.
    Os dados podem ser utilizados para análises de gastos, receitas e investimentos.
    A tabela contém as seguintes colunas:

    - id (BIGINT): identificador primário da tabela.
    - tipo_movimentacao (TEXT): indica se o movimento foi uma Entrada, Saída ou Transferência para Investimentos.
    - categoria (TEXT): define a categoria do movimento, podendo ser Investimento, Salário, Contas Fixas, Cartão de Crédito ou Outros.
    - motivo (TEXT): descreve o motivo do movimento, como Internet, Luz, Academia, entre outros.
    - valor (DOUBLE): representa o valor financeiro do movimento.
    - data (DATE): armazena a data do movimento no formato AAAA-MM-DD.
    """


@app.route('/')
def homepage():
    return render_template('index.html')

@app.route('/ask', methods=['POST'])
def ask_question():
    data = request.get_json()
    pergunta = data.get('pergunta')

    if not pergunta:
        return jsonify({'error': 'Pergunta não fornecida'}), 400

    sql_gerado = assistente.gerar_sql(pergunta, contexto_tabela)
    resultados_sql = None
    analise_dados = None

    if sql_gerado:
        resultados_sql = assistente.executar_sql(sql_gerado)
        if resultados_sql:
            analise_dados = assistente.analisar_dados(resultados_sql, contexto_tabela)

    response = {
        'sql_gerado': sql_gerado,
        'resultados_sql': resultados_sql,
        'analise_dados': analise_dados
    }

    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)