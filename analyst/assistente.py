import google.generativeai as genai
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

load_dotenv()

# Substitua pela sua chave de API do Google AI Studio
GOOGLE_API_KEY = os.getenv("GEMINI_API_KEY")

# Configure a chave da API
genai.configure(api_key=GOOGLE_API_KEY)

# Seleciona o modelo Gemini Pro
model = genai.GenerativeModel('gemini-2.0-flash')

# Configuração da URL do banco de dados MySQL
db_url = os.getenv("URL_MYSQL")

# Cria o engine do SQLAlchemy
engine = create_engine(db_url)

# Função para gerar SQL com Gemini
def gerar_sql(pergunta, contexto_tabela):
    
    # Incluir isso se quiser garantir que a resposta seja formatada corretamente:
    prompt = f"""{contexto_tabela}

    Sua tarefa é converter a pergunta abaixo em uma consulta SQL (MySQL) do tipo SELECT, usando a tabela 'view_operacoes_financeiras'. 

    - Retorne apenas o código SQL, sem explicações.
    - Use nomes de colunas exatamente como estão no contexto.
    - Caso haja filtros de data, considere o formato AAAA-MM-DD.

    Pergunta do usuário: {pergunta}
    """

    response = model.generate_content(prompt)
    
    # Junta todas as partes da resposta em uma string única
    sql_query_raw = "".join(part.text for part in response.parts)

    # Remove blocos markdown e espaços extras
    sql_query = sql_query_raw.strip().replace("```sql", "").replace("```", "").strip()
        
    # Evita manipular coisas como "sql" dentro da query
    return sql_query


def executar_sql(sql_query):
    try:
        with engine.connect() as connection:
            result = connection.execute(text(sql_query))
            if result.returns_rows:
                columns = result.keys()
                resultados = [dict(zip(columns, row)) for row in result]
                return resultados
            else:
                print("\nConsulta SQL executada com sucesso (sem resultados).")
                return []
    except SQLAlchemyError as e:
        print(f"\nErro ao executar a consulta SQL: {e}")
        return None


def analisar_dados(data, contexto_tabela):
    prompt_analise = f"""
    Você é um analista de dados.

    Abaixo estão dados financeiros da tabela 'view_operacoes_financeiras', que registra movimentos financeiros individuais utilizados para análises de receitas, despesas e investimentos.

    {contexto_tabela}

    Dados:
    {data}

    Com base nas informações acima, escreva uma análise descritiva clara e objetiva, destacando os principais padrões ou comportamentos observados. A análise deve começar com um parágrafo resumindo os principais insights de forma detalhada.

    Em seguida, liste sugestões de ações ou investigações adicionais que poderiam ajudar a entender melhor os dados ou apoiar decisões estratégicas. 

    Evite usar negritos, itálicos ou símbolos especiais na resposta. Mantenha o texto limpo e direto.
    """
    response = model.generate_content(prompt_analise)
    return "".join(part.text for part in response.parts).strip()