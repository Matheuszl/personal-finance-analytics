from flask import Flask, request, render_template, flash, redirect, url_for
import os
import pandas as pd
from werkzeug.utils import secure_filename
import subprocess
import tempfile
from datetime import datetime
import re
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

app = Flask(__name__)

app.config['SECRET_KEY'] = 'chave_secreta_para_flash_messages'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload
app.config['ALLOWED_EXTENSIONS'] = {'xls', 'xlsx'}

# Configurações do banco de dados
DB_URL = os.getenv("URL_MYSQL")

# Garantir que a pasta de uploads exista
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def allowed_file(filename):
    """Verifica se a extensão do arquivo é permitida"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def testar_conexao_db():
    """Testa a conexão com o banco de dados"""
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {str(e)}")
        return False

def verificar_e_criar_tabela(engine):
    """
    Verifica se a tabela existe e, se existir, não a recria.
    Se não existir, cria a tabela.
    """
    try:
        # Verifica se a tabela existe
        check_table_sql = """
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'gestao_financas' 
        AND table_name = 'extrato_conta_corrente';
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(check_table_sql)).fetchone()
            table_exists = result[0] > 0
            
            if not table_exists:
                # SQL para criar a tabela do zero
                criar_tabela_sql = """
                CREATE TABLE extrato_conta_corrente (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    data DATE,
                    descricao VARCHAR(255),
                    documento VARCHAR(50),
                    valor DECIMAL(10,2),
                    valor_original DECIMAL(10,2),
                    saldo DECIMAL(10,2),
                    arquivo_origem VARCHAR(255),
                    tipo_movimentacao VARCHAR(50),
                    data_processamento DATETIME
                );
                """
                conn.execute(text(criar_tabela_sql))
                conn.commit()
                print("✅ Tabela criada com sucesso!")
            else:
                print("✅ Tabela já existe, não será recriada.")
            
        return True

    except Exception as e:
        print(f"Erro ao verificar/criar tabela: {str(e)}")
        return False

def processar_arquivo_excel(caminho_arquivo):
    """
    Processa um arquivo Excel (.xls) e retorna um DataFrame com os dados limpos
    """
    try:
        # Lê o arquivo Excel ignorando as 10 primeiras linhas
        df = pd.read_excel(caminho_arquivo, skiprows=10, header=None)
        
        # Encontra o índice onde está "Saldo da Conta" ou uma linha em branco
        indice_final = None
        
        for idx, row in df.iterrows():
            # Verifica se todos os valores da linha são NaN ou espaços em branco
            is_linha_vazia = all(
                pd.isna(valor) or (isinstance(valor, str) and valor.strip() == '')
                for valor in row
            )
            
            # Se encontrar "Saldo da Conta" ou linha vazia, para a leitura
            if is_linha_vazia or any(str(valor).strip().lower() == "saldo da conta" for valor in row):
                indice_final = idx
                break
        
        # Se encontrou ponto de parada, corta o DataFrame
        if indice_final is not None:
            df = df.iloc[:indice_final]
        
        # Remove linhas com todos os valores NaN
        df = df.dropna(how='all')
        
        # Remove linhas onde todos os valores são espaços em branco
        df = df[~df.astype(str).apply(lambda x: x.str.strip().eq('').all(), axis=1)]
        
        # Adiciona os cabeçalhos específicos
        novos_cabecalhos = ['data', 'descricao', 'documento', 'valor', 'saldo']
        
        # Verifica se o número de colunas corresponde ao número de cabeçalhos
        if len(df.columns) == len(novos_cabecalhos):
            df.columns = novos_cabecalhos
        else:
            print(f"Aviso: Arquivo {os.path.basename(caminho_arquivo)}: O número de colunas ({len(df.columns)}) não corresponde ao número de cabeçalhos ({len(novos_cabecalhos)})")
            if len(df.columns) > len(novos_cabecalhos):
                df.columns = novos_cabecalhos + [f'coluna_{i+1}' for i in range(len(df.columns) - len(novos_cabecalhos))]
            else:
                df.columns = novos_cabecalhos[:len(df.columns)]
        
        return df
    
    except Exception as e:
        print(f"Erro ao processar o arquivo {os.path.basename(caminho_arquivo)}: {str(e)}")
        return None

def processar_e_salvar_no_db(caminho_arquivo):
    """
    Processa o arquivo Excel e salva no banco de dados
    """
    try:
        # Testa a conexão com o banco
        engine = create_engine(DB_URL)
        
        # Verifica/cria a tabela se necessário
        if not verificar_e_criar_tabela(engine):
            return False, "Não foi possível criar/verificar a tabela no banco de dados."
        
        # Processa o arquivo
        df = processar_arquivo_excel(caminho_arquivo)
        
        if df is None or df.empty:
            return False, "Arquivo vazio ou com formato inválido."
        
        # Nome da tabela no banco de dados
        nome_tabela = 'extrato_conta_corrente'
        
        # Converte a coluna de data para datetime
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce')
        
        # Limpa os numeros da coluna descricao
        df['descricao'] = df['descricao'].apply(lambda x: re.sub(r'[0-9]', '', str(x)).lower().strip())
        
        # Converte valor e saldo para número
        for coluna in ['valor', 'saldo']:
            df[coluna] = pd.to_numeric(
                df[coluna].astype(str).str.replace(',', '.', regex=False).str.replace('R$', '', regex=False).str.strip(),
                errors='coerce'
            )
            
        # Criar a coluna "movimentacao"
        df['tipo_movimentacao'] = df.apply(
            lambda row: 'Transferências para Investimentos' if row['descricao'] == 'aplic.financ.aviso previo'
            else ('Entrada' if row['valor'] > 0 else 'Saída' if row['valor'] < 0 else 'Neutra'),
            axis=1
        )
                            
        # Armazena o valor original
        df['valor_original'] = df['valor']
        
        # Cria a coluna "valor_absoluto"
        df['valor'] = df['valor'].abs()    
        
        # Adiciona informações extras
        df['arquivo_origem'] = os.path.basename(caminho_arquivo)
        df['data_processamento'] = datetime.now()
        
        # Salva no banco de dados em chunks para evitar problemas de memória
        chunk_size = 1000
        for i in range(0, len(df), chunk_size):
            chunk_df = df.iloc[i:i + chunk_size]
            chunk_df.to_sql(
                nome_tabela,
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=chunk_size
            )
        
        registros_processados = len(df)
        engine.dispose()
        
        return True, f"Arquivo processado com sucesso: {registros_processados} registros inseridos"
    
    except Exception as e:
        return False, f"Erro ao processar arquivo: {str(e)}"

def executar_transformacao():
    """Executa o script de transformação de dados como um processo separado"""
    try:
        import subprocess
        import os
        
        # Obtém o caminho completo para o script de transformação
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scr_transform_data.py")
        
        # Verifica se o arquivo existe
        if not os.path.exists(script_path):
            print(f"Erro: Script não encontrado em {script_path}")
            return False
        
        # Executa o script como um processo separado
        print(f"Executando script de transformação: {script_path}")
        
        result = subprocess.run(
            ["python", script_path], 
            check=False,
            capture_output=True,
            text=True
        )
        
        # Verifica o resultado
        if result.returncode == 0:
            print("Transformação executada com sucesso!")
            return True
        else:
            print(f"Erro ao executar o script de transformação.")
            print(f"Saída de erro: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Erro ao executar transformação: {str(e)}")
        return False

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # Verifica se há arquivo na requisição
        if 'file' not in request.files:
            flash('Nenhum arquivo selecionado', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        
        # Se o usuário não selecionar um arquivo
        if file.filename == '':
            flash('Nenhum arquivo selecionado', 'error')
            return redirect(request.url)
        
        # Verifica a conexão com o banco de dados
        if not testar_conexao_db():
            flash('Erro ao conectar ao banco de dados. Verifique as configurações.', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            
            # Salva o arquivo
            file.save(filepath)
            
            # Processa o arquivo e salva no banco
            sucesso, mensagem = processar_e_salvar_no_db(filepath)
            
            if sucesso:
                flash(mensagem, 'success')
                
                # Executa a transformação dos dados
                if executar_transformacao():
                    flash('Dados transformados com sucesso!', 'success')
                else:
                    flash('Erro ao executar a transformação dos dados.', 'warning')
            else:
                flash(mensagem, 'error')
                
            return redirect(url_for('upload_file'))
        else:
            flash('Tipo de arquivo não permitido. Utilize apenas arquivos .xls ou .xlsx', 'error')
            return redirect(request.url)
    
    return render_template('upload.html')

if __name__ == '__main__':
    app.run(port=5001)
    app.run(debug=True)