import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, Tuple
import os
import logging

class ProcessamentoFinanceiro:
    def __init__(self):
        
        # [Mantido o código anterior dos mapeamentos...]
        self.mapeamento_motivos = {
            'pagto fatura master': 'Pagamento cartão de credito',
            'aplic.financ.aviso previo': 'Saida para investimento',
            'credito folha pgto.': 'Salario 25',
            'liquidacao boleto sicredi  ziani e': 'Condominio',
            'adto. salario mes': 'Salario 10',
            'liquidacao boleto  pjbank pagament': 'Aluguel',
            'resg.aplic.fin.aviso prev': 'Resgate de investimento',
            'liquidacao boleto sicredi  rede co': 'Internet',
            'pagamento pix sicredi  rede conesu': 'Internet',
            'debito convenios  rge sul-g': 'Luz',
            'debito convenios id  adm.c': 'Consorcio',
            'liquidacao boleto  pjbank': 'Aluguel',
            'pagamento pix  pjbank pagamentos s': 'Aluguel',
            'pagamento bolsa auxilio': 'Salario Estagio',
            'liquidacao boleto  lopes planos': 'Plano de Saude (Angelus)'
        }
        
        # [Mantido o resto do código anterior...]
        # Casos especiais baseados em valor e descrição
        self.casos_especiais_motivos = [
            {'valor': 109, 'descricao': None, 'motivo': 'Academia'},
            {'valor': 95, 'descricao': 'pagamento pix sicredi  rafael mardega', 'motivo': 'Academia'},
            {'valor': 65, 'descricao': 'pagamento pix  ana paula zalamena', 'motivo': 'Convenio Mãe (Vans)'},
            {'valor': None, 'descricao': 'pagamento pix sicredi  carla cristian', 'motivo': 'Pastel dona Carla'}
        ]
        
        # [Mantido o código das listas de categorização...]
        self.contas_fixas = [
            'liquidacao boleto sicredi  ziani e',
            'liquidacao boleto  pjbank pagament',
            'liquidacao boleto sicredi  rede co',
            'pagamento pix sicredi  rede conesu',
            'debito convenios  rge sul-g',
            'debito convenios id  adm.c',
            'liquidacao boleto  pjbank',
            'pagamento pix  pjbank pagamentos s',
            'liquidacao boleto  lopes planos'
        ]
        
        self.investimentos = [
            'aplic.financ.aviso previo',
            'resg.aplic.fin.aviso prev'
        ]
        
        self.salarios = [
            'credito folha pgto.',
            'adto. salario mes',
            'pagamento bolsa auxilio'
        ]

    # [Mantido os métodos determinar_motivo e determinar_tipo...]
    def determinar_motivo(self, row: pd.Series) -> str:
        if row['descricao'] in self.mapeamento_motivos:
            return self.mapeamento_motivos[row['descricao']]
        
        for caso in self.casos_especiais_motivos:
            if row['valor'] == caso['valor']:
                if caso['descricao'] is None or row['descricao'] == caso['descricao']:
                    return caso['motivo']
        
        return 'Outros'

    def determinar_tipo(self, row: pd.Series) -> str:
        descricao = row['descricao']
        valor = row['valor']
        
        if (descricao in self.contas_fixas or 
            valor == 109 or
            (valor == 95 and descricao == 'pagamento pix sicredi  rafael mardega') or
            (valor == 65 and descricao == 'pagamento pix  ana paula zalamena')):
            return 'Contas Fixas'
        
        if descricao in self.investimentos:
            return 'Investimento'
        
        if descricao in self.salarios:
            return 'Salario'
        
        if descricao == 'pagto fatura master':
            return 'Cartão de Crédito'
        
        return 'Outros'

    def processar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        df_processado = df.copy()
        df_processado['motivo'] = df_processado.apply(self.determinar_motivo, axis=1)
        df_processado['tipo'] = df_processado.apply(self.determinar_tipo, axis=1)
        return df_processado

class GerenciadorBancoDados:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.engine = None
        self._configurar_logging()

    def _configurar_logging(self):
        """Configura o logging para a classe"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def conectar(self) -> Optional[bool]:
        """
        Estabelece conexão com o banco de dados
        Retorna True se bem sucedido, False caso contrário
        """
        try:
            self.engine = create_engine(self.db_url)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("Conexão com o banco de dados estabelecida com sucesso")
            return True
        except Exception as e:
            self.logger.error(f"Erro ao conectar ao banco de dados: {str(e)}")
            return False

    def ler_dados(self, query: str) -> Optional[pd.DataFrame]:
        """
        Lê dados do banco usando a query fornecida
        """
        try:
            if not self.engine:
                if not self.conectar():
                    return None
            
            df = pd.read_sql(query, self.engine)
            self.logger.info(f"Dados lidos com sucesso. Registros: {len(df)}")
            return df
        except Exception as e:
            self.logger.error(f"Erro ao ler dados: {str(e)}")
            return None

    def salvar_dados(self, df: pd.DataFrame, tabela: str, if_exists: str = 'append') -> bool:
        """
        Salva DataFrame no banco de dados com colunas específicas
        """
        try:
            if not self.engine:
                if not self.conectar():
                    return False
            
            # Seleciona apenas as colunas desejadas
            colunas_desejadas = ['id', 'data', 'descricao', 'valor', 'valor_original', 'motivo', 'tipo', 'tipo_movimentacao']
            df_final = df[colunas_desejadas]
            
            df_final.to_sql(
                name=tabela,
                con=self.engine,
                if_exists=if_exists,
                index=False
            )
            self.logger.info(f"Dados salvos com sucesso na tabela {tabela}")
            return True
        except Exception as e:
            self.logger.error(f"Erro ao salvar dados: {str(e)}")
            return False

def criar_view_contas_principais(db_url: str) -> Tuple[Optional[pd.DataFrame], bool]:
    """
    Processa os dados financeiros e salva no banco
    Retorna uma tupla com (DataFrame processado, status do salvamento)
    """
    # Inicializa o gerenciador de banco de dados
    db = GerenciadorBancoDados(db_url)
    
    # Lê os dados
    query = "SELECT * FROM gestao_financas.extrato_conta_corrente"
    df = db.ler_dados(query)
    
    if df is None:
        return None, False
    
    # Processa os dados
    processador = ProcessamentoFinanceiro()
    df_processado = processador.processar_dados(df)
    
    # Salva os dados processados
    sucesso = db.salvar_dados(
        df=df_processado,
        tabela='contas_principais',
        if_exists='append'
    )
    
    return df_processado, sucesso

# Exemplo de uso
if __name__ == "__main__":
    db_url = os.getenv("URL_MYSQL")
    
    df_processado, sucesso = criar_view_contas_principais(db_url)
    
    if sucesso:
        print("Processamento e salvamento concluídos com sucesso!")
        print(f"Total de registros processados: {len(df_processado)}")
    else:
        print("Houve um erro no processamento ou salvamento dos dados.")