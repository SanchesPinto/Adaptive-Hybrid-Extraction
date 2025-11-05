import logging
import json
import os
import time
from parser_repository import ParserRepository

# --- Importamos a nossa "caixa preta", a função principal ---
# (Precisamos garantir que o main.py não execute o __main__
#  quando for importado, então limpe o bloco if __name__ == "__main__":
#  do seu main.py se ele ainda tiver o sleep)
from main import processar_extracao

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- DEFINIÇÃO DOS DADOS DE TESTE (Do PDF do Desafio) ---

LABEL_OAB = "carteira_oab" 

# --- DADOS DO EXEMPLO 1 (SON GOKU) ---
PDF_TEXTO_EXEMPLO1 = """
SON GOKU

Inscrição
101943
Seccional
PR
Subseção
CONSELHO SECCIONAL-PARANÁ

SUPLEMENTAR
Endereco Profissional
Telefone Profissional
SITUAÇÃO REGULAR
""" 

SCHEMA_EXEMPLO1 = {
  "nome": "Nome do profissional, normalmente no canto superior esquerdo da imagem",
  "inscricao": "Número de inscrição do profissional",
  "seccional": "Seccional do profissional",
  "subsecao": "Subseção à qual o profissional faz parte",
  "categoria": "Categoria, pode ser ADVOGADO, ADVOGADA, SUPLEMENTAR, ESTAGIARIO, ESTAGIARIA",
  "telefone_profissional": "Telefone do profissional",
  "situacao": "Situação do profissional, normalmente no canto inferior direito."
} 

# --- DADOS DO EXEMPLO 2 (JOANA D'ARC) ---
PDF_TEXTO_EXEMPLO2 = """
JOANA D'ARC
Inscrição
101943
Seccional
Subseção
PR
CONSELHO SECCIONAL-PARANA
SUPLEMENTAR
Endereço Profissional
AVENIDA PAULISTA, N 2300 andar Pilotis. Bela Vista
SÃO PAULO-SP
01310300
Telefone Profissional
SITUAÇÃO REGULAR
""" 

SCHEMA_EXEMPLO2 = {
  "nome": "Nome do profissional, normalmente no canto superior esquerdo da imagem",
  "inscricao": "Número de inscrição do profissional",
  "seccional": "Seccional do profissional",
  "subsecao": "Subseção à qual o profissional faz parte",
  "categoria": "Categoria, pode ser ADVOGADO, ADVOGADA, SUPLEMENTAR, ESTAGIARIO, ESTAGIARIA",
  "endereco_profissional": "Endereço profissional completo",
  "situacao": "Situação do profissional, normalmente no canto inferior direito."
} 

# Tempo de espera (em segundos) para a tarefa de background (M1) terminar
# Seus logs mostraram ~56 segundos. Vamos usar 60 para ter uma margem.
TEMPO_ESPERA_GERACAO = 60

# --- FUNÇÃO PRINCIPAL DO HARNESS ---

def executar_testes():
    logging.info("--- [HARNESS] INICIANDO TESTE DE ARQUITETURA V2.1 ---")
    repo = ParserRepository()

    # --- ETAPA 1: LIMPEZA ---
    logging.info("[HARNESS] ETAPA 1: Limpando o cache e locks...")
    parser_path = repo._get_parser_filepath(LABEL_OAB)
    lock_path = repo._get_lock_filepath(LABEL_OAB)
    if os.path.exists(parser_path): os.remove(parser_path)
    if os.path.exists(lock_path): os.remove(lock_path)
    logging.info("[HARNESS] Cache e locks limpos.")
    
    # --- ETAPA 2: TESTE DE CACHE MISS (REQUISIÇÃO 1) ---
    logging.info("\n--- [HARNESS] ETAPA 2: Teste de Cache Miss (Req 1 - Goku)")
    logging.info("[HARNESS] Esperado: Fallback Síncrono (Rápido) + Disparo de Thread (Background)")
    
    start_time_1 = time.time()
    resultado_1 = processar_extracao(LABEL_OAB, SCHEMA_EXEMPLO1, PDF_TEXTO_EXEMPLO1.strip())
    end_time_1 = time.time()
    
    logging.info(f"[HARNESS] Resposta Síncrona 1 recebida em {end_time_1 - start_time_1:.2f} segundos.")
    logging.info(f"[HARNESS] Resultado 1 (do Fallback):\n{json.dumps(resultado_1, indent=2, ensure_ascii=False)}\n")
    
    # --- ETAPA 3: TESTE DE LOCK (REQUISIÇÃO 2) ---
    logging.info("\n--- [HARNESS] ETAPA 3: Teste de Lock (Req 2 - Goku)")
    logging.info("[HARNESS] Esperado: Fallback Síncrono (Rápido) + 'Lock encontrado' (Sem nova thread)")
    
    start_time_2 = time.time()
    resultado_2 = processar_extracao(LABEL_OAB, SCHEMA_EXEMPLO1, PDF_TEXTO_EXEMPLO1.strip())
    end_time_2 = time.time()

    logging.info(f"[HARNESS] Resposta Síncrona 2 recebida em {end_time_2 - start_time_2:.2f} segundos.")
    logging.info(f"[HARNESS] Resultado 2 (do Fallback):\n{json.dumps(resultado_2, indent=2, ensure_ascii=False)}\n")

    # --- ETAPA 4: ESPERA DA GERAÇÃO EM BACKGROUND ---
    logging.info(f"\n--- [HARNESS] ETAPA 4: Aguardando {TEMPO_ESPERA_GERACAO}s para a thread de geração (M1) terminar...")
    time.sleep(TEMPO_ESPERA_GERACAO)
    
    # --- ETAPA 5: TESTE DE CACHE HIT E ROBUSTEZ (REQUISIÇÃO 3) ---
    logging.info("\n--- [HARNESS] ETAPA 5: Teste de Cache Hit e Robustez (Req 3 - Joana)")
    logging.info("[HARNESS] Usando parser gerado pelo Goku para extrair dados da Joana.")
    logging.info("[HARNESS] Esperado: 'CACHE HIT' (Rápido) + Dados da Joana (incluindo endereço)")

    start_time_3 = time.time()
    # Usamos o Schema 2 (Joana) e o Texto 2 (Joana), mas o Label é o mesmo
    resultado_3 = processar_extracao(LABEL_OAB, SCHEMA_EXEMPLO2, PDF_TEXTO_EXEMPLO2.strip())
    end_time_3 = time.time()

    logging.info(f"[HARNESS] Resposta Síncrona 3 recebida em {end_time_3 - start_time_3:.2f} segundos.")
    logging.info(f"[HARNESS] Resultado 3 (do Parser Regex):\n{json.dumps(resultado_3, indent=2, ensure_ascii=False)}\n")

    logging.info("--- [HARNESS] TESTE COMPLETO ---")

if __name__ == "__main__":
    # Limpe o bloco if __name__ == "__main__": do seu main.py
    # para que ele não rode quando este script o importar.
    
    # (Opcional, mas recomendado: Mova o bloco __main__
    #  do main.py para cá)
    
    executar_testes()
