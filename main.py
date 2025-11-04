import logging
import json 
import os
import threading # <-- 1. Importamos o módulo de threading

# --- Importando todos os nossos Módulos ---
from parser_repository import ParserRepository
from parser_generator import ParserGenerator         # Módulo 1 (Lento)
from parser_executor import ParserExecutor           # Módulo 2 (Rápido)
from confidence_calculator import ConfidenceCalculator # Módulo 3 (Rápido)
from fallback_extractor import FallbackExtractor     # Módulo de Fallback (Rápido)

# Configuração inicial de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# (Você pode reverter isso para 0.8 após o teste)
MIN_CONFIDENCE_THRESHOLD = 0.8 

def processar_extracao(label: str, schema: dict, pdf_text: str):
    """
    Função principal do Orquestrador (Módulo 4), agora com
    lógica de "lock" V2.1 para evitar "race conditions".
    """
    logging.info(f"Iniciando processamento (V2.1 Lock) para o label: {label}")
    
    repo = ParserRepository()
    parser = repo.get_parser(label)

    if parser:
        # --- CAMINHO 1: CACHE HIT (O "Caminho Rápido") ---
        # (Esta lógica está perfeita. Nenhuma mudança aqui.)
        logging.info("CACHE HIT. Acionando Módulo 2 (Executor)...")
        executor = ParserExecutor()
        extracted_data = executor.execute_parser(parser, pdf_text)
        
        logging.info("--- DADOS EXTRAÍDOS (Resultado Módulo 2) ---")
        logging.info(json.dumps(extracted_data, indent=2, ensure_ascii=False))
        
        calculator = ConfidenceCalculator()
        confidence = calculator.calculate_confidence(extracted_data, schema)
        
        if confidence >= MIN_CONFIDENCE_THRESHOLD:
            logging.info(f"Confiança Alta ({confidence:.2f} >= {MIN_CONFIDENCE_THRESHOLD}). Retornando dados do Módulo 2.")
            return extracted_data
        else:
            # (Lógica de Fallback Otimizado V2 permanece a mesma)
            logging.warning(f"Confiança Baixa ({confidence:.2f} < {MIN_CONFIDENCE_THRESHOLD}). Acionando Fallback Otimizado (Modo 2)...")
            fallback = FallbackExtractor()
            campos_faltantes = {
                k: v for k, v in schema.items() 
                if k not in extracted_data or not extracted_data[k]
            }
            if not campos_faltantes:
                 return extracted_data # Retorna o melhor que temos

            fallback_data = fallback.extract_missing(campos_faltantes, pdf_text, extracted_data)
            
            if fallback_data:
                final_data = extracted_data.copy(); final_data.update(fallback_data)
                return final_data
            else:
                return extracted_data
    
    else:
        # --- CAMINHO 2: CACHE MISS (V2.1 com Lock) ---
        # Esta é a lógica refatorada.
        
        logging.warning(f"CACHE MISS para {label}. Verificando lock de geração...")

        # 1. VERIFICAR O LOCK
        if repo.is_generation_locked(label):
            # Geração já está em progresso. Pular a thread.
            logging.warning(f"Geração para '{label}' já em progresso (lock encontrado). Pulando criação de nova thread.")
        else:
            # Ninguém está gerando. Vamos criar o lock e disparar a thread.
            logging.info(f"Lock não encontrado. Criando lock e disparando thread de geração...")
            
            # 2. CRIAR O LOCK
            repo.create_lock(label) 

            # 3. TAREFA ASSÍNCRONA (Background)
            def _run_parser_generation_task():
                # Instancia novas classes dentro da thread para segurança
                task_repo = ParserRepository()
                task_generator = ParserGenerator()
                try:
                    logging.info(f"[BACKGROUND] TAREFA INICIADA: Gerando parser para '{label}'...")
                    new_parser = task_generator.generate_parser(schema, pdf_text)
                    
                    if new_parser:
                        task_repo.save_parser(label, new_parser)
                        logging.info(f"[BACKGROUND] TAREFA CONCLUÍDA: Novo parser para '{label}' salvo.")
                    else:
                        logging.error(f"[BACKGROUND] TAREFA FALHOU: Módulo 1 falhou em gerar parser para '{label}'.")
                
                except Exception as e:
                    logging.error(f"[BACKGROUND] TAREFA CRASHOU: {e}")
                
                finally:
                    # 4. REMOVER O LOCK (ESSENCIAL!)
                    # Aconteça o que acontecer (sucesso ou falha), removemos o lock
                    # para que a próxima requisição possa tentar gerar novamente.
                    logging.info(f"[BACKGROUND] Removendo lock para '{label}'...")
                    task_repo.remove_lock(label)

            # Dispara a thread em background.
            generation_thread = threading.Thread(target=_run_parser_generation_task)
            generation_thread.start() 

        # 5. TAREFA SÍNCRONA (Foreground)
        # Esta parte roda independentemente da lógica de lock (sempre damos uma resposta)
        logging.info("Executando Fallback Síncrono (Modo 1) para responder ao usuário...")
        fallback = FallbackExtractor()
        extracted_data = fallback.extract_all(schema, pdf_text)
        
        if not extracted_data:
            logging.error("Falha Síncrona: Fallback (Modo 1) também falhou.")
            return {"error": "Falha na extração de fallback."}
        
        logging.info("Fallback Síncrono concluído. Retornando dados ao usuário.")
        return extracted_data

# --- SIMULAÇÃO DE FLUXO (Exatamente o mesmo de antes) ---
if __name__ == "__main__":
    logging.info("--- INICIANDO SIMULAÇÃO DE FLUXO (V2 ASYNC) ---")

    # Carregando dados do EXEMPLO 1 do desafio
    label_teste = "carteira_oab"
    
    pdf_texto_exemplo1 = """
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
    
    schema_exemplo1 = {
      "nome": "Nome do profissional, normalmente no canto superior esquerdo da imagem",
      "inscricao": "Número de inscrição do profissional",
      "seccional": "Seccional do profissional",
      "subsecao": "Subseção à qual o profissional faz parte",
      "categoria": "Categoria, pode ser ADVOGADO, ADVOGADA, SUPLEMENTAR, ESTAGIARIO, ESTAGIARIA",
      "telefone_profissional": "Telefone do profissional",
      "situacao": "Situação do profissional, normalmente no canto inferior direito."
    }

    # --- TESTE 1: CACHE MISS (Força o Fluxo Assíncrono) ---
    print("\n\n--- [TESTE 1: CACHE MISS (V2)] ---")
    print("Limpando cache (se existir) para forçar o Módulo 1 (Geração) em BACKGROUND...")
    
    repo_para_limpar = ParserRepository()
    parser_path = repo_para_limpar._get_parser_filepath(label_teste)
    if os.path.exists(parser_path):
        os.remove(parser_path)
        logging.info(f"Cache antigo '{parser_path}' removido.")
    
    dados_teste_1 = processar_extracao(label_teste, schema_exemplo1, pdf_texto_exemplo1.strip())
    print(f"--- RESULTADO (TESTE 1 - DO FALLBACK SÍNCRONO): ---\n{json.dumps(dados_teste_1, indent=2, ensure_ascii=False)}\n")
    print("... A GERAÇÃO DO PARSER (M1) PODE ESTAR RODANDO EM BACKGROUND AGORA ...")

    # (Adicionamos uma pequena espera para dar tempo da thread terminar
    #  antes de rodar o teste 2, apenas para fins de simulação)
    import time
    print("... Aguardando a tarefa de background (M1) terminar (simulação)...")
    # (Em um servidor real, a thread continuaria rodando
    #  independentemente desta simulação)
    # Vamos esperar um pouco mais que os 55s do seu teste anterior
    # time.sleep(60) 
    # (Comente o sleep(60) se não quiser esperar, mas o Teste 2 pode falhar se a thread não terminar)


    # --- TESTE 2: CACHE HIT (O Caminho Rápido) ---
    print("\n\n--- [TESTE 2: CACHE HIT (V2)] ---")
    print("Executando 2ª vez. Se a thread de background terminou, deve dar CACHE HIT.")
    
    dados_teste_2 = processar_extracao(label_teste, schema_exemplo1, pdf_texto_exemplo1.strip())
    print(f"--- RESULTADO (TESTE 2 - DO MÓDULO 2): ---\n{json.dumps(dados_teste_2, indent=2, ensure_ascii=False)}\n")
    
    print("\n--- SIMULAÇÃO CONCLUÍDA ---")