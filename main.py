# Módulo: main.py
# (Arquitetura V18.2: Fallback Heurístico + Geração Tripla)

import logging
import json 
import os
import threading
import time
import fitz 
from queue import Queue

# --- Importando todos os nossos Módulos V18.2 ---
from parser_repository import ParserRepository         # (V16 - Mantido)
from parser_generator import ParserGenerator           # (V18.2 - Novo)
from validation_generator import ValidationGenerator   # (V18.2 - Novo)
from parser_executor import ParserExecutor             # (V16 - Mantido)
from confidence_calculator import ConfidenceCalculator # (V18 - Mantido)
from fallback_extractor import FallbackExtractor       # (V16 - Mantido)
from heuristic_extractor import HeuristicExtractor     # (V18.1 - Mantido)

# Configuração inicial de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MIN_CONFIDENCE_THRESHOLD = 0.8 

LLM_TIMEOUT_SECONDS = 9.9 # Nosso timeout global

def _run_llm_extract_missing_in_thread(queue: Queue, 
                                       missing_schema: dict, 
                                       pdf_text: str, 
                                       partial_data: dict):
    """
    Executa o 'extract_missing' (Modo 2) em uma thread
    para podermos aplicar o timeout.
    """
    try:
        # FALLBACK é o nosso singleton FallbackExtractor
        result = FALLBACK.extract_missing(missing_schema, pdf_text, partial_data)
        queue.put(result)
    except Exception as e:
        logging.error(f"THREAD LLM (extract_missing) FALHOU: {e}")
        queue.put(None) # Sinaliza falha

# --- CARREGAMENTO SINGLETON DOS MÓDULOS ---
try:
    logging.info("Carregando módulos (Singleton)...")
    REPO = ParserRepository()
    EXECUTOR = ParserExecutor()
    CALCULATOR = ConfidenceCalculator()
    FALLBACK = FallbackExtractor()
    HEURISTIC_FALLBACK = HeuristicExtractor()
    PARSER_GENERATOR = ParserGenerator() 
    VALIDATION_GENERATOR = ValidationGenerator()
    logging.info("Módulos V18.2 carregados.")
except Exception as e:
    logging.critical(f"Falha ao carregar módulos. Encerrando. Erro: {e}")
    exit(1)
# ----------------------------------------

def ler_texto_do_pdf(pdf_path: str) -> str | None:
    """ Extrai o texto de um arquivo PDF (Mantido). """
    full_path = os.path.join("files", pdf_path)
    if not os.path.exists(full_path):
        logging.error(f"Arquivo PDF não encontrado em: {full_path}")
        return None
    try:
        with fitz.open(full_path) as doc:
            return "".join(page.get_text() for page in doc)
    except Exception as e:
        logging.error(f"Falha ao ler o PDF {full_path}: {e}")
        return None

def _run_parser_generation_task(label: str, 
                                schema_completo: dict, 
                                seed_pdf_text: str):
    """
    THREAD DE BACKGROUND (V18.2)
    
    Orquestra a pipeline de 3 chamadas de LLM para gerar
    o pacote de 'conhecimento'.
    """
    try:
        logging.info(f"[BACKGROUND] TAREFA INICIADA: Gerando pacote V18.2 para '{label}'...")
        
        # --- CHAMADA 1: OBTER O GABARITO ---
        logging.info(f"[BACKGROUND] (1/3) Obtendo 'gabarito' via FallbackExtractor...")
        gabarito = FALLBACK.extract_all(schema_completo, seed_pdf_text)
        
        if not gabarito:
            logging.error(f"[BACKGROUND] Falha ao obter gabarito. Abortando geração.")
            return

        # --- CHAMADA 2: GERAR O PARSER ---
        logging.info(f"[BACKGROUND] (2/3) Gerando 'parser' via ParserGenerator...")
        parser_rules = PARSER_GENERATOR.generate_parser(
            schema=schema_completo,
            pdf_text=seed_pdf_text,
            correct_json_example=gabarito
        )
        if not parser_rules:
            logging.error(f"[BACKGROUND] Falha ao gerar parser. Abortando geração.")
            return

        # --- CHAMADA 3: GERAR AS REGRAS ---
        logging.info(f"[BACKGROUND] (3/3) Gerando 'validation_rules' via ValidationGenerator...")
        validation_rules = VALIDATION_GENERATOR.generate_rules(
            schema=schema_completo,
            correct_json_example=gabarito
        )
        if not validation_rules:
            logging.error(f"[BACKGROUND] Falha ao gerar validation_rules. Abortando geração.")
            return

        # --- SUCESSO: Combinar e Salvar o Pacote ---
        new_bundle = {
            "parser": parser_rules,
            "validation_rules": validation_rules
        }
        
        REPO.save_parser(label, new_bundle)
        logging.info(f"[BACKGROUND] TAREFA CONCLÍDA: Novo pacote V18.2 para '{label}' salvo.")
    
    except Exception as e:
        logging.error(f"[BACKGROUND] TAREFA CRASHOU: {e}")
    finally:
        logging.info(f"[BACKGROUND] Removendo lock para '{label}'...")
        REPO.remove_lock(label)


def pre_scan_e_mesclar_schemas(batch_data: list) -> dict:
    """ (Mantido da V16) """
    logging.info("--- FASE 1: Pré-Scan e Mesclagem de Schemas ---")
    merged_schemas_map = {}
    for item in batch_data:
        label = item.get("label")
        schema = item.get("extraction_schema")
        if label and schema:
            if label not in merged_schemas_map:
                merged_schemas_map[label] = schema.copy()
            else:
                merged_schemas_map[label].update(schema)
    logging.info(f"Pré-Scan concluído. {len(merged_schemas_map)} schemas únicos mesclados.")
    return merged_schemas_map

#
# --- FUNÇÃO PRINCIPAL (V18.2 - Idêntica à V18.1) ---
#
def processar_extracao(label: str, 
                       item_schema: dict, 
                       pdf_text: str,
                       merged_schemas_map: dict,
                       item_index: int, # NOVO: O índice do item (ex: 0, 1, 2...)
                       batch_start_time: float # NOVO: O time.time() de início do lote
                       ) -> tuple[dict, float]: # Retorna os dados E o tempo do item
    """
    Orquestrador V21.1
    (V18.2 + Gerador V19.2 + Watchdog CUMULATIVO V21.1)
    """
    logging.info(f"Iniciando processamento (V21.1) para o label: {label}")
    start_time_item = time.time() # Início do processamento deste item
    
    bundle = REPO.get_parser(label)

    if bundle:
        # --- CAMINHO 2: CACHE HIT (V21.1) ---
        logging.info("CACHE HIT (V21). Acionando Módulo 2 (Executor)...")
        
        parser_rules = bundle.get("parser", {})
        validation_rules = bundle.get("validation_rules", {})
        
        extracted_data = EXECUTOR.execute_parser(parser_rules, pdf_text)
        
        logging.info("--- DADOS EXTRAÍDOS (Resultado Módulo 2 - Parser V21) ---")
        logging.info(json.dumps(extracted_data, indent=2, ensure_ascii=False))

        confidence = CALCULATOR.calculate_confidence(extracted_data, validation_rules)

        final_data = {
            k: extracted_data.get(k) for k in item_schema.keys()
        }

        if confidence >= MIN_CONFIDENCE_THRESHOLD:
            logging.info(f"Confiança Alta ({confidence:.2f}). Retornando dados do Parser V21.")
            return final_data, (time.time() - start_time_item)
        else:
            # MÓDULO 4 (V21.1) - Fallback Otimizado com WATCHDOG CUMULATIVO
            logging.warning(f"Confiança Baixa ({confidence:.2f}). Acionando Fallback Otimizado (Modo 2)...")
            
            campos_faltantes = {
                k: v for k, v in item_schema.items() 
                if k not in final_data or not final_data[k]
            }
            if not campos_faltantes:
                 return final_data, (time.time() - start_time_item)

            # --- LÓGICA DO WATCHDOG V21.1 (CUMULATIVO) ---
            q = Queue()
            llm_thread = threading.Thread(
                target=_run_llm_extract_missing_in_thread,
                args=(q, campos_faltantes, pdf_text, final_data)
            )
            
            # --- CÁLCULO DE TIMEOUT CUMULATIVO ---
            # O item atual é (item_index + 1). Ex: item 0 é o 1º item.
            # O orçamento de tempo total para ESTE item é (item_index + 1) * 10.0 segundos.
            absolute_deadline = batch_start_time + (item_index + 1) * 10.0
            
            # O tempo restante que temos é o deadline menos o tempo atual
            current_time = time.time()
            time_remaining = absolute_deadline - current_time
            
            # Deixar um buffer de segurança (ex: 0.5s)
            safety_buffer = 0.5
            timeout_budget = max(0.1, time_remaining - safety_buffer) 
            # ----------------------------------------
            
            llm_thread.start()
            
            try:
                logging.info(f"Tentando extração LLM 'missing' com timeout CUMULATIVO de {timeout_budget:.2f}s...")
                fallback_data = q.get(timeout=timeout_budget)
                llm_thread.join()
                
                if fallback_data:
                    logging.info("Sucesso no Fallback LLM (Modo 2).")
                    final_data.update(fallback_data)
                else:
                    logging.warning("Fallback LLM (Modo 2) falhou. Retornando dados parciais.")
                    
            except: # (Timeout)
                logging.critical(f"TIMEOUT CUMULATIVO DE {timeout_budget:.2f}s ATINGIDO no Modo 2! Retornando dados parciais.")
            
            return final_data, (time.time() - start_time_item)
    
    else:
        # --- CAMINHO 1: CACHE MISS (V21.1) ---
        logging.warning(f"CACHE MISS (V21) para {label}. Verificando lock...")
        
        logging.info("Executando Fallback Síncrono Local (Heurístico V18.3)...")
        heuristic_data = HEURISTIC_FALLBACK.extract(pdf_text, item_schema)
        
        if not REPO.is_generation_locked(label):
            logging.info(f"Disparando thread de geração de pacote V21 (Híbrido)...")
            REPO.create_lock(label)
            
            generation_thread = threading.Thread(
                target=_run_parser_generation_task,
                args=(
                    label,
                    merged_schemas_map[label], 
                    pdf_text
                )
            )
            generation_thread.start() 
        else:
            logging.warning(f"Geração para '{label}' já em progresso. Pulando.")

        logging.info("Retornando dados do Fallback Heurístico (V18.3) ao usuário.")
        return heuristic_data, (time.time() - start_time_item)

#
# --- (O resto do main.py (V18.1) é idêntico) ---
#

def processar_batch_serial(batch_data: list, merged_schemas_map: dict):
    """
    FASE 3 (V21.1): Processa o batch serialmente e passa os dados de tempo.
    """
    logging.info("--- FASE 3: Iniciando Processamento Serial do Batch (V21.1) ---")
    start_time_total = time.time() # O início do LOTE

    for i, item in enumerate(batch_data):
        logging.info(f"--- Processando Item {i+1}/{len(batch_data)} ---")
        item_label = item.get("label")
        item_schema = item.get("extraction_schema")
        pdf_path = item.get("pdf_path")
        
        pdf_text = ler_texto_do_pdf(pdf_path) 
        
        if not all([item_label, item_schema, pdf_text]):
            logging.error(f"Item {i+1} inválido. Pulando.")
            continue
        
        # Chama a extração passando o índice (i) e o tempo de início do lote
        resultado, tempo_item = processar_extracao(
            label=item_label,
            item_schema=item_schema,
            pdf_text=pdf_text,
            merged_schemas_map=merged_schemas_map,
            item_index=i, # Passa o índice
            batch_start_time=start_time_total # Passa o tempo de início
        )
        
        tempo_acumulado = time.time() - start_time_total
        # O limite de tempo para ESTE ponto no lote
        limite_item_n = (i + 1) * 10.0 
        
        logging.info(f"--- Item {i+1} Processado ---")
        logging.info(f"Dados Finais: {json.dumps(resultado, indent=2, ensure_ascii=False)}")
        logging.info(f"Tempo do Item: {tempo_item:.2f}s")
        
        if tempo_acumulado <= limite_item_n:
            logging.info(f"Tempo Acumulado: {tempo_acumulado:.2f}s. Limite: {limite_item_n:.2f}s. ... OK.")
        else:
            logging.critical(f"Tempo Acumulado: {tempo_acumulado:.2f}s. Limite: {limite_item_n:.2f}s. ... FALHA NO REQUISITO DE TEMPO!")

    logging.info("--- Processamento do Batch Concluído ---")
    tempo_total = time.time() - start_time_total
    logging.info(f"Tempo total para {len(batch_data)} itens: {tempo_total:.2f}s")
    
    logging.info("Aguardando threads de geração pendentes (se houver)...")
    # (Resto da sua lógica de thread)


def carregar_dataset(filepath="dataset.json") -> list:
    """ (Mantido da V16) """
    logging.info(f"Carregando dataset de {filepath}...")
    if not os.path.exists(filepath):
        logging.error(f"Arquivo do dataset não encontrado em: {filepath}")
        return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            dataset = json.load(f)
        logging.info(f"Dataset carregado com sucesso. {len(dataset)} itens encontrados.")
        return dataset
    except Exception as e:
        logging.error(f"Erro ao carregar o dataset: {e}")
        return []

if __name__ == "__main__":
    logging.info("--- INICIANDO SIMULAÇÃO DE BATCH (V18.2) ---")

    # REPO.limpar_cache_completo()

    batch_data = carregar_dataset("dataset.json")

    if not batch_data:
        logging.error("Simulação interrompida. Dataset não pôde ser carregado.")
    else:
        merged_schemas_map = pre_scan_e_mesclar_schemas(batch_data)
        processar_batch_serial(batch_data, merged_schemas_map)