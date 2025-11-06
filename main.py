import logging
import json 
import os
import threading
import time
import fitz 

# --- Importando todos os nossos Módulos ---
from parser_repository import ParserRepository
from parser_generator import ParserGenerator         
from parser_executor import ParserExecutor           
from confidence_calculator import ConfidenceCalculator 
from fallback_extractor import FallbackExtractor     

# Configuração inicial de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MIN_CONFIDENCE_THRESHOLD = 0.8 

def ler_texto_do_pdf(pdf_path: str) -> str | None:
    """
    Extrai o texto de um arquivo PDF usando PyMuPDF (fitz).
    """
    full_path = os.path.join("files", pdf_path)
    
    if not os.path.exists(full_path):
        logging.error(f"Arquivo PDF não encontrado em: {full_path}")
        return None
    
    try:
        with fitz.open(full_path) as doc:
            texto_completo = ""
            for page in doc:
                texto_completo += page.get_text()
            return texto_completo
    except Exception as e:
        logging.error(f"Falha ao ler o PDF {full_path}: {e}")
        return None

#
# --- FUNÇÃO PRINCIPAL (V16) ---
#
def processar_extracao(label: str, 
                       item_schema: dict, 
                       pdf_text: str,
                       merged_schemas_map: dict): # <-- 1. Trazemos o mapa de schemas V14
    """
    Função principal do Orquestrador (V16).
    Passa o resultado do Fallback (Modo 1) para o Gerador (Modo 1).
    """
    logging.info(f"Iniciando processamento (V2.1 Lock) para o label: {label}")
    
    repo = ParserRepository()
    parser = repo.get_parser(label)

    if parser:
        # --- CAMINHO 1: CACHE HIT (V14) ---
        # (Esta lógica de Confiança-Primeiro está estável e correta)
        logging.info("CACHE HIT. Acionando Módulo 2 (Executor)...")
        executor = ParserExecutor()
        
        extracted_data_COMPLETO = executor.execute_parser(parser, pdf_text)
        
        logging.info("--- DADOS EXTRAÍDOS (Resultado Módulo 2 - Completo) ---")
        logging.info(json.dumps(extracted_data_COMPLETO, indent=2, ensure_ascii=False))

        schema_COMPLETO = merged_schemas_map[label] # [V14]

        calculator = ConfidenceCalculator()
        confidence = calculator.calculate_confidence(extracted_data_COMPLETO, schema_COMPLETO)
        
        extracted_data_FILTRADO = {
            k: extracted_data_COMPLETO.get(k) for k in item_schema.keys()
        }
        
        logging.info("--- DADOS EXTRAÍDOS (Filtrado para este Item) ---")
        logging.info(json.dumps(extracted_data_FILTRADO, indent=2, ensure_ascii=False))

        if confidence >= MIN_CONFIDENCE_THRESHOLD:
            logging.info(f"Confiança Alta ({confidence:.2f} >= {MIN_CONFIDENCE_THRESHOLD}). Retornando dados do Módulo 2 (Filtrados).")
            return extracted_data_FILTRADO
        else:
            logging.warning(f"Confiança Baixa ({confidence:.2f} < {MIN_CONFIDENCE_THRESHOLD}). Acionando Fallback Otimizado (Modo 2)...")
            fallback = FallbackExtractor()
            
            campos_faltantes = {
                k: v for k, v in item_schema.items() 
                if k not in extracted_data_FILTRADO or not extracted_data_FILTRADO[k]
            }
            if not campos_faltantes:
                 logging.warning("Confiança baixa em campos não solicitados. Retornando dados filtrados.")
                 return extracted_data_FILTRADO 

            fallback_data = fallback.extract_missing(campos_faltantes, pdf_text, extracted_data_FILTRADO)
            
            if fallback_data:
                final_data = extracted_data_FILTRADO.copy(); final_data.update(fallback_data)
                return final_data
            else:
                return extracted_data_FILTRADO
    
    else:
        # --- CAMINHO 2: CACHE MISS (V16) ---
        logging.warning(f"CACHE MISS para {label}. Verificando lock de geração...")
        
        # 5. TAREFA SÍNCRONA (Foreground) - EXECUTADA PRIMEIRO
        #    Precisamos do resultado dela ANTES de disparar a thread.
        logging.info("Executando Fallback Síncrono (Modo 1) para responder ao usuário...")
        fallback = FallbackExtractor()
        
        # O Fallback Síncrono (Modo 1) usa o schema PARCIAL do item
        dados_de_fallback_sincrono = fallback.extract_all(item_schema, pdf_text)
        
        if not dados_de_fallback_sincrono:
            logging.error("Falha Síncrona: Fallback (Modo 1) também falhou.")
            return {"error": "Falha na extração de fallback."}
        
        logging.info("Fallback Síncrono concluído. Preparando job de geração em background...")

        # 1. VERIFICAR O LOCK
        if repo.is_generation_locked(label):
            logging.warning(f"Geração para '{label}' já em progresso (lock encontrado). Pulando criação de nova thread.")
        else:
            logging.info(f"Lock não encontrado. Criando lock e disparando thread de geração...")
            repo.create_lock(label)

            # 2. DEFINIÇÃO DA TAREFA ASSÍNCRONA (Background) [MODIFICADA]
            def _run_parser_generation_task(schema_completo, seed_pdf_text, seed_json_output):
                task_repo = ParserRepository()
                task_generator = ParserGenerator()
                try:
                    logging.info(f"[BACKGROUND] TAREFA INICIADA: Gerando parser para '{label}'...")
                    
                    # Passamos o schema completo E o exemplo de "gabarito"
                    new_parser = task_generator.generate_parser(
                        schema=schema_completo, 
                        pdf_text=seed_pdf_text,
                        correct_json_example=seed_json_output
                    )
                    
                    if new_parser:
                        task_repo.save_parser(label, new_parser)
                        logging.info(f"[BACKGROUND] TAREFA CONCLÍDA: Novo parser para '{label}' salvo.")
                    else:
                        logging.error(f"[BACKGROUND] TAREFA FALHOU: Módulo 1 falhou em gerar parser para '{label}'.")
                
                except Exception as e:
                    logging.error(f"[BACKGROUND] TAREFA CRASHOU: {e}")
                finally:
                    logging.info(f"[BACKGROUND] Removendo lock para '{label}'...")
                    task_repo.remove_lock(label)

            # 3. DISPARAR A THREAD [MODIFICADO]
            #    Passamos os dados necessários como argumentos para a thread
            schema_completo_mesclado = merged_schemas_map[label]
            
            generation_thread = threading.Thread(
                target=_run_parser_generation_task,
                args=(
                    schema_completo_mesclado, # O schema completo que queremos gerar
                    pdf_text,                 # O texto que gerou o JSON
                    dados_de_fallback_sincrono # O JSON de gabarito
                )
            )
            generation_thread.start() 

        # 4. RETORNAR OS DADOS SÍNCRONOS IMEDIATAMENTE
        logging.info("Retornando dados do Fallback Síncrono ao usuário.")
        return dados_de_fallback_sincrono

#
# --- (A FUNÇÃO pre_scan... É REVERTIDA PARA V14) ---
#
def pre_scan_e_mesclar_schemas(batch_data: list) -> dict:
    """
    FASE 1 (V14): Itera por todo o batch ANTES do processamento
    para construir um mapa de schemas mesclados.
    """
    logging.info("--- FASE 1: Pré-Scan e Mesclagem de Schemas ---")
    merged_schemas_map = {}
    
    for item in batch_data:
        label = item.get("label")
        schema = item.get("extraction_schema")
        
        if not label or not schema:
            logging.warning("Item no dataset sem 'label' ou 'extraction_schema'. Pulando.")
            continue
            
        if label not in merged_schemas_map:
            # Primeiro vez que vemos esse label, apenas copiamos o schema
            merged_schemas_map[label] = schema.copy()
        else:
            # Já vimos esse label, mesclamos os campos (chaves)
            merged_schemas_map[label].update(schema)
            
    logging.info(f"Pré-Scan concluído. {len(merged_schemas_map)} schemas únicos mesclados.")
    
    # Log para depuração
    for label, schema in merged_schemas_map.items():
        logging.info(f"  Schema mesclado para '{label}': {len(schema)} campos únicos.")
        
    return merged_schemas_map

#
# --- (A FUNÇÃO processar_batch_serial É MODIFICADA) ---
#
def processar_batch_serial(batch_data: list, merged_schemas_map: dict):
    """
    FASE 3 (V16): Processa o batch serialmente, item por item,
    lendo os PDFs reais.
    """
    logging.info("--- FASE 3: Iniciando Processamento Serial do Batch ---")
    
    start_time_total = time.time()
    resultados_finais = []

    for i, item in enumerate(batch_data):
        logging.info(f"--- Processando Item {i+1}/{len(batch_data)} ---")
        item_label = item.get("label")
        item_schema = item.get("extraction_schema")
        pdf_path = item.get("pdf_path") #
        
        pdf_text = ler_texto_do_pdf(pdf_path) 
        
        if not all([item_label, item_schema, pdf_text]):
            logging.error(f"Item {i+1} inválido (label, schema ou texto do PDF ausente). Pulando.")
            continue
            
        start_time_item = time.time()
        
        # Chamamos o orquestrador V16
        resultado = processar_extracao(
            label=item_label,
            item_schema=item_schema,
            pdf_text=pdf_text,
            merged_schemas_map=merged_schemas_map # Passa o mapa V14
        )
        
        resultados_finais.append(resultado)
        
        # ... (Resto da lógica de logging de tempo é idêntica) ...
        tempo_item = time.time() - start_time_item
        tempo_acumulado = time.time() - start_time_total
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
    time.sleep(10) 
    while threading.active_count() > 1: 
        time.sleep(2)
    logging.info("Todas as threads concluídas. Simulação finalizada.")

#
# --- (A FUNÇÃO carregar_dataset FICA AQUI, IDÊNTICA) ---
#
def carregar_dataset(filepath="dataset.json") -> list:
    """
    Carrega o arquivo dataset.json.
    """
    if not os.path.exists(filepath):
        logging.error(f"Arquivo do dataset não encontrado em: {filepath}")
        return []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            dataset = json.load(f)
        
        if isinstance(dataset, list):
            logging.info(f"Dataset carregado com sucesso. {len(dataset)} itens encontrados.")
            return dataset
        
        logging.error(f"Formato do dataset.json inesperado. Esperava uma Lista []. Encontrado: {type(dataset)}")
        return []

    except json.JSONDecodeError:
        logging.error(f"Falha ao decodificar o {filepath}. Verifique se é um JSON válido.")
        return []
    except Exception as e:
        logging.error(f"Erro ao carregar o dataset: {e}")
        return []

#
# --- (A FUNÇÃO if __name__ == "__main__": FICA AQUI, IDÊNTICA) ---
#
if __name__ == "__main__":
    logging.info("--- INICIANDO SIMULAÇÃO DE BATCH (V9 + V2.1) ---")

    repo_para_limpar = ParserRepository()
    repo_para_limpar.limpar_cache_completo() 


    batch_data = carregar_dataset("dataset.json")

    if not batch_data:
        logging.error("Simulação interrompida. Dataset não pôde ser carregado.")
    else:
        # --- FASE 1 (V14) ---
        merged_schemas_map = pre_scan_e_mesclar_schemas(batch_data)
        
        # --- FASE 3 (V16) ---
        processar_batch_serial(batch_data, merged_schemas_map)