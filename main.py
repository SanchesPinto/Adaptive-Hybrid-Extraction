import logging
import json 
import os
import threading
import time 
import fitz # PyMuPDF

# --- Importando todos os nossos Módulos ---
# (Sem alterações aqui)
from parser_repository import ParserRepository
from parser_generator import ParserGenerator         
from parser_executor import ParserExecutor           
from confidence_calculator import ConfidenceCalculator 
from fallback_extractor import FallbackExtractor     

# Configuração inicial de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MIN_CONFIDENCE_THRESHOLD = 0.8 

# --- NOVA FUNÇÃO HELPER ---
def ler_texto_do_pdf(pdf_path: str) -> str | None:
    """
    Extrai o texto de um arquivo PDF usando PyMuPDF (fitz).
    Assume que o PDF já tem texto (conforme ).
    """
    # Constrói o caminho completo, assumindo que a pasta 'files' está no mesmo nível
    full_path = os.path.join("files", pdf_path) # (baseado na sua 'tree')
    
    if not os.path.exists(full_path):
        logging.error(f"Arquivo PDF não encontrado em: {full_path}")
        return None
    
    try:
        with fitz.open(full_path) as doc:
            texto_completo = ""
            for page in doc:
                # O desafio diz que cada PDF tem apenas uma página 
                texto_completo += page.get_text()
            return texto_completo
    except Exception as e:
        logging.error(f"Falha ao ler o PDF {full_path}: {e}")
        return None

#
# --- MODIFICAÇÃO 1: Passamos a receber o 'merged_schema_map' ---
#
def processar_extracao(label: str, 
                       item_schema: dict, 
                       pdf_text: str,
                       merged_data_map: dict): # <-- 1. Nome do parâmetro mudou
    """
    Função principal do Orquestrador (Módulo 4), V15.
    Agora usa o 'merged_data_map' (schema E textos).
    """
    logging.info(f"Iniciando processamento (V2.1 Lock) para o label: {label}")
    
    repo = ParserRepository()
    parser = repo.get_parser(label)

    if parser:
        # --- CAMINHO 1: CACHE HIT ---
        # (A lógica V14 que implementamos está perfeita, 
        #  só precisamos garantir que ela receba o 'merged_data_map')
        
        logging.info("CACHE HIT. Acionando Módulo 2 (Executor)...")
        executor = ParserExecutor()
        
        extracted_data_COMPLETO = executor.execute_parser(parser, pdf_text)
        
        logging.info("--- DADOS EXTRAÍDOS (Resultado Módulo 2 - Completo) ---")
        logging.info(json.dumps(extracted_data_COMPLETO, indent=2, ensure_ascii=False))

        # Usamos o 'merged_data_map' para pegar o schema completo para validação
        schema_COMPLETO = merged_data_map[label]['schema'] # [V14]

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
        # --- CAMINHO 2: CACHE MISS (V2.1 com Lock) ---
        logging.warning(f"CACHE MISS para {label}. Verificando lock de geração...")

        if repo.is_generation_locked(label):
            logging.warning(f"Geração para '{label}' já em progresso (lock encontrado). Pulando criação de nova thread.")
        else:
            logging.info(f"Lock não encontrado. Criando lock e disparando thread de geração...")
            repo.create_lock(label)

            # 3. TAREFA ASSÍNCRONA (Background) [MODIFICADA]
            def _run_parser_generation_task():
                task_repo = ParserRepository()
                task_generator = ParserGenerator()
                try:
                    logging.info(f"[BACKGROUND] TAREFA INICIADA: Gerando parser para '{label}'...")
                    
                    # --- AQUI ESTÁ A MUDANÇA ---
                    # Pegamos o schema mesclado E os textos agregados
                    dados_completos_label = merged_data_map[label]
                    schema_completo = dados_completos_label['schema']
                    textos_agregados = dados_completos_label['text_examples']
                    
                    # Passamos AMBOS para o gerador
                    new_parser = task_generator.generate_parser(schema_completo, textos_agregados)
                    
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

            generation_thread = threading.Thread(target=_run_parser_generation_task)
            generation_thread.start() 

        # 5. TAREFA SÍNCRONA (Foreground)
        # (Esta parte não muda, continua perfeita)
        logging.info("Executando Fallback Síncrono (Modo 1) para responder ao usuário...")
        fallback = FallbackExtractor()
        extracted_data = fallback.extract_all(item_schema, pdf_text)
        
        if not extracted_data:
            logging.error("Falha Síncrona: Fallback (Modo 1) também falhou.")
            return {"error": "Falha na extração de fallback."}
        
        logging.info("Fallback Síncrono concluído. Retornando dados ao usuário.")
        return extracted_data

#
# --- NOVA FUNÇÃO (FASE 1: PRÉ-SCAN) ---
#
def pre_scan_e_agregar_dados(batch_data: list) -> dict:
    """
    FASE 1 (V15): Itera por todo o batch ANTES do processamento
    para construir um mapa que contém:
    1. O schema mesclado.
    2. Uma string agregada de TODOS os textos de exemplo para esse label.
    """
    logging.info("--- FASE 1: Pré-Scan, Mesclagem de Schemas e Agregação de Textos ---")
    merged_data_map = {}
    
    # Precisamos ler os textos dos PDFs primeiro
    textos_por_path = {}
    for item in batch_data:
        pdf_path = item.get("pdf_path")
        if pdf_path and pdf_path not in textos_por_path:
            textos_por_path[pdf_path] = ler_texto_do_pdf(pdf_path)

    for item in batch_data:
        label = item.get("label")
        schema = item.get("extraction_schema")
        pdf_path = item.get("pdf_path")
        
        if not all([label, schema, pdf_path]):
            logging.warning("Item no dataset sem 'label', 'schema' ou 'pdf_path'. Pulando.")
            continue
            
        pdf_text = textos_por_path.get(pdf_path)
        if not pdf_text:
            logging.warning(f"Texto para {pdf_path} não encontrado. Pulando item.")
            continue

        if label not in merged_data_map:
            # Primeira vez que vemos esse label
            merged_data_map[label] = {
                'schema': schema.copy(),
                'text_examples': f"--- INÍCIO EXEMPLO ({pdf_path}) ---\n{pdf_text}\n--- FIM EXEMPLO ({pdf_path}) ---\n\n"
            }
        else:
            # Já vimos esse label, mesclamos
            merged_data_map[label]['schema'].update(schema)
            # E concatenamos o texto do exemplo
            merged_data_map[label]['text_examples'] += f"--- INÍCIO EXEMPLO ({pdf_path}) ---\n{pdf_text}\n--- FIM EXEMPLO ({pdf_path}) ---\n\n"
            
    logging.info(f"Pré-Scan concluído. {len(merged_data_map)} labels únicos processados.")
    
    for label, data in merged_data_map.items():
        logging.info(f"  Label '{label}': {len(data['schema'])} campos únicos. {len(data['text_examples'])} caracteres de texto de exemplo.")
        
    return merged_data_map

#
# --- NOVA FUNÇÃO (FASE 3: PROCESSAMENTO SERIAL) ---
#
def processar_batch_serial(batch_data: list, merged_data_map: dict):
    """
    FASE 3 (V15): Processa o batch serialmente, item por item,
    lendo os PDFs reais.
    """
    logging.info("--- FASE 3: Iniciando Processamento Serial do Batch ---")
    
    start_time_total = time.time()
    resultados_finais = []

    for i, item in enumerate(batch_data):
        logging.info(f"--- Processando Item {i+1}/{len(batch_data)} ---")
        item_label = item.get("label")
        item_schema = item.get("extraction_schema")
        pdf_path = item.get("pdf_path")
        
        pdf_text = ler_texto_do_pdf(pdf_path)
        
        if not all([item_label, item_schema, pdf_text]):
            logging.error(f"Item {i+1} inválido (label, schema ou texto do PDF ausente). Pulando.")
            continue
            
        start_time_item = time.time()
        
        # Chamamos o orquestrador V15
        resultado = processar_extracao(
            label=item_label,
            item_schema=item_schema,
            pdf_text=pdf_text,
            merged_data_map=merged_data_map # Passa o mapa completo
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

if __name__ == "__main__":
    logging.info("--- INICIANDO SIMULAÇÃO DE BATCH (V9 + V2.1) ---")

    #repo_para_limpar = ParserRepository()
    #repo_para_limpar.limpar_cache_completo() 

    batch_data = carregar_dataset("dataset.json")

    if not batch_data:
        logging.error("Simulação interrompida. Dataset não pôde ser carregado.")
    else:
        # --- FASE 1 (V15) ---
        merged_data_map = pre_scan_e_agregar_dados(batch_data)
        
        # --- FASE 3 (V15) ---
        processar_batch_serial(batch_data, merged_data_map)