import logging
import json 
import os
import threading
import time # Usaremos para medir o tempo total

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

#
# --- MODIFICAÇÃO 1: Passamos a receber o 'merged_schema_map' ---
#
def processar_extracao(label: str, 
                       item_schema: dict, 
                       pdf_text: str,
                       merged_schemas_map: dict):
    """
    Função principal do Orquestrador (Módulo 4), V2.1 + V9.
    [CORRIGIDA com a lógica de Filtro-Primeiro]
    """
    logging.info(f"Iniciando processamento (V2.1 Lock) para o label: {label}")
    
    repo = ParserRepository()
    parser = repo.get_parser(label)

    if parser:
        # --- CAMINHO 1: CACHE HIT (O "Caminho Rápido") [CORRIGIDO] ---
        logging.info("CACHE HIT. Acionando Módulo 2 (Executor)...")
        executor = ParserExecutor()
        
        # 1. O Executor (Módulo 2) extrai o schema COMPLETO (ex: 8 campos)
        extracted_data_COMPLETO = executor.execute_parser(parser, pdf_text) #
        
        logging.info("--- DADOS EXTRAÍDOS (Resultado Módulo 2 - Completo) ---")
        logging.info(json.dumps(extracted_data_COMPLETO, indent=2, ensure_ascii=False))

        # 2. [A CORREÇÃO] Filtramos o resultado do parser PRIMEIRO
        #    para conter APENAS o que o `item_schema` pediu.
        extracted_data_FILTRADO = {
            k: extracted_data_COMPLETO.get(k) for k in item_schema.keys()
        }
        
        logging.info("--- DADOS EXTRAÍDOS (Filtrado para este Item) ---")
        logging.info(json.dumps(extracted_data_FILTRADO, indent=2, ensure_ascii=False))

        # 3. Agora calculamos a confiança no resultado FILTRADO.
        calculator = ConfidenceCalculator()
        confidence = calculator.calculate_confidence(extracted_data_FILTRADO, item_schema) #
        
        if confidence >= MIN_CONFIDENCE_THRESHOLD:
            logging.info(f"Confiança Alta ({confidence:.2f} >= {MIN_CONFIDENCE_THRESHOLD}). Retornando dados do Módulo 2.")
            return extracted_data_FILTRADO # Retorna os dados filtrados
        else:
            # 4. O Fallback Otimizado agora opera sobre os dados já filtrados.
            logging.warning(f"Confiança Baixa ({confidence:.2f} < {MIN_CONFIDENCE_THRESHOLD}). Acionando Fallback Otimizado (Modo 2)...")
            fallback = FallbackExtractor() #
            
            campos_faltantes = {
                k: v for k, v in item_schema.items() 
                if k not in extracted_data_FILTRADO or not extracted_data_FILTRADO[k]
            }
            if not campos_faltantes:
                 return extracted_data_FILTRADO # Retorna o melhor que temos

            # O contexto (partial_data) também deve ser o filtrado
            fallback_data = fallback.extract_missing(campos_faltantes, pdf_text, extracted_data_FILTRADO) #
            
            if fallback_data:
                final_data = extracted_data_FILTRADO.copy(); final_data.update(fallback_data)
                return final_data
            else:
                return extracted_data_FILTRADO
    
    else:
        # --- CAMINHO 2: CACHE MISS (V2.1 com Lock) ---
        # (Esta lógica está perfeita e não precisa de mudanças)
        
        logging.warning(f"CACHE MISS para {label}. Verificando lock de geração...")

        if repo.is_generation_locked(label): #
            logging.warning(f"Geração para '{label}' já em progresso (lock encontrado). Pulando criação de nova thread.")
        else:
            logging.info(f"Lock não encontrado. Criando lock e disparando thread de geração...")
            repo.create_lock(label) #

            def _run_parser_generation_task():
                task_repo = ParserRepository()
                task_generator = ParserGenerator() #
                try:
                    logging.info(f"[BACKGROUND] TAREFA INICIADA: Gerando parser para '{label}'...")
                    
                    schema_completo_mesclado = merged_schemas_map[label]
                    new_parser = task_generator.generate_parser(schema_completo_mesclado, pdf_text) #
                    
                    if new_parser:
                        task_repo.save_parser(label, new_parser) #
                        logging.info(f"[BACKGROUND] TAREFA CONCLÍDA: Novo parser para '{label}' salvo.")
                    else:
                        logging.error(f"[BACKGROUND] TAREFA FALHOU: Módulo 1 falhou em gerar parser para '{label}'.")
                
                except Exception as e:
                    logging.error(f"[BACKGROUND] TAREFA CRASHOU: {e}")
                finally:
                    logging.info(f"[BACKGROUND] Removendo lock para '{label}'...")
                    task_repo.remove_lock(label) #

            generation_thread = threading.Thread(target=_run_parser_generation_task)
            generation_thread.start() 

        logging.info("Executando Fallback Síncrono (Modo 1) para responder ao usuário...")
        fallback = FallbackExtractor() #
        
        # O Fallback Síncrono (Modo 1) já usa o item_schema, então está correto.
        extracted_data = fallback.extract_all(item_schema, pdf_text) #
        
        if not extracted_data:
            logging.error("Falha Síncrona: Fallback (Modo 1) também falhou.")
            return {"error": "Falha na extração de fallback."}
        
        logging.info("Fallback Síncrono concluído. Retornando dados ao usuário.")
        return extracted_data

#
# --- NOVA FUNÇÃO (FASE 1: PRÉ-SCAN) ---
#
def pre_scan_e_mesclar_schemas(batch_data: list) -> dict:
    """
    FASE 1: Itera por todo o batch ANTES do processamento
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
# --- NOVA FUNÇÃO (FASE 3: PROCESSAMENTO SERIAL) ---
#
def processar_batch_serial(batch_data: list, merged_schemas_map: dict):
    """
    FASE 3: Processa o batch serialmente, item por item,
    passando o mapa de schemas mesclados para o orquestrador.
    """
    logging.info("--- FASE 3: Iniciando Processamento Serial do Batch ---")
    
    start_time_total = time.time()
    resultados_finais = []
    
    # (Não estamos lendo o PDF real, então vamos usar um texto mockado
    #  baseado no pdf_path, apenas para a simulação funcionar)
    pdf_textos_mockados = {
        "oab_1.pdf": "SON GOKU\nInscrição 101943\nSeccional PR\nSubseção CONSELHO SECCIONAL-PARANÁ\nSUPLEMENTAR\nSITUAÇÃO REGULAR",
        "oab_2.pdf": "JOANA D'ARC\nInscrição 101943\nSeccional PR\nSubseção CONSELHO SECCIONAL-PARANA SUPLEMENTAR\nEndereco Profissional AVENIDA PAULISTA, N 2300\nSITUAÇÃO REGULAR",
        "oab_3.pdf": "VEGETA\nInscrição 123456\nSeccional SP\nSubseção CAPITAL\nADVOGADO\nTelefone Profissional (11) 99999-9999\nSITUAÇÃO REGULAR",
        "tela_sistema_1.pdf": "Data base 01/01/2025\nData verncimento 01/02/2025\nQuantidade parcelas 1\nProduto XYZ\nSistema XPTO\nTipo de operacao FINANC\nTipo de sistema INTERNO",
        "tela_sistema_2.pdf": "Pesquisa por: Cliente\nPesquisa tipo: CPF\nSistema XPTO\nValor parcela 150,00\nCidade RIO DE JANEIRO",
        "tela_sistema_3.pdf": "Data referencia 05/11/2025\nSeleção de parcelas: Pendente\nTotal de parcelas: 150,00",
    }

    for i, item in enumerate(batch_data):
        logging.info(f"--- Processando Item {i+1}/{len(batch_data)} ---")
        item_label = item.get("label")
        item_schema = item.get("extraction_schema")
        pdf_path = item.get("pdf_path")
        
        # (Substituir pela leitura real do PDF quando integrarmos)
        pdf_text = pdf_textos_mockados.get(pdf_path)
        
        if not all([item_label, item_schema, pdf_text]):
            logging.error(f"Item {i+1} inválido (faltando label, schema ou texto). Pulando.")
            continue
            
        start_time_item = time.time()
        
        # Chamamos o orquestrador V2.1 + V9
        resultado = processar_extracao(
            label=item_label,
            item_schema=item_schema,
            pdf_text=pdf_text,
            merged_schemas_map=merged_schemas_map
        )
        
        resultados_finais.append(resultado)
        
        tempo_item = time.time() - start_time_item
        tempo_acumulado = time.time() - start_time_total
        limite_item_n = (i + 1) * 10.0 # O limite de tempo serial 
        
        logging.info(f"--- Item {i+1} Processado ---")
        logging.info(f"Dados Finais: {json.dumps(resultado, indent=2, ensure_ascii=False)}")
        logging.info(f"Tempo do Item: {tempo_item:.2f}s")
        
        if tempo_acumulado <= limite_item_n:
            logging.info(f"Tempo Acumulado: {tempo_acumulado:.2f}s. Limite: {limite_item_n:.2f}s. ... OK.")
        else:
            logging.critical(f"Tempo Acumulado: {tempo_acumulado:.2f}s. Limite: {limite_item_n:.2f}s. ... FALHA NO REQUISITO DE TEMPO!")
            # (Em um sistema real, poderíamos parar, mas aqui continuamos)

    logging.info("--- Processamento do Batch Concluído ---")
    tempo_total = time.time() - start_time_total
    logging.info(f"Tempo total para {len(batch_data)} itens: {tempo_total:.2f}s")
    
    # (Aguardar threads de background para um shutdown limpo, se necessário)
    logging.info("Aguardando threads de geração pendentes (se houver)...")
    # (Em um app real, isso seria gerenciado de forma mais robusta)
    time.sleep(10) # Dá tempo para as últimas threads disparadas iniciarem
    while threading.active_count() > 1: # Espera até que só a thread principal reste
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

    # 1. Limpa o cache para um teste limpo
    # repo_para_limpar = ParserRepository()
    # repo_para_limpar.limpar_cache_completo() # (Função que adicionamos)

    # 2. Carrega os dados
    batch_data = carregar_dataset("dataset.json")

    if not batch_data:
        logging.error("Simulação interrompida. Dataset não pôde ser carregado.")
    else:
        # --- FASE 1: PRÉ-SCAN ---
        merged_schemas_map = pre_scan_e_mesclar_schemas(batch_data)
        
        # --- FASE 3: PROCESSAMENTO SERIAL ---
        processar_batch_serial(batch_data, merged_schemas_map)