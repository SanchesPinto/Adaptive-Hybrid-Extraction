# Módulo: main.py
# (Arquitetura V17: "Donut-First")

import logging
import json 
import os
import time
import fitz # (PyMuPDF)

# --- Importando todos os nossos Módulos V17 ---
from donut_extractor import DonutExtractor
from confidence_calculator import ConfidenceCalculator # (V17 Robusto)
from fallback_extractor import FallbackExtractor     

# --- Configuração Global ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Limite de confiança para acionar o Fallback (Módulo 4)
MIN_CONFIDENCE_THRESHOLD = 0.8 

# --- CARREGAMENTO SINGLETON DOS MODELOS ---
# Carregamos os modelos na inicialização do script, UMA VEZ.
# Isso garante que a inferência seja rápida e não pague o custo
# de carregamento do modelo em cada requisição.
try:
    logging.info("Carregando modelos (Singleton)...")
    DONUT_EXTRACTOR = DonutExtractor()
    CONFIDENCE_CALCULATOR = ConfidenceCalculator() # V17 Robusto
    FALLBACK_EXTRACTOR = FallbackExtractor()
    logging.info("Modelos V17 (Donut, Confidence, Fallback) carregados.")
except Exception as e:
    logging.critical(f"Falha ao carregar modelos de ML. Encerrando. Erro: {e}")
    exit(1)
# ----------------------------------------

def ler_texto_do_pdf(pdf_path: str) -> str | None:
    """
    Extrai o texto de um arquivo PDF usando PyMuPDF (fitz).
    (Mantido, pois o FallbackExtractor (M4) ainda precisa do texto 1D).
    """
    # O path completo é relativo ao 'files'
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
# --- FUNÇÃO PRINCIPAL (V17) ---
#
def processar_extracao(label: str, 
                       item_schema: dict, 
                       pdf_path: str, # (Necessário para o Donut)
                       pdf_text: str  # (Necessário para o Fallback)
                       ):
    """
    Função principal do Orquestrador (V17).
    Executa o pipeline: Donut -> Confidence -> Fallback.
    """
    logging.info(f"Iniciando processamento (V17) para o label: {label}")
    
    # --- MÓDULO 2 (V17): DonutExtractor ---
    # O "Caminho Rápido" (Local, Custo $0, Layout-Aware)
    logging.info("Acionando Módulo 2 (DonutExtractor)...")
    # Passamos o PATH do PDF, não o texto
    full_pdf_path = os.path.join("files", pdf_path)
    extracted_data = DONUT_EXTRACTOR.extract(full_pdf_path, item_schema)
    
    logging.info("--- DADOS EXTRAÍDOS (Resultado Módulo 2 - Donut) ---")
    logging.info(json.dumps(extracted_data, indent=2, ensure_ascii=False))

    # --- MÓDULO 3 (V17): ConfidenceCalculator Robusto ---
    # Validamos os dados para evitar Falsos Positivos
    logging.info("Acionando Módulo 3.1 (ConfidenceCalculator Robusto)...")
    confidence = CONFIDENCE_CALCULATOR.calculate_confidence(
        extracted_data=extracted_data,
        requested_schema=item_schema,
        label=label # O Módulo 3.1 precisa do label para as regras
    )
    
    # --- MÓDULO 4 (V16): Decisão e Fallback ---
    if confidence >= MIN_CONFIDENCE_THRESHOLD:
        # SUCESSO V17: Dados são bons, custo $0, tempo < 1s.
        logging.info(f"Confiança Alta ({confidence:.2f} >= {MIN_CONFIDENCE_THRESHOLD}). Retornando dados do Módulo 2 (Donut).")
        return extracted_data
    else:
        # FALHA V17: Donut falhou ou teve dados inválidos.
        # Acionamos nossa rede de segurança (LLM Pago).
        logging.warning(f"Confiança Baixa ({confidence:.2f} < {MIN_CONFIDENCE_THRESHOLD}). Acionando Fallback Otimizado (Modo 2)...")
        
        # A lógica para encontrar campos faltantes (vazios) permanece a mesma
        campos_faltantes = {
            k: v for k, v in item_schema.items() 
            if k not in extracted_data or not extracted_data[k]
        }
        
        # Se a confiança foi baixa, mas TODOS os campos pedidos estão preenchidos
        # (ex: Donut retornou lixo validado como ruim),
        # não há nada que o Fallback Otimizado (Modo 2) possa fazer.
        if not campos_faltantes:
             logging.warning("Confiança baixa, mas sem campos vazios para o Modo 2 preencher. Retornando dados (potencialmente ruins) do Módulo 2.")
             return extracted_data 

        # Acionamos o Módulo 4 (FallbackExtractor)
        fallback_data = FALLBACK_EXTRACTOR.extract_missing(
            missing_schema=campos_faltantes, 
            pdf_text=pdf_text, 
            partial_data=extracted_data
        )
        
        if fallback_data:
            # Mescla os dados bons (M2) com os corrigidos (M4)
            final_data = extracted_data.copy()
            final_data.update(fallback_data)
            return final_data
        else:
            # O Fallback (M4) também falhou
            return extracted_data

#
# --- FUNÇÃO DE PROCESSAMENTO DE BATCH (V17) ---
#
def processar_batch_serial(batch_data: list):
    """
    FASE 2 (V17): Processa o batch serialmente, item por item,
    lendo os PDFs reais.
    (Fase 1 'pre-scan' foi removida).
    """
    logging.info("--- FASE 2: Iniciando Processamento Serial do Batch (V17) ---")
    
    start_time_total = time.time()
    resultados_finais = []

    for i, item in enumerate(batch_data):
        logging.info(f"--- Processando Item {i+1}/{len(batch_data)} ---")
        item_label = item.get("label")
        item_schema = item.get("extraction_schema")
        pdf_path = item.get("pdf_path") # (Para o Donut)
        
        # Lemos o texto 1D aqui, caso o Fallback precise dele
        pdf_text = ler_texto_do_pdf(pdf_path) # (Para o Fallback)
        
        if not all([item_label, item_schema, pdf_path, pdf_text]):
            logging.error(f"Item {i+1} inválido (label, schema ou PDF ausente). Pulando.")
            continue
            
        start_time_item = time.time()
        
        # Chamamos o orquestrador V17
        resultado = processar_extracao(
            label=item_label,
            item_schema=item_schema,
            pdf_path=pdf_path,
            pdf_text=pdf_text
        )
        
        resultados_finais.append(resultado)
        
        # --- Lógica de Medição de Tempo (Idêntica) ---
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
    
    # Não há mais threads para aguardar
    logging.info("Simulação V17 finalizada.")

#
# --- CARREGADOR DE DATASET (Idêntico) ---
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
# --- PONTO DE ENTRADA (V17) ---
#
if __name__ == "__main__":
    logging.info("--- INICIANDO SIMULAÇÃO DE BATCH (V17: Donut-First) ---")

    # Não precisamos mais limpar o cache do parser V16
    # repo_para_limpar = ParserRepository()
    # repo_para_limpar.limpar_cache_completo() 

    batch_data = carregar_dataset("dataset.json")

    if not batch_data:
        logging.error("Simulação interrompida. Dataset não pôde ser carregado.")
    else:
        # Fase 1 (pre-scan) foi removida.
        
        # --- FASE 2 (V17) ---
        processar_batch_serial(batch_data)