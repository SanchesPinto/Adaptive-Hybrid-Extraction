import logging
import json 
import os 
import threading # Importa o threading para o fluxo assíncrono
from typing import Dict, Any, Optional, Set # <--- CORREÇÃO 1: Importação de tipagem

# --- Importando todos os nossos Módulos V4 ---
from knowledge_repository import KnowledgeRepository
from parser_generator import ParserGenerator           # Módulo 1 (V4)
from parser_executor import ParserExecutor             # Módulo 2 (V4)
from confidence_calculator import ConfidenceCalculator # Módulo 3 (Sem mudanças)
from fallback_extractor import FallbackExtractor       # Camada 2 (Sem mudanças)

# Configuração inicial de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Gatilho de Fallback
MIN_CONFIDENCE_THRESHOLD = 0.9

#
# --- CORREÇÃO 2: Funções auxiliares movidas para CIMA ---
#

def _get_campos_faltantes(
    requested_schema: Dict[str, str], 
    parser_v4: Optional[Dict[str, Any]]
) -> Set[str]: # (Tipo de retorno Set[str] adicionado para clareza)
    """
    Função helper para verificar o "Cache Hit Parcial".
    Compara as chaves do schema pedido com as chaves que o parser V4 conhece.
    """
    if not parser_v4:
        # Se não há parser, todos os campos são "faltantes"
        return set(requested_schema.keys())
        
    # Usamos o 'semantic_mapping' como a fonte da verdade sobre o que o parser sabe
    campos_no_parser = parser_v4.get("semantic_mapping", {}).keys()
    campos_requisitados = requested_schema.keys()
    
    campos_faltantes = set(campos_requisitados) - set(campos_no_parser)
    return campos_faltantes


def _generate_and_save_knowledge_async(
    label: str, 
    requested_schema: Dict[str, str], 
    pdf_text: str
):
    """
    [cite_start]FUNÇÃO ALVO DA THREAD (Trabalho Lento) [cite: 597-599]
    
    Responsável por (re)gerar e salvar o parser E o schema mesclado.
    """
    logging.info(f"[ASYNC] Iniciando geração de conhecimento para {label}...")
    
    repo = KnowledgeRepository()
    generator = ParserGenerator()
    
    # 1. Carrega o conhecimento antigo (se existir)
    conhecimento_antigo = repo.get_knowledge(label)
    schema_mesclado = {}

    if conhecimento_antigo:
        schema_mesclado = conhecimento_antigo.get("merged_schema", {})

    # 2. Mescla o schema antigo com o novo schema (Acumula conhecimento)
    # Isso garante que o parser será (re)gerado com TODOS os campos já vistos
    schema_mesclado.update(requested_schema)
    
    # 3. Gera o novo parser V4 usando o schema completo
    # Esta é a chamada de 55 segundos ao gpt-5-mini
    novo_parser_v4 = generator.generate_parser(schema_mesclado, pdf_text)
    
    if novo_parser_v4:
        # 4. Salva o NOVO conhecimento (schema mesclado + parser V4)
        novo_conhecimento = {
            "merged_schema": schema_mesclado,
            "parser_v4": novo_parser_v4
        }
        repo.save_knowledge(label, novo_conhecimento)
        logging.info(f"[ASYNC] Conhecimento para {label} atualizado com sucesso.")
    else:
        logging.error(f"[ASYNC] Falha ao gerar parser V4 para {label}.")

#
# --- Fim da Seção de Funções Auxiliares ---
#

def processar_extracao(label: str, schema: dict, pdf_text: str):
    """
    Função principal do Orquestrador (Módulo 4), agora com a lógica V4 completa.
    [cite_start]Implementa a "Arquitetura Híbrida Sintética" [cite: 392-393] com "Cache Parcial".
    """
    logging.info(f"Iniciando processamento V4 para o label: {label}")
    
    repo = KnowledgeRepository()
    
    # 1. Carrega o Conhecimento (Parser V4 + Schema Mesclado)
    conhecimento_do_cache = repo.get_knowledge(label)
    parser_v4_do_cache = None
    if conhecimento_do_cache:
        parser_v4_do_cache = conhecimento_do_cache.get("parser_v4")

    # 2. Lógica de Decisão (O "Cache Hit Parcial")
    # (Agora esta chamada funciona, pois a função foi definida acima)
    campos_faltantes_no_parser = _get_campos_faltantes(schema, parser_v4_do_cache)
    
    # 3. Define o "Caminho" (Rápido ou Lento)
    # CAMINHO RÁPIDO: Se há um parser E ele conhece TODOS os campos pedidos
    if parser_v4_do_cache and not campos_faltantes_no_parser:
        
        # --- CAMINHO RÁPIDO (Cache Hit Total) ---
        logging.info(f"KNOWLEDGE-HIT TOTAL para {label}. Acionando Módulo 2 (Executor V4)...")
        
        executor = ParserExecutor()
        # Note a nova assinatura: (parser, texto, schema)
        extracted_data = executor.execute_parser(parser_v4_do_cache, pdf_text, schema)
        
        logging.info("--- DADOS EXTRAÍDOS (Resultado do Módulo 2 V4) ---")
        logging.info(json.dumps(extracted_data, indent=2, ensure_ascii=False))
        logging.info("-------------------------------------------------")
        
        # (Opcional: Módulo 3 de Confiança.)
        # calculator = ConfidenceCalculator()
        # confidence = calculator.calculate_confidence(extracted_data, schema)
        # if confidence < MIN_CONFIDENCE_THRESHOLD:
        #    ... (Lógica de Fallback Otimizado - MODO 2) ...
        
        return extracted_data
        
    else:
        # --- CAMINHO LENTO (Cache Miss Total ou Parcial) ---
        if not parser_v4_do_cache:
            logging.warning(f"KNOWLEDGE-MISS TOTAL para {label}. Acionando Caminho Lento.")
        else:
            logging.warning(f"KNOWLEDGE-MISS PARCIAL para {label}. Faltam campos: {campos_faltantes_no_parser}")
            
        # [cite_start]A. Resposta Síncrona (< 10s) [cite: 597-599]
        logging.info("Acionando Fallback Direto (Modo 1) para resposta síncrona.")
        fallback = FallbackExtractor()
        sync_data = fallback.extract_all(schema, pdf_text)
        
        if not sync_data:
            logging.critical(f"FALHA CRÍTICA: Fallback síncrono falhou.")
            sync_data = {"error": "Falha crítica no fallback."}

        # [cite_start]B. Geração Assíncrona (Background) [cite: 597-599]
        logging.info("Disparando geração de conhecimento em background...")
        # (Agora esta chamada funciona, pois a função foi definida acima)
        thread = threading.Thread(
            target=_generate_and_save_knowledge_async,
            args=(label, schema, pdf_text)
        )
        thread.start()
        
        # C. Retorna a resposta síncrona para o usuário
        logging.info("Retornando dados do Fallback (Modo 1).")
        return sync_data


# --- SIMULAÇÃO DE FLUXO (V4) ---
if __name__ == "__main__":
    logging.info("--- INICIANDO SIMULAÇÃO DE FLUXO V4 ---")

    # --- DADOS DE TESTE ---
    label_teste = "carteira_oab"
    
    # [cite_start]Texto do PDF Exemplo 1 [cite: 112-116, 120]
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
    
    # [cite_start]Schema do Exemplo 1 [cite: 117-130]
    schema_exemplo1 = {
      "nome": "Nome do profissional...",
      "inscricao": "Número de inscrição...",
      "seccional": "Seccional do profissional",
      "subsecao": "Subseção...",
      "categoria": "Categoria...",
      "telefone_profissional": "Telefone...",
      "situacao": "Situação do profissional..."
    }
    
    # [cite_start]Texto do PDF Exemplo 2 [cite: 164-177]
    pdf_texto_exemplo2 = """
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

    # [cite_start]Schema do Exemplo 2 (Note o campo "endereco_profissional") [cite: 152-162]
    schema_exemplo2 = {
      "nome": "Nome do profissional...",
      "inscricao": "Número de inscrição...",
      "seccional": "Seccional do profissional",
      "subsecao": "Subseção...",
      "categoria": "Categoria...",
      "endereco_profissional": "Endereço profissional completo",
      "situacao": "Situação do profissional..."
    }

    # --- LIMPEZA INICIAL (Simula uma sessão limpa) ---
    print("\n\n--- [LIMPEZA] ---")
    print("Limpando cache de conhecimento (se existir)...")
    repo_para_limpar = KnowledgeRepository()
    knowledge_path = repo_para_limpar._get_knowledge_filepath(label_teste)
    if os.path.exists(knowledge_path):
        os.remove(knowledge_path)
        logging.info(f"Cache antigo '{knowledge_path}' removido.")


    # --- TESTE 1: CACHE MISS TOTAL (Exemplo 1) ---
    print("\n\n--- [TESTE 1: CACHE MISS TOTAL (Exemplo 1)] ---")
    # 1. Deve acionar o Fallback (Síncrono)
    # 2. Deve disparar a Geração (Assíncrona) do parser V4 com o Schema 1
    dados_teste_1 = processar_extracao(label_teste, schema_exemplo1, pdf_texto_exemplo1.strip())
    print(f"--- RESULTADO (TESTE 1 - Fallback): ---\n{json.dumps(dados_teste_1, indent=2, ensure_ascii=False)}\n")
    print("--- (Aguarde o job async terminar no background...) ---")
    # Em um app real, não esperaríamos. Mas para o teste, precisamos que o job termine.
    # Vamos pausar o script principal para dar tempo à thread de 55s.
    # Em uma API (FastAPI), isso não seria necessário.
    import time
    time.sleep(80) # Simula o tempo de geração do parser


    # --- TESTE 2: CACHE HIT PARCIAL (Exemplo 2) ---
    print("\n\n--- [TESTE 2: CACHE HIT PARCIAL (Exemplo 2)] ---")
    print("O parser do Teste 1 existe, mas não tem 'endereco_profissional'.")
    # 1. Deve detectar 'campos_faltantes_no_parser'
    # 2. Deve acionar o Fallback (Síncrono)
    # 3. Deve disparar a (Re)Geração (Assíncrona) do parser V4 com o Schema MESCLADO (1+2)
    dados_teste_2 = processar_extracao(label_teste, schema_exemplo2, pdf_texto_exemplo2.strip())
    print(f"--- RESULTADO (TESTE 2 - Fallback): ---\n{json.dumps(dados_teste_2, indent=2, ensure_ascii=False)}\n")
    print("--- (Aguarde o job async de RE-GERAÇÃO terminar...) ---")
    time.sleep(80) # Simula o tempo de RE-geração do parser


    # --- TESTE 3: CACHE HIT TOTAL (Exemplo 1 Novamente) ---
    print("\n\n--- [TESTE 3: CACHE HIT TOTAL (Exemplo 1)] ---")
    print("O parser do Teste 2 (Schema Mesclado) existe e cobre o Schema 1.")
    # 1. Deve detectar que não há 'campos_faltantes_no_parser'
    # 2. Deve acionar o CAMINHO RÁPIDO (Módulo 2 V4)
    dados_teste_3 = processar_extracao(label_teste, schema_exemplo1, pdf_texto_exemplo1.strip())
    print(f"--- RESULTADO (TESTE 3 - Caminho Rápido): ---\n{json.dumps(dados_teste_3, indent=2, ensure_ascii=False)}\n")


    # --- TESTE 4: CACHE HIT TOTAL (Exemplo 2 Novamente) ---
    print("\n\n--- [TESTE 4: CACHE HIT TOTAL (Exemplo 2)] ---")
    print("O parser do Teste 2 (Schema Mesclado) existe e cobre o Schema 2.")
    # 1. Deve acionar o CAMINHO RÁPIDO (Módulo 2 V4)
    dados_teste_4 = processar_extracao(label_teste, schema_exemplo2, pdf_texto_exemplo2.strip())
    print(f"--- RESULTADO (TESTE 4 - Caminho Rápido): ---\n{json.dumps(dados_teste_4, indent=2, ensure_ascii=False)}\n")
    
    
    print("\n--- SIMULAÇÃO V4 CONCLUÍDA ---")