import logging
import json 
import os # Importado para limpar o cache nos testes

# --- Importando todos os nossos Módulos ---
from parser_repository import ParserRepository
from parser_generator import ParserGenerator         # Módulo 1
from parser_executor import ParserExecutor           # Módulo 2
from confidence_calculator import ConfidenceCalculator # Módulo 3
from fallback_extractor import FallbackExtractor     # Módulo de Fallback (Camada 2)

# Configuração inicial de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Este é o nosso "Gatilho de Fallback".
# Alinhado com o requisito de 80% de acurácia [cite: 362]
MIN_CONFIDENCE_THRESHOLD = 1.0 

def processar_extracao(label: str, schema: dict, pdf_text: str):
    """
    Função principal do Orquestrador (Módulo 4), agora completa
    com a lógica de fallback (Camada 2).
    
    Implementa a "Arquitetura Híbrida Sintética" (Alternativa 3).
    """
    logging.info(f"Iniciando processamento para o label: {label}")
    
    repo = ParserRepository()
    parser = repo.get_parser(label)
    extracted_data = {}
    
    # Inicializa o Fallback Extractor para ser usado em caso de falha
    fallback = FallbackExtractor()

    if not parser:
        # --- Cenário B (Cache Miss) ---
        logging.info(f"CACHE MISS para {label}. Acionando Módulo 1 (Gerador)...")
        generator = ParserGenerator()
        parser = generator.generate_parser(schema, pdf_text)
        
        if parser:
            # Geração foi um SUCESSO
            repo.save_parser(label, parser)
            logging.info(f"Novo parser para {label} gerado e salvo.")
        else:
            # FALHA NA GERAÇÃO: Aciona o fallback para extração completa
            logging.error(f"Falha ao gerar parser para {label}. Acionando Fallback Direto (Modo 1).")
            extracted_data = fallback.extract_all(schema, pdf_text)
            
            if not extracted_data:
                logging.critical(f"FALHA CRÍTICA: Geração do Parser e Fallback falharam.")
                return {"error": "Falha crítica na geração do parser E no fallback."}
            
            logging.info("Fallback (Modo 1) concluído. Retornando dados.")
            return extracted_data # Retorna dados do fallback
    
    # --- Cenário A (Cache Hit ou Geração Bem-Sucedida) ---
    logging.info(f"Parser para {label} está pronto. Acionando Módulo 2 (Executor)...")
    
    executor = ParserExecutor()
    extracted_data = executor.execute_parser(parser, pdf_text)
    
    logging.info("--- DADOS EXTRAÍDOS (Resultado do Módulo 2) ---")
    logging.info(json.dumps(extracted_data, indent=2, ensure_ascii=False))
    logging.info("-------------------------------------------------")
    
    # --- Módulo 3 (Confiança) ---
    calculator = ConfidenceCalculator()
    confidence = calculator.calculate_confidence(extracted_data, schema)
    
    # --- Lógica de Gatilho (Decisão) ---
    if confidence >= MIN_CONFIDENCE_THRESHOLD:
        logging.info(f"Confiança Alta ({confidence:.2f} >= {MIN_CONFIDENCE_THRESHOLD}). Retornando dados do Módulo 2.")
        return extracted_data
    else:
        # BAIXA CONFIANÇA: Aciona o fallback para campos FALTANTES (Modo 2)
        logging.warning(f"Confiança Baixa ({confidence:.2f} < {MIN_CONFIDENCE_THRESHOLD}). Acionando Fallback Otimizado (Modo 2)...")
        
        # Calcula os campos que o Módulo 2 não conseguiu encontrar
        campos_faltantes = {
            k: v for k, v in schema.items() 
            if k not in extracted_data or not extracted_data[k]
        }
        
        if not campos_faltantes:
             logging.error("Confiança baixa, mas não há campos faltantes? (Erro de lógica). Retornando dados parciais.")
             return extracted_data

        # Chama o fallback (Modo 2) apenas para os campos faltantes
        fallback_data = fallback.extract_missing(campos_faltantes, pdf_text, extracted_data)
        
        if fallback_data:
            # Combina os dados (Módulo 2 + Fallback)
            final_data = extracted_data.copy()
            final_data.update(fallback_data)
            logging.info("Fallback (Modo 2) concluído. Retornando dados combinados.")
            return final_data
        else:
            logging.error("Fallback (Modo 2) falhou. Retornando dados parciais do Módulo 2 (melhor esforço).")
            return extracted_data # Retorna o melhor que temos

# --- SIMULAÇÃO DE FLUXO (Pronto para Testar) ---
if __name__ == "__main__":
    logging.info("--- INICIANDO SIMULAÇÃO DE FLUXO COMPLETA ---")

    # Carregando dados do EXEMPLO 1 do desafio
    label_teste = "carteira_oab" 
    
    # Texto do PDF Exemplo 1 [cite: 409-413, 417]
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
    
    # Schema do Exemplo 1 [cite: 416-427]
    schema_exemplo1 = {
      "nome": "Nome do profissional, normalmente no canto superior esquerdo da imagem",
      "inscricao": "Número de inscrição do profissional",
      "seccional": "Seccional do profissional",
      "subsecao": "Subseção à qual o profissional faz parte",
      "categoria": "Categoria, pode ser ADVOGADO, ADVOGADA, SUPLEMENTAR, ESTAGIARIO, ESTAGIARIA",
      "telefone_profissional": "Telefone do profissional",
      "situacao": "Situação do profissional, normalmente no canto inferior direito."
    }

    # --- TESTE 1: CACHE MISS (Força a Geração do Parser) ---
    print("\n\n--- [TESTE 1: CACHE MISS] ---")
    print("Limpando cache (se existir) para forçar o Módulo 1 (Geração)...")
    
    # Bloco de limpeza de cache
    repo_para_limpar = ParserRepository()
    parser_path = repo_para_limpar._get_parser_filepath(label_teste)
    if os.path.exists(parser_path):
        os.remove(parser_path)
        logging.info(f"Cache antigo '{parser_path}' removido.")
    
    # Executa a primeira vez (vai chamar o Módulo 1 e o Módulo 2)
    dados_teste_1 = processar_extracao(label_teste, schema_exemplo1, pdf_texto_exemplo1.strip())
    print(f"--- RESULTADO (TESTE 1): ---\n{json.dumps(dados_teste_1, indent=2, ensure_ascii=False)}\n")


    # --- TESTE 2: CACHE HIT (O Caminho Rápido) ---
    print("\n\n--- [TESTE 2: CACHE HIT] ---")
    print("Executando 2ª vez. O parser já deve existir no cache.")
    
    # Executa a segunda vez (vai usar o Módulo 2 diretamente)
    dados_teste_2 = processar_extracao(label_teste, schema_exemplo1, pdf_texto_exemplo1.strip())
    print(f"--- RESULTADO (TESTE 2): ---\n{json.dumps(dados_teste_2, indent=2, ensure_ascii=False)}\n")
    
    # (Opcional) Teste 3: Forçar Fallback de Baixa Confiança
    # Para fazer isso, você pode:
    # 1. Editar manualmente o .json em 'parser_repository_cache' e quebrar uma das Regex.
    # 2. Ou mais fácil: mude `MIN_CONFIDENCE_THRESHOLD = 1.0` temporariamente e rode o Teste 2.
    
    print("\n--- SIMULAÇÃO CONCLUÍDA ---")