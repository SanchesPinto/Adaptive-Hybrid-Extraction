import re
import logging
from typing import Dict, Optional, Any

class ParserExecutor:
    """
    Implementa o "Módulo 2: Executor de Parser" (Camada 1.5), 
    refatorado para a "Arquitetura V5" (Dict de Regex).
    
    Esta versão reverte à lógica V1/V2 (iterativa), que é mais robusta
    do que a Regex única V4, que se provou frágil.
    """
    
    def execute_parser(self, 
                         parser_v4: Dict[str, Any], 
                         pdf_text: str, 
                         requested_schema: Dict[str, str]
                         ) -> Dict[str, Optional[str]]:
        """
        Executa cada Regex do 'generated_regex_dict' contra o texto do PDF.
        
        Args:
            parser_v4: O dicionário V4/V5 completo (com "generated_regex_dict").
            pdf_text: A string de texto completa do PDF.
            requested_schema: O schema da requisição *atual*.
            
        Returns:
            Um dicionário com os dados extraídos.
        """
        logging.info("Iniciando Módulo 2 (V5 - Regex Dict): Execução do Parser...")
        
        # 1. Obter o dicionário de Regexes
        regex_dict = parser_v4.get("generated_regex_dict")
        
        if not regex_dict:
            logging.error("PARSER INVÁLIDO: O parser V5 não contém a chave 'generated_regex_dict'.")
            return {field_name: None for field_name in requested_schema.keys()}

        extracted_data = {}

        # 2. Iterar sobre os campos que o USUÁRIO pediu
        for field_name in requested_schema.keys():
            
            # 3. Obter a Regex individual para este campo
            regex_pattern = regex_dict.get(field_name)
            
            if not regex_pattern:
                logging.warning(f"Campo '{field_name}' está no schema da requisição, mas não no parser. Retornando null.")
                extracted_data[field_name] = None
                continue

            try:
                # 4. Executar a Regex individual
                match = re.search(regex_pattern, pdf_text, re.DOTALL)
                
                if match:
                    # 5. Sucesso: Extrai o primeiro grupo de captura ()
                    # (Assumimos que o M1 seguiu a regra de usar '()' para o valor)
                    value = match.group(1)
                    
                    if value:
                        extracted_data[field_name] = value.strip()
                    else:
                        # A Regex deu match, mas o grupo de captura estava vazio
                        extracted_data[field_name] = None
                else:
                    # 6. Falha: A Regex não encontrou match.
                    logging.warning(f"Módulo 2 (V5): Regex para '{field_name}' não encontrou match.")
                    extracted_data[field_name] = None
                    
            except re.error as e:
                # 7. Erro Crítico: O LLM gerou uma Regex individual inválida.
                logging.error(f"ERRO DE REGEX V5 para '{field_name}': {e} | Pattern: {regex_pattern}")
                extracted_data[field_name] = None
            except IndexError:
                # 8. Erro Crítico: O LLM esqueceu o grupo de captura '()'.
                logging.error(f"ERRO DE REGEX V5 para '{field_name}': Padrão não possui grupo de captura ().")
                extracted_data[field_name] = None

        logging.info("Módulo 2 (V5): Execução do parser concluída.")
        return extracted_data