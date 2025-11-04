import re
import logging
from typing import Dict, Optional

class ParserExecutor:
    """
    Implementa o "Módulo 2: Executor de Parser" (Camada 1.5).
    
    Responsável por pegar um parser (dict de Regex) e executá-lo
    contra uma string de texto para extrair os dados.
    Esta operação é local, rápida e gratuita.
    """
    
    def execute_parser(self, parser: Dict[str, Optional[str]], pdf_text: str) -> Dict[str, Optional[str]]:
        """
        Executa cada Regex do parser contra o texto do PDF.
        
        Args:
            parser: O dicionário gerado pelo Módulo 1. 
                    Ex: {"nome": "(?i)nome: (.*)", "valor": null}
            pdf_text: A string de texto completa do PDF.
            
        Returns:
            Um dicionário com os dados extraídos.
            Ex: {"nome": "Son Goku", "valor": null}
        """
        extracted_data = {}
        logging.info("Iniciando Módulo 2: Execução do Parser...")

        for field_name, regex_pattern in parser.items():
            
            # 1. Verifica se o Módulo 1 nos deu uma Regex (ou se disse 'null')
            if not regex_pattern:
                logging.warning(f"Campo '{field_name}' não possui Regex (null). Pulando.")
                extracted_data[field_name] = None
                continue

            try:
                # 2. Executa a Regex
                # re.DOTALL é um flag crucial: faz com que o '.' (ponto) 
                # também corresponda a quebras de linha (\n),
                # o que é vital para campos multilinha.
                match = re.search(regex_pattern, pdf_text, re.DOTALL)
                
                if match:
                    # 3. Sucesso: A Regex encontrou um match.
                    # Nosso prompt pediu por 'grupos de captura ()'.
                    # 'match.group(1)' pega o texto do *primeiro* grupo.
                    value = match.group(1)
                    
                    if value:
                        # Limpa espaços em branco extras do início/fim
                        extracted_data[field_name] = value.strip()
                    else:
                        # A Regex deu match, mas o grupo de captura estava vazio
                        extracted_data[field_name] = None
                        
                else:
                    # 4. Falha: A Regex não encontrou nenhum match no texto.
                    logging.warning(f"Campo '{field_name}' não encontrado no texto.")
                    extracted_data[field_name] = None
                    
            except re.error as e:
                # 5. Erro Crítico: O LLM gerou uma Regex inválida.
                logging.error(f"ERRO DE REGEX para '{field_name}': {e} | Pattern: {regex_pattern}")
                extracted_data[field_name] = None
            except IndexError:
                # 6. Erro Crítico: O LLM esqueceu o grupo de captura '()'.
                logging.error(f"ERRO DE REGEX para '{field_name}': Padrão não possui grupo de captura ().")
                extracted_data[field_name] = None

        logging.info("Módulo 2: Execução do parser concluída.")
        return extracted_data