import re
import logging
from typing import Dict, Optional

class ParserExecutor:
    """
    Implementa o "Módulo 2: Executor de Parser" (Camada 1.5), 
    refatorado para a "Abordagem 2/V4".
    
    Responsável por pegar um parser V4 (um JSON com uma 'generated_regex' única)
    e executá-lo contra uma string de texto para extrair os dados.
    Esta operação é local, rápida e gratuita.
    """
    
    def execute_parser(self, 
                         parser: Dict[str, any], 
                         pdf_text: str, 
                         requested_schema: Dict[str, str] # <-- Assinatura atualizada!
                         ) -> Dict[str, Optional[str]]:
        """
        Executa a 'generated_regex' (com grupos nomeados) contra o texto do PDF.
        
        Args:
            parser: O dicionário V4 completo gerado pelo Módulo 1.
                    Ex: {"layout_analysis": "...", "semantic_mapping": {...}, "generated_regex": "(?<nome>...)"}
            pdf_text: A string de texto completa do PDF.
            requested_schema: O schema da requisição *atual*.
            
        Returns:
            Um dicionário com os dados extraídos, contendo *apenas* as chaves
            do requested_schema.
            Ex: {"nome": "Son Goku", "inscricao": "101943"}
        """
        logging.info("Iniciando Módulo 2 (V4 - Single Regex): Execução do Parser...")

        # 1. Obter a string da Regex única do parser V4
        regex_string = parser.get("generated_regex")
        
        if not regex_string:
            logging.error("PARSER INVÁLIDO: O parser V4 não contém a chave 'generated_regex'.")
            # Retorna null para tudo que foi pedido
            return {field_name: None for field_name in requested_schema.keys()}

        try:
            # 2. Executar a Regex (uma única vez)
            # re.DOTALL é crucial para campos multilinha
            match = re.search(regex_string, pdf_text, re.DOTALL)
            
            if not match:
                logging.warning("Módulo 2 (V4): A Regex não encontrou NENHUM match no texto.")
                return {field_name: None for field_name in requested_schema.keys()}
            
            # 3. Extrair todos os grupos nomeados de uma vez
            # Esta é a mágica: .groupdict() retorna {'nome': 'SON GOKU', 'inscricao': '101943', ...}
            # Grupos que não deram match (ex: um campo opcional) retornam None.
            all_extracted_data = match.groupdict()
            
            # 4. Filtrar/Limpar: Garantir que retornamos apenas o que foi pedido
            final_data = {}
            for field_name in requested_schema.keys():
                # Pega o valor (ou None se não foi encontrado/não estava no grupo)
                value = all_extracted_data.get(field_name) 
                
                if value:
                    final_data[field_name] = value.strip()
                else:
                    final_data[field_name] = None
                    
            logging.info("Módulo 2 (V4): Execução do parser concluída.")
            return final_data
            
        except re.error as e:
            # 5. Erro Crítico: O LLM gerou uma Regex V4 inválida.
            logging.error(f"ERRO DE REGEX V4: {e} | Pattern: {regex_string}")
            return {field_name: None for field_name in requested_schema.keys()}