import logging
from typing import Dict, Optional

class ConfidenceCalculator:
    """
    Implementa o "Módulo 3: O Sistema de Confiança".
    
    Responsável por analisar os dados extraídos pelo Módulo 2
    e atribuir um score de confiança (0.0 a 1.0).
    
    Este score decide se acionamos o fallback (Camada 2).
    """

    def calculate_confidence(self, 
                             extracted_data: Dict[str, Optional[str]], 
                             requested_schema: Dict[str, str]) -> float:
        """
        Calcula um score de confiança baseado na taxa de preenchimento.
        
        Args:
            extracted_data: Os dados retornados pelo Módulo 2 (Executor).
                            Ex: {"nome": "Son Goku", "inscricao": None}
            requested_schema: O schema original da requisição.
                              Ex: {"nome": "...", "inscricao": "..."}
                              
        Returns:
            Um score de 0.0 a 1.0.
        """
        
        # Garante que o schema não esteja vazio para evitar divisão por zero
        if not requested_schema:
            logging.warning("Schema da requisição está vazio. Retornando confiança 0.0")
            return 0.0

        total_fields_requested = len(requested_schema)
        filled_fields = 0
        
        logging.info("Iniciando Módulo 3: Cálculo de Confiança...")

        for field_name in requested_schema.keys():
            # Verificamos se o campo está nos dados extraídos
            # E se o valor não é None (ou uma string vazia)
            if field_name in extracted_data and extracted_data[field_name]:
                filled_fields += 1
            else:
                logging.warning(f"Confiança: Campo '{field_name}' está ausente ou vazio.")

        # Calcula a taxa de preenchimento
        confidence_score = filled_fields / total_fields_requested
        
        logging.info(f"Módulo 3: {filled_fields} de {total_fields_requested} campos preenchidos.")
        logging.info(f"Módulo 3: Score de Confiança Final = {confidence_score:.2f}")
        
        return confidence_score