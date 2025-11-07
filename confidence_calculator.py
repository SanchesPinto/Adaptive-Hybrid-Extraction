# Módulo: confidence_calculator.py
# (Substituto do Módulo 3 ingênuo)

import logging
import re
from typing import Dict, Optional, Callable, Any

class ConfidenceCalculator:
    """
    Implementa o "Módulo 3.1: O Sistema de Confiança Robusto" (V17).
    
    Substitui a verificação ingênua de "está preenchido?" por um
    sistema de validação heurística por campo.
    
    Isso nos protege contra FALSOS POSITIVOS, onde o Módulo 2 (Donut)
    retorna lixo (ex: "inscricao" = "Seccional").
    """

    def __init__(self):
        # Cache de validadores para não precisar recriar as regras
        self._validator_cache: Dict[str, Dict[str, Callable]] = {}

    # --- REGEX DE VALIDAÇÃO GERAL ---
    # Usado para checar se um valor parece um número de inscrição/CEP
    def _is_numeric_like(self, value: str) -> bool:
        return bool(value and re.search(r'\d', value) and not self_is_generic_label(value))

    # Usado para checar se um valor é um rótulo genérico
    def _is_generic_label(self, value: str) -> bool:
        return value.lower() in ["seccional", "subseção", "inscrição", "nome"]

    # Usado para checar formatos de data
    def _is_date(self, value: str) -> bool:
        return bool(value and re.match(r'^\d{2}/\d{2}/\d{4}$', value))

    # Usado para checar valores de enum
    def _is_in_enum(self, value: str, enum_list: list) -> bool:
        return bool(value and value.lower() in enum_list)

    # --- VALIDADORES ESPECIALIZADOS POR LABEL ---

    def _get_validation_rules(self, label: str) -> Dict[str, Callable]:
        """
        Factory que retorna o conjunto de regras de validação
        correto para um determinado 'label'.
        """
        if label in self._validator_cache:
            return self._validator_cache[label]

        rules = {}
        if label == "carteira_oab":
            rules = {
                "nome": lambda v, data: bool(v and len(v) > 2 and not self._is_generic_label(v) and v != data.get('situacao')),
                "inscricao": lambda v, data: self._is_numeric_like(v) and v != data.get('seccional'),
                "seccional": lambda v, data: bool(v and len(v) == 2 and not v.isdigit()), # Ex: "PR"
                "subsecao": lambda v, data: bool(v and len(v) > 5 and not v.isdigit()),
                "categoria": lambda v, data: self._is_in_enum(v, ["advogado", "advogada", "suplementar", "estagiario", "estagiaria"]),
                "situacao": lambda v, data: self._is_in_enum(v, ["situação regular"]) and v != data.get('nome')
                # 'endereco' e 'telefone' são muito variáveis, 
                # então um 'is_not_empty' básico é o melhor que podemos fazer.
            }
        
        elif label == "tela_sistema":
            rules = {
                "data_base": lambda v, data: self._is_date(v),
                "data_verncimento": lambda v, data: self._is_date(v),
                "valor_parcela": lambda v, data: bool(v and re.search(r'[\d.,]', v)),
                "cidade": lambda v, data: bool(v and len(v) > 3),
                "data_referencia": lambda v, data: self._is_date(v)
            }
        
        # Cache para performance
        self._validator_cache[label] = rules
        return rules

    # --- MÉTODO PRINCIPAL ---

    def calculate_confidence(self, 
                             extracted_data: Dict[str, Optional[str]], 
                             requested_schema: Dict[str, str],
                             label: str) -> float:
        """
        Calcula um score de confiança baseado em regras de validação V17.
        
        Args:
            extracted_data: Os dados retornados pelo Módulo 2 (Donut).
            requested_schema: O schema original da requisição.
            label: O label do item (ex: 'carteira_oab')
                              
        Returns:
            Um score de 0.0 a 1.0.
        """
        
        if not requested_schema:
            logging.warning("Schema da requisição está vazio. Retornando confiança 0.0")
            return 0.0

        total_fields_requested = len(requested_schema)
        validated_fields = 0
        
        logging.info("Iniciando Módulo 3.1: Cálculo de Confiança Robusto...")
        
        # Pega as regras de validação para este label
        validation_rules = self._get_validation_rules(label)

        for field_name in requested_schema.keys():
            value = extracted_data.get(field_name)

            # 1. O campo está vazio?
            if not value:
                logging.warning(f"Confiança: Campo '{field_name}' está ausente ou vazio.")
                continue # Vai para o próximo loop, não conta como validado

            # 2. Existe uma regra de validação específica para ele?
            validator = validation_rules.get(field_name)
            
            if validator:
                # 3. Se sim, execute a regra
                try:
                    # Passamos o valor E o dict de dados completo (para validação cruzada)
                    if validator(value, extracted_data):
                        validated_fields += 1 # SUCESSO!
                    else:
                        # REGRA FALHOU! (Ex: "inscricao" = "Seccional")
                        logging.warning(f"Confiança: Campo '{field_name}' falhou na validação. Valor: '{value}'")
                except Exception as e:
                    logging.error(f"Confiança: Erro ao executar validador para {field_name}: {e}")
            else:
                # 4. Se não há regra, contamos como válido (confiança ingênua)
                #    (Ex: para 'endereco_profissional' que não tem regra)
                validated_fields += 1

        # Calcula o score final
        confidence_score = validated_fields / total_fields_requested
        
        logging.info(f"Módulo 3.1: {validated_fields} de {total_fields_requested} campos VALIDADOS.")
        logging.info(f"Módulo 3.1: Score de Confiança Final = {confidence_score:.2f}")
        
        return confidence_score