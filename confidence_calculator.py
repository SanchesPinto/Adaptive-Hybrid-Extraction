# Módulo: confidence_calculator.py
# (V18.3: Corrigido para iterar corretamente)

import logging
import re
from typing import Dict, Optional, Any

class ConfidenceCalculator:
    """
    Implementa o "Módulo 3: O Sistema de Confiança" (V18.3).
    
    Esta versão corrige o bug de lógica da V18,
    garantindo que o loop 'for' itere sobre cada regra
    individualmente, em vez de tratar o dicionário de regras
    como um único item.
    """

    def _validate_rule(self, value: Optional[str], rule: Dict[str, Any]) -> bool:
        """
        Executa uma regra de validação individual.
        (Implementação stub - pode ser expandida)
        """
        # 1. Checagem de Nulabilidade
        is_nullable = rule.get("nullable", True)
        if not value:
            return is_nullable # Retorna True se for 'nullable', False se não for

        # 2. Checagens de Tipo e Formato
        rule_type = rule.get("type")
        
        try:
            if rule_type == "string":
                if "min_length" in rule and len(value) < rule["min_length"]:
                    return False
                if "max_length" in rule and len(value) > rule["max_length"]:
                    return False
                if "length" in rule and len(value) != rule["length"]:
                    return False
                if "pattern" in rule and not re.match(rule["pattern"], value):
                    return False
            
            elif rule_type == "integer":
                if not value.isdigit():
                    return False
                if "minimum" in rule and int(value) < rule["minimum"]:
                    return False
            
            elif rule_type == "date":
                if "format" in rule and rule["format"] == "dd/mm/yyyy":
                    if not re.match(r'^\d{2}/\d{2}/\d{4}$', value):
                        return False
            
            elif rule_type == "enum":
                if value.lower().strip() not in [v.lower() for v in rule.get("values", [])]:
                    return False

        except Exception as e:
            logging.warning(f"CONF (V18.3): Erro ao processar regra '{rule}' para valor '{value}': {e}")
            return False
            
        return True # Passou em todas as validações

    def calculate_confidence(self, 
                             extracted_data: Dict[str, Optional[str]], 
                             validation_rules: Dict[str, Any]) -> float:
        """
        Calcula um score de confiança baseado nas regras V18.3.
        
        Args:
            extracted_data: Os dados retornados pelo Módulo 2 (ParserExecutor).
            validation_rules: O dict de regras (ex: {"nome": ..., "inscricao": ...}).
                              
        Returns:
            Um score de 0.0 a 1.0.
        """
        
        if not validation_rules:
            logging.warning("CONF (V18.3): Não há regras de validação. Retornando 0.0")
            return 0.0
        
        # *** CORREÇÃO DO BUG (Início) ***
        # O log mostrou que 'validation_rules' podia
        # ser um dict aninhado: {"validation_rules": {...}}.
        # Esta lógica defensiva garante que peguemos o dict interno.
        if "validation_rules" in validation_rules and isinstance(validation_rules["validation_rules"], dict):
            logging.debug("CONF (V18.3): Detectado dict aninhado. Usando regras internas.")
            rules_to_validate = validation_rules["validation_rules"]
        else:
            rules_to_validate = validation_rules
        
        if not rules_to_validate:
             logging.warning("CONF (V18.3): Dicionário de regras de validação está vazio. Retornando 0.0")
             return 0.0
        # *** CORREÇÃO DO BUG (Fim) ***

        total_fields_with_rules = len(rules_to_validate)
        validated_fields = 0
        
        logging.info("Iniciando Módulo 3 (ConfidenceCalculator V18.3)...")

        # Itera sobre as REGRAS, não sobre os dados
        for field_name, rule in rules_to_validate.items():
            value = extracted_data.get(field_name)
            
            if self._validate_rule(value, rule):
                validated_fields += 1
            else:
                logging.warning(f"CONF (V18.3): Campo '{field_name}' falhou na validação. Valor: '{value}', Regra: {rule}")

        confidence_score = validated_fields / total_fields_with_rules
        
        logging.info(f"Módulo 3 (V18.3): {validated_fields} de {total_fields_with_rules} campos VALIDADOS.")
        logging.info(f"Módulo 3 (V18.3): Score de Confiança Final = {confidence_score:.2f}")
        
        return confidence_score