# Módulo: validation_generator.py
# (V19.2: "Gerador de Validação Heurístico" - Sem LLM)

import re
import logging
from typing import Optional, Dict, Any

class ValidationGenerator:
    """
    Implementa o Gerador de Regras de Validação (V19.2).
    Substitui a chamada de LLM (V18.5) por uma geração
    heurística local que faz engenharia reversa do 'gabarito'.
    """
    
    def _infer_rule_from_value(self, value: Optional[str]) -> Dict[str, Any]:
        """
        Cria a regra de validação mais forte possível
        com base em um valor de exemplo (do gabarito).
        """
        
        # Regra 1: Nulabilidade
        if value is None or value == "":
            return {"type": "string", "nullable": True}

        # Regra 2: Datas (Formato DD/MM/YYYY)
        if re.match(r"^\d{2}/\d{2}/\d{4}$", value):
            return {"type": "date", "nullable": False, "format": "dd/mm/yyyy"}
            
        # Regra 3: CPF
        if re.match(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$", value):
            return {"type": "string", "nullable": False, "pattern": r"^\d{3}\.\d{3}\.\d{3}-\d{2}$"}
            
        # Regra 4: CEP
        if re.match(r"^\d{5}-\d{3}$", value):
            return {"type": "string", "nullable": False, "pattern": r"^\d{5}-\d{3}$"}

        # Regra 5: IDs Numéricos (ex: "101943")
        if re.match(r"^\d+$", value):
            length = len(value)
            return {"type": "string", "nullable": False, "pattern": f"^\\d{{{length}}}$"}

        # Regra 6: Valores Monetários (ex: "2.372,64")
        if re.match(r"^(R\$|\$)?\s*[\d.,]+$", value, re.IGNORECASE):
            return {"type": "string", "nullable": False, "pattern": r"^(R\$|\$)?\s*[\d.,]+$"}

        # Regra 7: Enum/String Curta (ex: "PR" ou "SUPLEMENTAR" ou "CLIENTE")
        if len(value.split()) < 3 and re.match(r"^[A-Z\s'DARC]+$", value):
             return {"type": "enum", "nullable": False, "values": [v.strip() for v in value.split()]}

        # Regra 8: Default (String genérica)
        return {"type": "string", "nullable": False, "min_length": 2}


    def generate_rules(self, 
                       schema: dict, 
                       correct_json_example: dict) -> Optional[Dict[str, Any]]:
        """
        Chama a lógica de inferência para cada campo
        do gabarito (extraído pelo FallbackExtractor).
        """
        logging.info(f"[BACKGROUND] Gerando ValidationRules HEURÍSTICAS (V19.2)...")
        
        validation_rules = {}
        
        for field_name in schema.keys():
            example_value = correct_json_example.get(field_name)
            rule = self._infer_rule_from_value(example_value)
            validation_rules[field_name] = rule
            
        logging.info("[BACKGROUND] ValidationRules HEURÍSTICAS (V19.2) geradas com sucesso.")
        # Retorna o dicionário no formato que o ConfidenceCalculator espera
        #
        return {"validation_rules": validation_rules}