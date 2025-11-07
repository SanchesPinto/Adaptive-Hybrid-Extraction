# Módulo: validation_generator.py
# (V18.5: Foco em Regras de Validação Extremamente Fortes)

import os
import json
import logging
from openai import OpenAI
from dotenv import load_dotenv
from typing import Optional, Dict, Any

load_dotenv()

class ValidationGenerator:
    """
    Implementa o Gerador de Regras de Validação (V18.5).
    
    Nosso 'ParserGenerator' é 
    não confiável (gera Regex ruins e Falsos Positivos 
   ).
    
    Esta é a nossa principal linha de defesa. O prompt V18.5 é 
    projetado para criar regras de validação (patterns)
    extremamente rigorosas para que o 'ConfidenceCalculator'
    possa PEGAR os Falsos Positivos.
    """
    
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logging.error("OPENAI_API_KEY não encontrada.")
            raise ValueError("API key da OpenAI não configurada.")
            
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-5-mini" 
        
    def _build_prompt_v18_validation(self, 
                                      schema: dict, 
                                      correct_json_example: dict) -> str:
        """
        Monta o prompt V18.5 - Focado em Regras de Padrão (Pattern).
        """
        
        schema_str = json.dumps(schema, indent=2, ensure_ascii=False)
        json_example_str = json.dumps(correct_json_example, indent=2, ensure_ascii=False)
        
        prompt_template = f"""
Você é um analista de dados sênior especialista em Regex.
Sua tarefa é gerar um JSON de `validation_rules`
para o `EXTRACTION_SCHEMA`. Use o `JSON_DE_GABARITO` como
exemplo de dados corretos.

REGRAS DE GERAÇÃO (MUITO IMPORTANTE):
1.  **Priorize 'pattern':** Para campos `string`, sempre que possível,
    gere uma regra `pattern` (Regex) que valide o *formato* exato.
    * **Exemplo:** Se o gabarito tem `"inscricao": "101943"`, a regra
        NÃO é `{{'type': 'string'}}`.
        A regra CORRETA é `{{'type': 'string', 'pattern': '^\\d{{6}}$'}}`
        (exatamente 6 dígitos).
    * **Exemplo:** Se o gabarito tem `"seccional": "PR"`, a regra
        CORRETA é `{{'type': 'string', 'pattern': '^[A-Z]{{2}}$'}}`
        (exatamente 2 letras maiúsculas).

2.  **Validação Cruzada (Anti-Falso Positivo):** Preste atenção no
    gabarito. Se `"telefone_profissional"` for `null` e
    `"situacao"` for `"SITUAÇÃO REGULAR"`, sua regra para
    `telefone_profissional` NÃO PODE aceitar "SITUAÇÃO REGULAR".
    * **Exemplo:** Para `telefone_profissional`, a regra
        CORRETA é `{{'type': 'string', 'nullable': True, 'pattern': '^[0-9()\\-\\s]+$'}}`.

3.  **Use `nullable: False`:** Por padrão, os campos não podem ser
    nulos, a menos que o `JSON_DE_GABARITO` mostre um `null`
    (ex: `telefone_profissional`).

---
EXTRACTION_SCHEMA (O que validar):
{schema_str}
---
JSON_DE_GABARITO (Exemplo de dados corretos):
{json_example_str}
---
OUTPUT: JSON (APENAS O JSON do 'validation_rules', com 'pattern' fortes)
""" 
        return prompt_template.strip()

    def generate_rules(self, 
                       schema: dict, 
                       correct_json_example: dict) -> Optional[Dict[str, Any]]:
        """
        Chama a API (Chamada 3) para gerar APENAS as regras (V18.5).
        """
        prompt = self._build_prompt_v18_validation(schema, correct_json_example)
        
        try:
            logging.info(f"[BACKGROUND] Chamando {self.model} para gerar ValidationRules (V18.5)...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Você é um arquiteto de dados especialista em Regex que responde apenas com JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
                # A API não aceita mais temperature=0.0
            )
            
            response_content = response.choices[0].message.content
            rules_dict = json.loads(response_content)
            logging.info(f"[BACKGROUND] ValidationRules (V18.5) geradas com sucesso.")
            return rules_dict
            
        except Exception as e:
            logging.error(f"[BACKGROUND] Erro ao gerar ValidationRules (V18.5): {e}")
            return None