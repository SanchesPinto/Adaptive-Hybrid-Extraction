import os
import json
import logging
import re # <-- Adicionado para a Regex de fallback
from openai import OpenAI
from dotenv import load_dotenv
from typing import Optional # <-- Adicionado para clareza

# Carrega as variáveis de ambiente (OPENAI_API_KEY)
load_dotenv()

class ParserGenerator:
    """
    Implementa o "Módulo 1: Gerador de Parser" (V16).
    
    Agora usa um exemplo de JSON "correto" (do Fallback) para
    fazer a engenharia reversa das Regex.
    """
    
    def __init__(self):
        """
        Inicializa o cliente da OpenAI.
        """
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logging.error("OPENAI_API_KEY não encontrada. Verifique seu arquivo .env")
            raise ValueError("API key da OpenAI não configurada.")
            
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-5-mini" 
        
    def _build_prompt(self, 
                      schema: dict, 
                      pdf_text: str, 
                      correct_json_example: dict) -> str:
        """
        Monta o prompt final (V16) com base no schema mesclado,
        UM texto de exemplo, e o JSON "gabarito" extraído dele.
        """
        
        schema_str = json.dumps(schema, indent=2, ensure_ascii=False)
        json_example_str = json.dumps(correct_json_example, indent=2, ensure_ascii=False)
        
        # O template do prompt V16
        prompt_template = f"""
Você é um motor de engenharia reversa. Sua especialidade é criar Expressões Regulares (Regex) em Python.

Sua tarefa é gerar um PARSER. Você receberá:
1.  `EXTRACTION_SCHEMA`: O schema JSON completo que o parser deve ser capaz de extrair.
2.  `TEXTO_PDF_EXEMPLO`: Um texto de exemplo.
3.  `JSON_DE_GABARITO`: O JSON que foi extraído com SUCESSO do `TEXTO_PDF_EXEMPLO` (provavelmente por um humano ou outro LLM).

Seu objetivo é gerar as Regex que mapeiam o `TEXTO_PDF_EXEMPLO` ao `JSON_DE_GABARITO`.

REGRAS DE GERAÇÃO:
1.  **Mapeamento Direto:** Para cada campo no `JSON_DE_GABARITO` que NÃO seja `null` (ex: "nome": "SON GOKU"), crie uma Regex robusta que encontre o rótulo (ex: "Nome") e capture o valor (ex: "SON GOKU") do `TEXTO_PDF_EXEMPLO`.
2.  **Generalização de Campos `null`:** O `JSON_DE_GABARITO` pode ter campos `null` (ex: "telefone_profissional": null). Isso significa que o gabarito falhou em encontrar esse campo.
3.  **REGRA CRÍTICA (Campos `null`):** Para qualquer campo que seja `null` no `JSON_DE_GABARITO`, você DEVE consultar o `EXTRACTION_SCHEMA` e gerar uma Regex genérica baseada no *nome* da chave (ex: `(?i)telefone_profissional\\s*[:\\-]?\\s*([^\\n\\r]+)`).
4.  **NÃO GERE `null`:** A saída JSON final NUNCA deve conter `null` como valor. Gere uma Regex para CADA chave no `EXTRACTION_SCHEMA`.
5.  **Formato de Saída:** Responda APENAS com um objeto JSON válido. As chaves devem ser EXATAMENTE as chaves do `EXTRACTION_SCHEMA` de entrada. Os valores devem ser a STRING da Regex.

---
INPUT 1: EXTRACTION SCHEMA (O schema completo que o parser deve ter)
```json
{schema_str}
```
```Plaintext
{pdf_text}
```
INPUT 3: JSON_DE_GABARITO (A "resposta correta" para o texto acima)
```json
{json_example_str}
```

OUTPUT: JSON (APENAS O JSON, SEM nulls NOS VALORES) 
""" 

        return prompt_template.strip()

    def generate_parser(self, 
                    schema: dict, 
                    pdf_text: str, 
                    correct_json_example: dict) -> Optional[dict]:
        """
        Chama a API do gpt-5 mini para gerar o parser (V16).
        
        Args:
            schema: O schema mesclado completo (ex: 14 campos).
            pdf_text: O texto do primeiro item (ex: Item 4).
            correct_json_example: O JSON extraído pelo Fallback (ex: 7 campos).
            
        Returns:
            Um dicionário (o parser) em caso de sucesso, ou None em caso de falha.
        """
        prompt = self._build_prompt(schema, pdf_text, correct_json_example)
        
        try:
            logging.info(f"Chamando {self.model} para gerar parser (com gabarito V16)...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Você é um programador Python especialista em Regex que responde apenas com JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}, 
                # temperature=0.0 
            )
            
            response_content = response.choices[0].message.content
            
            parser_dict = json.loads(response_content)
            
            # Verificação final para garantir que ele não gerou 'null'
            # (A Regra 3 do prompt V16 já lida com isso, mas é uma boa garantia)
            for key, value in parser_dict.items():
                if value is None:
                    logging.warning(f"O LLM (V16) ignorou a regra e gerou 'null' para {key}. Corrigindo.")
                    parser_dict[key] = f"(?i){re.escape(key)}\\s*[:\\-]?\\s*([^\\n\\r]+)"

            logging.info(f"Parser (V16) gerado com sucesso pelo {self.model}.")
            return parser_dict
            
        except json.JSONDecodeError as e:
            logging.error(f"Falha ao decodificar JSON da resposta do LLM: {e}")
            logging.error(f"Resposta recebida (não-JSON): {response_content}")
            return None
        except Exception as e:
            logging.error(f"Erro ao chamar a API OpenAI: {e}")
            return None
        

        