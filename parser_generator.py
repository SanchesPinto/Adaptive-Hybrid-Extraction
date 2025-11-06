import os
import json
import logging
from openai import OpenAI
from dotenv import load_dotenv
import re

# Carrega as variáveis de ambiente (OPENAI_API_KEY) do arquivo .env
# Isso permite que os.getenv("OPENAI_API_KEY") funcione.
load_dotenv()

class ParserGenerator:
    """
    Implementa o "Módulo 1: Gerador de Parser" (Coração da Camada 3).
    
    Responsável por chamar o gpt-5 mini com um prompt específico
    para gerar um parser Regex a partir de um schema e um exemplo de texto.
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
        # O modelo exato exigido pelo desafio
        self.model = "gpt-5-mini" 
        
    def _build_prompt(self, schema: dict, pdf_texts_aggregate: str) -> str:
        """
        Monta o prompt final (V15) com base no schema mesclado
        e nos MÚLTIPLOS textos de exemplo agregados.
        """
        
        schema_str = json.dumps(schema, indent=2, ensure_ascii=False)
        
        # O template do prompt V15
        prompt_template = f"""
Você é um programador de elite, especialista em criar Expressões Regulares (Regex) robustas em Python.

Sua tarefa é gerar um PARSER. Você receberá um `extraction_schema` (JSON) e um conjunto de `textos_de_exemplo` (strings).

Seu objetivo é criar um conjunto de expressões Regex em Python para extrair CADA campo definido no `extraction_schema`.

REGRAS DE GERAÇÃO:
1.  **Generalização:** Os `textos_de_exemplo` mostram diferentes layouts e campos para o mesmo label. Sua Regex deve ser robusta o suficiente para funcionar em TODOS os exemplos.
2.  **Robustez:** A Regex deve ser resiliente a espaços em branco extras (`\\s*`), quebras de linha (`\\n`), e variações de caixa (use `(?i)` para case-insensitive onde apropriado). Use grupos de captura (parênteses `()`) para isolar o valor exato.
3.  **Campos Opcionais:** Se um campo (ex: "telefone_profissional") aparece em um exemplo mas não em outro, a Regex gerada para ele deve ser *opcional* (ex: não falhar se não encontrar).
4.  **Formato de Saída:** Responda APENAS com um objeto JSON válido. As chaves devem ser EXATAMENTE as chaves do `extraction_schema` de entrada. Os valores devem ser a STRING da Regex.
5.  **NÃO GERE `null`:** Se um campo do schema não for encontrado em NENHUM exemplo, gere a melhor Regex possível que você puder inferir (ex: `(?i)NomeDoCampo\\s*[:\\-]?\\s*(.*)`), mas NÃO retorne `null`.

---
INPUT: EXTRACTION SCHEMA (COMPLETO E MESCLADO)
```json
{schema_str}
```
```Plaintext
{pdf_texts_aggregate}
```
OUTPUT: JSON (APENAS O JSON, SEM nulls NOS VALORES) 
""" 

        return prompt_template.strip()

    def generate_parser(self, schema: dict, pdf_texts_aggregate: str) -> dict | None:
        """
        Chama a API do gpt-5 mini para gerar o parser.
        
        Retorna um dicionário (o parser) em caso de sucesso, ou None em caso de falha.
        """
        
        # 2. O nome do parâmetro mudou para clareza
        prompt = self._build_prompt(schema, pdf_texts_aggregate)
        
        try:
            logging.info(f"Chamando {self.model} para gerar parser (com textos agregados)...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Você é um programador Python especialista em Regex que responde apenas com JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}, 
                # temperature=0.0 # (Manter em 0 para consistência)
            )
            
            response_content = response.choices[0].message.content
            
            parser_dict = json.loads(response_content)
            
            # Verificação final para garantir que ele não gerou 'null'
            for key, value in parser_dict.items():
                if value is None:
                    logging.warning(f"O LLM ignorou a regra e gerou 'null' para {key}. Corrigindo.")
                    # Gera uma Regex "burra" como fallback final
                    parser_dict[key] = f"(?i){re.escape(key)}\\s*[:\\-]?\\s*([^\\n\\r]+)"

            logging.info(f"Parser (V15) gerado com sucesso pelo {self.model}.")
            return parser_dict
            
        except json.JSONDecodeError as e:
            logging.error(f"Falha ao decodificar JSON da resposta do LLM: {e}")
            logging.error(f"Resposta recebida (não-JSON): {response_content}")
            return None
        except Exception as e:
            logging.error(f"Erro ao chamar a API OpenAI: {e}")
            return None