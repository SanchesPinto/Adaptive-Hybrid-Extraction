import os
import json
import logging
from openai import OpenAI
from dotenv import load_dotenv

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
        
    def _build_prompt(self, schema: dict, pdf_text: str) -> str:
        """
        Monta o prompt final com base no template, inserindo o schema
        e o texto do PDF como exemplos.
        """
        
        # Converte o schema dict para uma string JSON formatada
        schema_str = json.dumps(schema, indent=2, ensure_ascii=False)
        
        # O template do prompt que definimos
        # Note que as barras invertidas do Regex (ex: \s) são escapadas (\\s)
        # para que sejam lidas corretamente como strings literais.
        prompt_template = f"""
Você é um programador de elite, especialista em criar Expressões Regulares (Regex) robustas em Python.

Sua tarefa é gerar um PARSER. Você receberá um `extraction_schema` (JSON) e um `texto_pdf` (string) como exemplo.

Seu objetivo é criar um conjunto de expressões Regex em Python para extrair CADA campo definido no `extraction_schema` a partir do `texto_pdf`.

REGRAS DE GERAÇÃO:
1.  **Robustez:** A Regex deve ser o mais robusta possível. Use grupos de captura (parênteses `()`) para isolar o valor exato a ser extraído. A Regex deve ser resiliente a espaços em branco extras (`\\s*`) e quebras de linha (`\\n`) entre o rótulo (ex: "Inscrição") e o valor (ex: "101943").
2.  **Multilinha:** Para valores que podem ter quebras de linha (como 'nome' ou 'endereco'), use padrões (como `[\\s\\S]+?`) que capturem texto multilinha de forma não-gananciosa.
3.  **Campos Não Encontrados:** Se um campo do `extraction_schema` for impossível de encontrar no `texto_pdf` de exemplo (como "telefone_profissional" no exemplo), o valor para essa chave no JSON de saída deve ser `null`.
4.  **Formato de Saída:** Responda APENAS com um objeto JSON válido. As chaves devem ser EXATAMENTE as chaves do `extraction_schema` de entrada. Os valores devem ser a STRING da Regex (ou `null`). Não inclua explicações, apenas o JSON.

---
INPUT: EXTRACTION SCHEMA
```json
{schema_str}
{pdf_text}
"""
        return prompt_template.strip()

    def generate_parser(self, schema: dict, pdf_text: str) -> dict | None:
        """
        Chama a API do gpt-5 mini para gerar o parser.
        
        Retorna um dicionário (o parser) em caso de sucesso, ou None em caso de falha.
        """
        prompt = self._build_prompt(schema, pdf_text)
        
        try:
            logging.info(f"Chamando {self.model} para gerar parser...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Você é um programador Python especialista em Regex que responde apenas com JSON."},
                    {"role": "user", "content": prompt}
                ],
                # Força o LLM a responder em formato JSON (essencial para nossa arquitetura)
                response_format={"type": "json_object"}, 
                # temperature=0.0 # Temperatura 0 para resultados determinísticos/consistentes
            )
            
            response_content = response.choices[0].message.content
            
            # Analisa a string JSON recebida para um dict Python
            parser_dict = json.loads(response_content)
            
            logging.info(f"Parser gerado com sucesso pelo {self.model}.")
            return parser_dict
            
        except json.JSONDecodeError as e:
            logging.error(f"Falha ao decodificar JSON da resposta do LLM: {e}")
            logging.error(f"Resposta recebida (não-JSON): {response_content}")
            return None
        except Exception as e:
            logging.error(f"Erro ao chamar a API OpenAI: {e}")
            return None