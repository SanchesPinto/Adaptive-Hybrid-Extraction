import os
import json
import logging
from openai import OpenAI
from dotenv import load_dotenv
from typing import Dict, Optional

# Carrega as variáveis de ambiente (OPENAI_API_KEY)
load_dotenv()

class FallbackExtractor:
    """
    Implementa o "Módulo de Fallback" (Camada 2).
    
    Responsável por chamar o gpt-5 mini para EXTRAIR DADOS diretamente
    quando o "Caminho Rápido" (Módulos 1/2/3) falha.
    """
    
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logging.error("OPENAI_API_KEY não encontrada. Verifique seu arquivo .env")
            raise ValueError("API key da OpenAI não configurada.")
            
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-5-mini" # O modelo do desafio [cite: 76]
        
    def _build_extraction_prompt(self, 
                                 schema_to_extract: dict, 
                                 pdf_text: str, 
                                 partial_data: Optional[dict] = None) -> str:
        """
        Monta o prompt para EXTRAÇÃO DE DADOS.
        Este prompt é diferente do prompt de GERAÇÃO DE PARSER.
        """
        schema_str = json.dumps(schema_to_extract, indent=2, ensure_ascii=False)
        
        # O prompt muda se for uma extração completa ou apenas de campos faltantes
        if partial_data:
            # --- Prompt para Campos Faltantes (Otimizado) ---
            logging.info(f"Realizando extração de campos faltantes...")
            partial_data_str = json.dumps(partial_data, indent=2, ensure_ascii=False)
            task_instruction = (
                f"Sua tarefa é encontrar APENAS os campos definidos no `SCHEMA_FALTANTE` "
                f"e ignorar os campos que já foram encontrados em `DADOS_PARCIAIS`."
            )
            context_str = f"""
                ---
                SCHEMA_FALTANTE (O que você deve encontrar):
                ```json 
                {schema_str}
                ```
                ---
                DADOS_PARCIAIS (O que já foi encontrado, use como contexto):
                ```json
                {partial_data_str}
                ```
                """     
        else: # --- Prompt para Extração Completa --- 
            logging.info(f"Realizando extração completa...")
            task_instruction = ( "Sua tarefa é extrair as informações do TEXTO_PDF " "e formatá-las de acordo com o EXTRACTION_SCHEMA, campos não encontrados podem ser preenchidos com null." ) 
            context_str = f"""
            ---
            EXTRACTION_SCHEMA:
            ```json
            {schema_str}
            ```
            """
        # O resto do prompt é o mesmo para ambos os modos

        prompt_template = f"""
        Você é um assistente de extração de dados preciso e que segue regras.

        
        {task_instruction} 
               
        {context_str}
        
        ---
        TEXTO_PDF:
        ```text
        {pdf_text}
        ```
        ---
        OUTPUT: JSON (APENAS O JSON)
        """

        return prompt_template.strip()

    def _call_llm_extractor(self, prompt: str) -> Optional[dict]:
        """
        MÉTODO PRIVADO (O "Trabalhador da API")
        
        Sua única função é chamar a API da OpenAI.
        Nós o separamos para que 'extract_all' e 'extract_missing'
        possam ambos usá-lo sem duplicar o código try/except.
        """
        try:
            logging.info(f"Acionando Fallback: Chamando {self.model} para extração direta...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Você é um assistente de extração de dados que responde apenas com JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}, 
                # temperature=0.0
            )
            
            response_content = response.choices[0].message.content
            extracted_data = json.loads(response_content)
            logging.info("Fallback: Extração de dados via LLM concluída.")
            return extracted_data
            
        except Exception as e:
            logging.error(f"Fallback: Erro ao chamar a API OpenAI: {e}")
            return None
        
    def extract_all(self, schema: dict, pdf_text: str) -> Optional[dict]:
        """
        Cenário de Falha Total: Extrai TODOS os campos do schema.
        """
        logging.warning("Fallback: Acionado para extração completa.")
        prompt = self._build_extraction_prompt(schema, pdf_text)
        return self._call_llm_extractor(prompt)

    def extract_missing(self, 
                        missing_schema: dict, 
                        pdf_text: str, 
                        partial_data: dict) -> Optional[dict]:
        """
        Cenário de Falha Parcial: Extrai APENAS os campos faltantes.
        """
        logging.warning(f"Fallback: Acionado para campos faltantes: {list(missing_schema.keys())}")
        prompt = self._build_extraction_prompt(missing_schema, pdf_text, partial_data)
        return self._call_llm_extractor(prompt)