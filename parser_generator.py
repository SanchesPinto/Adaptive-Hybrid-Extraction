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
        Monta o prompt "V6" (agora também robusto para campos vazios).
        
        Este prompt instrui o LLM a:
        1. Fazer Mapeamento Semântico (ex: "data_vencimento" -> "VENCIMENTO")
        2. Analisar o Layout (Princípio 1)
        3. Planejar a Invariância (Princípio 2)
        4. Usar os rótulos mapeados como Âncoras (Princípio 3)
        5. Usar as chaves do schema como Nomes dos Grupos de Captura
        6. Retornar um JSON estruturado com uma ÚNICA string de Regex.
        """
        
        # Converte o schema dict para uma string JSON formatada
        schema_str = json.dumps(schema, indent=2, ensure_ascii=False)
        
        # O template do prompt V4
        # Note que as barras invertidas do Regex (ex: \s) são escapadas (\\s)
        # para que sejam lidas corretamente como strings literais.
        prompt_template = f"""
# FUNÇÃO
Você é um motor de geração de parser de elite, especialista em Expressões Regulares (PCRE) em Python.
Sua função é analisar um `extraction_schema` e um `texto_pdf_exemplo` para gerar um **único** parser de Expressão Regular que seja semanticamente robusto e invariante a layout.
Você opera com "precisão cirúrgica".

# ENTRADAS
## 1. extraction_schema (JSON)
```json
{schema_str}
```

## 2. texto_pdf_exemplo (STRING)
```plaintext
{pdf_text}
```
REGRAS DE GERAÇÃO (CRÍTICO)
    1. MAPEAMENTO SEMÂNTICO: Sua primeira tarefa é analisar o extraction_schema e o texto_pdf_exemplo. Para CADA chave no schema (ex: "data_vencimento"), encontre o rótulo de texto literal mais provável no PDF (ex: "VENCIMENTO" ou "Vencim. Data"). Chaves e rótulos podem ser idênticos (ex: "seccional" -> "Seccional").
      1.1. CAMPOS NÃO ENCONTRADOS: Se um campo do schema (ex: "campo_fantasma") for impossível de encontrar no texto_pdf_exemplo, seu valor no semantic_mapping deve ser null.
      1.2. CAMPOS SEM VALOR: Se um rótulo de campo (ex: "Telefone Profissional") for encontrado, mas NÃO HOUVER VALOR associado a ele (ex: está seguido por outro rótulo ou pelo fim do texto), o mapeamento deve ser null.
    2. INVARIÂNCIA DE LAYOUT: A Regex gerada deve ser robusta a variações de layout . O exemplo fornecido é apenas uma possibilidade. O valor pode estar na mesma linha que o rótulo (ex: 'Seccional: PR'), na linha abaixo, ou em uma coluna adjacente. A Regex deve capturar o valor semanticamente.
    3. RESTRIÇÃO NEGATIVA: Não crie uma Regex que dependa da ordem das colunas ou de uma contagem fixa de colunas .
    4. ANCORAGEM SEMÂNTICA: A lógica da Regex deve usar os rótulos do PDF que você identificou na Regra 1 (ex: "VENCIMENTO") como âncoras para localizar os valores .
    5. DEFINIÇÃO DE LIMITES: A captura do valor deve ser o mais restrita possível (usar quantificadores non-greedy .*?) . O grupo de captura para um valor deve parar antes de encontrar o final da linha (\\n) ou o início de qualquer outro rótulo que você identificou .
    6. GRUPOS DE CAPTURA NOMEADOS: A Regex deve usar grupos de captura nomeados (named capture groups) (ex: ?<nome_do_campo>...) . O nome do grupo deve corresponder exatamente à CHAVE original do extraction_schema (ex: ?<data_vencimento>...), e não ao rótulo do PDF.

FORMATO DE SAÍDA OBRIGATÓRIO (SCHEMA JSON)

Você *deve* responder *apenas* com um objeto JSON válido que corresponda ao seguinte schema. Preencha cada campo com seu raciocínio.

```json
{{
  "type": "object",
  "properties": {{
    "layout_analysis": {{
      "type": "string",
      "description": "Sua análise verbal do padrão de layout do exemplo (Princípio 1)."
    }},
    "semantic_mapping": {{
      "type": "object",
      "description": "Mapeamento da CHAVE do schema para o RÓTULO literal no PDF. Se o rótulo não for encontrado ou não tiver valor, mapeie para `null`. Ex: {{'telefone_profissional': null, 'seccional': 'Seccional'}}"
    }},
    "invariance_plan": {{
      "type": "string",
      "description": "Como sua Regex lidará com variações de layout (Princípio 2)."
    }},
    "boundary_logic": {{
      "type": "string",
      "description": "Como sua Regex evita capturar texto adjacente (Regra 5)."
    }},
    "generated_regex_dict": {{
      "type": "object",
      "description": "Objeto JSON de pares chave-valor. A CHAVE é a chave do schema (ex: 'data_vencimento'). O VALOR é a string Regex INDIVIDUAL (com um grupo de captura `()`) para extrair esse campo. **Se o campo foi mapeado para `null` na Regra 1, o valor aqui também deve ser `null`**."
    }}
  }},
  "required": ["layout_analysis", "semantic_mapping", "invariance_plan", "boundary_logic", "generated_regex_dict"]
}}


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