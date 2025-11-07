import os
import json
import logging
import re
from openai import OpenAI
from dotenv import load_dotenv
from typing import Optional

# Carrega as variáveis de ambiente (OPENAI_API_KEY)
load_dotenv()

class ParserGenerator:
    """
    Implementa o "Módulo 1: Gerador de Parser" (V16.1).
    
    O prompt V16.1 foi reescrito para forçar o LLM
    a priorizar a engenharia reversa do GABARITO e do TEXTO,
    em vez de gerar Regex "preguiçosas" baseadas nas chaves.
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
        Monta o prompt final (V17.3) - Foco em acurácia contextual e validação semântica.
        """

        schema_str = json.dumps(schema, indent=2, ensure_ascii=False)
        json_example_str = json.dumps(correct_json_example, indent=2, ensure_ascii=False)

        prompt_template = f"""
Você é um engenheiro especialista em **engenharia reversa de documentos**.  
Sua função é criar um **PARSER altamente preciso** (em formato JSON) capaz de extrair informações de um texto PDF usando **expressões regulares (Regex)** em Python.

O objetivo é gerar um parser que, ao ser executado, produza **exatamente o mesmo JSON do gabarito**, campo por campo, com alta confiabilidade.

---

### 📘 INFORMAÇÕES DE ENTRADA

1. **EXTRACTION_SCHEMA** — estrutura completa que define todas as chaves que o parser deve conter.
2. **TEXTO_PDF_EXEMPLO** — um exemplo real de texto extraído de um PDF.
3. **JSON_DE_GABARITO** — o resultado correto esperado ao aplicar o parser ao texto de exemplo.

---

### ⚙️ REGRAS DE GERAÇÃO

1. **REGRA DE OURO — Eng. Reversa guiada pelo gabarito:**
   - Cada campo do JSON de saída deve corresponder diretamente ao valor encontrado no `JSON_DE_GABARITO`.
   - Analise o texto do PDF e **descubra como aquele valor aparece** (ex: rótulo, posição, linha adjacente, padrão de data, valor numérico etc.).
   - Crie a Regex com base **no contexto textual real** do PDF, não no nome da chave.
   - Exemplo:
     - ✅ Correto: `(?i)Data\\s*Refer[eê]ncia\\s*[:\\-]?\\s*([0-3]?\\d/[01]?\\d/\\d{4})`
     - ❌ Errado: `(?i)data_base\\s*[:\\-]?\\s*([^\\n\\r]+)`

2. **REGRA DE ROBUSTEZ:**
   - As Regex devem:
     - Usar `(?i)` (case-insensitive) e `(?m)` (multi-line) sempre que aplicável.
     - Tolerar pequenas variações de espaçamento e acentuação (`Refer[eê]ncia`, `Subse[cç][aã]o` etc.).
     - Evitar *capturas gulosas* (`.+`, `.*`) — prefira quantificadores limitados e classes de caracteres específicas.
     - Considerar o uso de `(?=\r?\n\s*PRÓXIMO_RÓTULO|$)` para delimitar blocos.

3. **REGRA DE FALHA (Campos nulos no gabarito):**
   - Se o valor do gabarito for `null`, significa que o campo não foi encontrado no texto.
   - Nesse caso, crie uma Regex genérica baseada no nome da chave, mas com um padrão prudente.
   - Exemplo: `"telefone_profissional": "(?i)Telefone\\s+Profissional\\s*[:\\-]?\\s*([^\\r\\n]+)"`

4. **REGRA DE FORMATO DE SAÍDA:**
   - O resultado deve ser **um único objeto JSON válido**.
   - Cada chave deve corresponder **exatamente** às chaves do `EXTRACTION_SCHEMA`.
   - O valor de cada chave deve ser uma **string contendo a Regex**.
   - Nunca inclua comentários, explicações, `null` ou texto fora do JSON.

---

### 🧠 MODO DE RACIOCÍNIO RECOMENDADO

Antes de gerar o JSON final:
1. Leia cuidadosamente o `TEXTO_PDF_EXEMPLO`.
2. Compare cada valor do `JSON_DE_GABARITO` com o texto original para entender **como o dado é apresentado**.
3. Crie Regex **contextual**, alinhada ao modo como o valor aparece (rótulo, linha, tabela, etc.).
4. Gere o JSON de Regex somente após essa análise.

---

### 📥 ENTRADAS

**INPUT 1 — EXTRACTION_SCHEMA:**
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

📤 SAÍDA ESPERADA
Responda apenas com o objeto JSON final contendo as Regex, no formato:
```json
{{ "campo_1": "REGEX_1", "campo_2": "REGEX_2", ... }}
```json
Nada além disso deve ser incluído.
""" 

        return prompt_template.strip()

    def generate_parser(self, 
                    schema: dict, 
                    pdf_text: str, 
                    correct_json_example: dict) -> Optional[dict]:
        """
        Chama a API do gpt-5 mini para gerar o parser (V16.1).
        
        Args:
            schema: O schema mesclado completo (ex: 14 campos).
            pdf_text: O texto do primeiro item (ex: Item 4).
            correct_json_example: O JSON extraído pelo Fallback (ex: 7 campos).
            
        Returns:
            Um dicionário (o parser) em caso de sucesso, ou None em caso de falha.
        """
        prompt = self._build_prompt(schema, pdf_text, correct_json_example)
        
        try:
            logging.info(f"Chamando {self.model} para gerar parser (com gabarito V16.1)...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Você é um programador Python especialista em Regex que responde apenas com JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}, 
                # temperature=0.0 # Baixa temperatura para seguir regras
            )
            
            response_content = response.choices[0].message.content
            
            parser_dict = json.loads(response_content)
            
            # Verificação final para garantir que ele não gerou 'null'
            for key, value in parser_dict.items():
                if value is None:
                    logging.warning(f"O LLM (V16.1) ignorou a regra e gerou 'null' para {key}. Corrigindo com fallback genérico.")
                    # Aplica a "Lógica de Fallback (Campos null)"
                    parser_dict[key] = f"(?i){re.escape(key)}\\s*[:\\-]?\\s*([^\\n\\r]+)"

            logging.info(f"Parser (V16.1) gerado com sucesso pelo {self.model}.")
            return parser_dict
            
        except json.JSONDecodeError as e:
            logging.error(f"Falha ao decodificar JSON da resposta do LLM: {e}")
            logging.error(f"Resposta recebida (não-JSON): {response_content}")
            return None
        except Exception as e:
            logging.error(f"Erro ao chamar a API OpenAI: {e}")
            return None
        

        