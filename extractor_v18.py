"""
PDF Data Extractor V18 - Arquitetura Híbrida Otimizada
Extração em <10s com >80% acurácia e custo minimizado
"""

import json
import re
import hashlib
import time
from typing import Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import PyPDF2
from openai import OpenAI

# ==================== CONFIGURAÇÃO ====================

class Config:
    """Configurações globais do sistema"""
    OPENAI_MODEL = "gpt-5-mini"  # Usar gpt-5-mini conforme especificado
    MAX_TEXT_LENGTH = 4000  # Truncar texto para economizar tokens
    CACHE_ENABLED = True
    TIMEOUT_SECONDS = 9  # Margem de segurança para 10s
    TEMPERATURE = 0  # Determinismo máximo
    MAX_TOKENS = 500  # Suficiente para extrações
    

# ==================== UTILITÁRIOS ====================

class PDFTextExtractor:
    """Extrai texto de PDFs de forma otimizada"""
    
    @staticmethod
    def extract_text(pdf_path: str) -> str:
        """Extrai texto completo do PDF"""
        try:
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                if len(reader.pages) == 0:
                    return ""
                # Apenas primeira página (conforme requisito)
                text = reader.pages[0].extract_text()
                return text.strip()
        except Exception as e:
            print(f"[ERROR] Falha ao extrair texto: {e}")
            return ""


class CacheManager:
    """Gerencia cache inteligente de extrações"""
    
    def __init__(self):
        self.cache: Dict[str, Dict[str, Any]] = {}
    
    def get_key(self, label: str, text: str) -> str:
        """Gera chave de cache baseada em label e texto"""
        # Usar apenas os primeiros 2000 chars (layout é consistente)
        content = f"{label}:{text[:2000]}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def get(self, label: str, text: str, schema: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Recupera dados do cache se disponível"""
        if not Config.CACHE_ENABLED:
            return None
        
        key = self.get_key(label, text)
        cached_data = self.cache.get(key)
        
        if cached_data:
            # Retornar apenas campos solicitados no schema atual
            return {k: cached_data.get(k) for k in schema.keys()}
        
        return None
    
    def set(self, label: str, text: str, data: Dict[str, Any]):
        """Armazena dados no cache"""
        if Config.CACHE_ENABLED:
            key = self.get_key(label, text)
            self.cache[key] = data
            print(f"[CACHE] Armazenado: {key[:8]}... ({len(data)} campos)")


# ==================== EXTRAÇÃO HEURÍSTICA (PASS 1) ====================

class HeuristicExtractor:
    """Extrator rápido baseado em padrões para campos estruturados"""
    
    # Padrões de regex otimizados
    PATTERNS = {
        'cpf': re.compile(r'\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b'),
        'cnpj': re.compile(r'\b\d{2}\.?\d{3}\.?\d{3}/?0001-?\d{2}\b'),
        'cep': re.compile(r'\b\d{5}-?\d{3}\b'),
        'data': re.compile(r'\b\d{2}[/-]\d{2}[/-]\d{4}\b'),
        'email': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
        'telefone': re.compile(r'\(?\d{2}\)?\s?\d{4,5}-?\d{4}'),
        'valor': re.compile(r'R\$\s*[\d.,]+'),
        'numero': re.compile(r'\b\d{4,}\b'),
    }
    
    @staticmethod
    def detect_field_type(field_name: str) -> Optional[str]:
        """Detecta o tipo de campo pelo nome"""
        field_lower = field_name.lower()
        
        type_keywords = {
            'cpf': ['cpf'],
            'cnpj': ['cnpj'],
            'cep': ['cep'],
            'data': ['data', 'nascimento', 'emissao', 'vencimento'],
            'email': ['email', 'e-mail'],
            'telefone': ['telefone', 'celular', 'fone', 'tel'],
            'valor': ['valor', 'preco', 'total', 'subtotal'],
            'numero': ['numero', 'inscricao', 'protocolo', 'codigo'],
        }
        
        for field_type, keywords in type_keywords.items():
            if any(keyword in field_lower for keyword in keywords):
                return field_type
        
        return None
    
    @staticmethod
    def extract_near_label(text: str, field_name: str, max_distance: int = 100) -> Optional[str]:
        """Extrai valor próximo a um label no texto"""
        # Procurar o nome do campo no texto
        pattern = re.compile(rf'{re.escape(field_name)}[\s:]*(.{{0,{max_distance}}}?)(?:\n|$)', re.IGNORECASE)
        match = pattern.search(text)
        
        if match:
            value = match.group(1).strip()
            # Limpar pontuação no final
            value = re.sub(r'[,;:.!?]+$', '', value)
            return value if value else None
        
        return None
    
    def extract(self, text: str, schema: Dict[str, str]) -> Dict[str, Any]:
        """Extrai campos usando heurísticas"""
        results = {}
        
        for field_name, field_description in schema.items():
            field_type = self.detect_field_type(field_name)
            value = None
            confidence = 0.0
            
            if field_type and field_type in self.PATTERNS:
                # Tentar padrão regex específico
                pattern = self.PATTERNS[field_type]
                match = pattern.search(text)
                
                if match:
                    value = match.group(0)
                    confidence = 0.95  # Alta confiança para matches de regex
            
            # Fallback: procurar valor próximo ao label
            if not value:
                value = self.extract_near_label(text, field_name)
                if value:
                    confidence = 0.7  # Confiança média para proximity match
            
            results[field_name] = {
                'value': value,
                'confidence': confidence,
                'method': 'heuristic'
            }
        
        return results


# ==================== EXTRAÇÃO LLM (PASS 2) ====================

class LLMExtractor:
    """Extrator preciso usando GPT-4o-mini"""
    
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)
    
    def extract(self, text: str, schema: Dict[str, str]) -> Dict[str, Any]:
        """Extrai campos usando LLM"""
        # Truncar texto para economizar tokens
        truncated_text = text[:Config.MAX_TEXT_LENGTH]
        
        # Construir prompt otimizado
        prompt = self._build_prompt(truncated_text, schema)
        
        try:
            response = self.client.chat.completions.create(
                model=Config.OPENAI_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "Você é um extrator preciso de dados de documentos. Retorne APENAS JSON válido."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=Config.TEMPERATURE,
                max_tokens=Config.MAX_TOKENS,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Adicionar metadados
            return {
                field: {
                    'value': result.get(field),
                    'confidence': 0.85,  # Confiança padrão para LLM
                    'method': 'llm'
                }
                for field in schema.keys()
            }
            
        except Exception as e:
            print(f"[ERROR] Falha na extração LLM: {e}")
            return {field: {'value': None, 'confidence': 0.0, 'method': 'llm'} for field in schema.keys()}
    
    @staticmethod
    def _build_prompt(text: str, schema: Dict[str, str]) -> str:
        """Constrói prompt otimizado para o LLM"""
        return f"""Extraia EXATAMENTE os campos especificados do texto do PDF.

REGRAS CRÍTICAS:
1. Se o campo não existe no texto, retorne null
2. Copie o valor EXATAMENTE como aparece (preserve formatação)
3. NÃO invente, NÃO infira, NÃO complete informações
4. Retorne APENAS um objeto JSON com os campos solicitados

SCHEMA (campos a extrair):
{json.dumps(schema, indent=2, ensure_ascii=False)}

TEXTO DO PDF:
{text}

Retorne JSON no formato:
{json.dumps({field: "valor_extraído_ou_null" for field in schema.keys()}, indent=2, ensure_ascii=False)}"""


# ==================== VALIDAÇÃO ====================

class Validator:
    """Valida dados extraídos usando heurísticas por tipo"""
    
    VALIDATORS = {
        'cpf': lambda v: bool(re.match(r'^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$', str(v))),
        'cnpj': lambda v: bool(re.match(r'^\d{2}\.?\d{3}\.?\d{3}/?0001-?\d{2}$', str(v))),
        'cep': lambda v: bool(re.match(r'^\d{5}-?\d{3}$', str(v))),
        'data': lambda v: bool(re.match(r'^\d{2}[/-]\d{2}[/-]\d{4}$', str(v))),
        'email': lambda v: '@' in str(v) and '.' in str(v),
        'telefone': lambda v: len(re.sub(r'\D', '', str(v))) >= 10,
        'numero': lambda v: bool(re.search(r'\d', str(v))),
    }
    
    @staticmethod
    def validate_field(field_name: str, value: Any) -> bool:
        """Valida um campo específico"""
        if value is None:
            return True  # null é sempre válido
        
        # Detectar tipo do campo
        field_type = HeuristicExtractor.detect_field_type(field_name)
        
        if field_type and field_type in Validator.VALIDATORS:
            validator = Validator.VALIDATORS[field_type]
            try:
                return validator(value)
            except:
                return False
        
        # Se não há validador específico, aceitar
        return True
    
    @staticmethod
    def validate_all(data: Dict[str, Any]) -> Tuple[Dict[str, Any], float]:
        """Valida todos os campos e calcula score de confiança"""
        valid_fields = 0
        total_fields = len(data)
        validated_data = {}
        
        for field, value in data.items():
            if Validator.validate_field(field, value):
                validated_data[field] = value
                if value is not None:
                    valid_fields += 1
            else:
                # Rejeitar valor inválido
                print(f"[VALIDATION] Campo '{field}' rejeitado: '{value}'")
                validated_data[field] = None
        
        confidence = valid_fields / total_fields if total_fields > 0 else 0.0
        return validated_data, confidence


# ==================== MERGE INTELIGENTE ====================

class SmartMerger:
    """Combina resultados de múltiplas fontes usando confiança"""
    
    @staticmethod
    def merge(heuristic_results: Dict[str, Any], llm_results: Dict[str, Any]) -> Dict[str, Any]:
        """Merge inteligente priorizando confiança"""
        merged = {}
        
        for field in heuristic_results.keys():
            h_data = heuristic_results.get(field, {})
            l_data = llm_results.get(field, {})
            
            h_value = h_data.get('value')
            h_confidence = h_data.get('confidence', 0.0)
            
            l_value = l_data.get('value')
            l_confidence = l_data.get('confidence', 0.0)
            
            # Heurística tem prioridade se confiança > 0.9
            if h_value and h_confidence > 0.9:
                merged[field] = h_value
            # LLM como fallback
            elif l_value:
                merged[field] = l_value
            # Heurística com confiança média
            elif h_value:
                merged[field] = h_value
            else:
                merged[field] = None
        
        return merged


# ==================== ORQUESTRADOR PRINCIPAL ====================

class PDFDataExtractor:
    """Orquestrador principal da extração V18"""
    
    def __init__(self, openai_api_key: str):
        self.cache = CacheManager()
        self.heuristic_extractor = HeuristicExtractor()
        self.llm_extractor = LLMExtractor(openai_api_key)
        self.merger = SmartMerger()
        self.validator = Validator()
    
    def extract(self, pdf_path: str, label: str, extraction_schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Extrai dados estruturados do PDF
        
        Args:
            pdf_path: Caminho do arquivo PDF
            label: Tipo de documento (ex: "carteira_oab")
            extraction_schema: Dicionário {campo: descrição}
        
        Returns:
            Dicionário com dados extraídos e metadados
        """
        start_time = time.time()
        print(f"\n{'='*60}")
        print(f"[EXTRAÇÃO] Iniciando - Label: {label}")
        print(f"[EXTRAÇÃO] Campos solicitados: {len(extraction_schema)}")
        
        # 1. Extrair texto do PDF (~0.1s)
        text = PDFTextExtractor.extract_text(pdf_path)
        if not text:
            return self._error_response("Falha ao extrair texto do PDF")
        
        print(f"[PDF] Texto extraído: {len(text)} caracteres")
        
        # 2. Verificar cache
        cached_result = self.cache.get(label, text, extraction_schema)
        if cached_result:
            elapsed = time.time() - start_time
            print(f"[CACHE HIT] Dados recuperados em {elapsed:.3f}s")
            return self._success_response(cached_result, elapsed, cache_hit=True)
        
        print("[CACHE MISS] Executando extração completa")
        
        # 3. Extração paralela (Pass 1 + Pass 2)
        heuristic_results = {}
        llm_results = {}
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submeter ambas extrações em paralelo
            future_heuristic = executor.submit(
                self.heuristic_extractor.extract, text, extraction_schema
            )
            future_llm = executor.submit(
                self.llm_extractor.extract, text, extraction_schema
            )
            
            # Aguardar conclusão
            for future in as_completed([future_heuristic, future_llm], timeout=Config.TIMEOUT_SECONDS):
                if future == future_heuristic:
                    heuristic_results = future.result()
                    print(f"[PASS 1] Heurísticas concluídas")
                elif future == future_llm:
                    llm_results = future.result()
                    print(f"[PASS 2] LLM concluído")
        
        # 4. Merge inteligente
        merged_data = self.merger.merge(heuristic_results, llm_results)
        print(f"[MERGE] Dados combinados")
        
        # 5. Validação
        validated_data, confidence = self.validator.validate_all(merged_data)
        print(f"[VALIDATION] Confiança: {confidence:.2%}")
        
        # 6. Atualizar cache (assíncrono - não conta no tempo)
        self.cache.set(label, text, validated_data)
        
        # 7. Retornar resultado
        elapsed = time.time() - start_time
        print(f"[CONCLUSÃO] Tempo total: {elapsed:.3f}s")
        
        return self._success_response(validated_data, elapsed, cache_hit=False, confidence=confidence)
    
    def _success_response(self, data: Dict[str, Any], time_elapsed: float, 
                         cache_hit: bool, confidence: float = 1.0) -> Dict[str, Any]:
        """Formata resposta de sucesso"""
        return {
            'success': True,
            'data': data,
            'metadata': {
                'time_seconds': round(time_elapsed, 3),
                'cache_hit': cache_hit,
                'confidence': round(confidence, 2),
                'fields_extracted': sum(1 for v in data.values() if v is not None),
                'fields_total': len(data)
            }
        }
    
    def _error_response(self, error_message: str) -> Dict[str, Any]:
        """Formata resposta de erro"""
        return {
            'success': False,
            'error': error_message,
            'data': {}
        }


# ==================== EXEMPLO DE USO ====================

if __name__ == "__main__":
    # Configurar API key
    import os
    API_KEY = os.getenv("OPENAI_API_KEY", "sua-api-key-aqui")
    
    # Inicializar extrator
    extractor = PDFDataExtractor(openai_api_key=API_KEY)
    
    # Exemplo de extração
    result = extractor.extract(
        pdf_path="exemplo.pdf",
        label="carteira_oab",
        extraction_schema={
            "nome": "Nome do profissional",
            "inscricao": "Número de inscrição",
            "seccional": "Seccional",
            "categoria": "Categoria do profissional",
            "situacao": "Situação do profissional"
        }
    )
    
    # Exibir resultado
    print("\n" + "="*60)
    print("RESULTADO:")
    print(json.dumps(result, indent=2, ensure_ascii=False))