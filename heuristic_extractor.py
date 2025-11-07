# Módulo: heuristic_extractor.py
# (V18.3: "Fallback Síncrono Inteligente" - Baseado em Descrição)

import re
import logging
from typing import Dict, Optional, List

class HeuristicExtractor:
    """
    Implementa o "Fallback Síncrono Local" (V18.3).
    
    Esta versão é mais inteligente que a V18.1.
    Em vez de usar Regex "fracas" baseadas apenas nas *chaves* do schema
    (o que gerou dados ruins),
    esta versão usa as *descrições* do schema para gerar
    Regex heurísticas melhores [baseado na sua ideia].
    
    O objetivo ainda é garantir o tempo de < 10s, mas com
    uma acurácia "aceitável" para o Caminho Lento.
    """
    
    def _get_keywords_from_description(self, description: str) -> List[str]:
        """
        Extrai palavras-chave da descrição do schema.
        Ex: "Número de inscrição do profissional" -> ["Número de inscrição", "inscrição"]
        """
        # Remove palavras de parada comuns (stopwords)
        description = description.lower()
        stopwords = {"do", "da", "de", "o", "a", "para", "com", "sem"}
        palavras_limpas = [p for p in re.split(r'\s+', description) if p not in stopwords]
        
        # Gera n-gramas (ex: "número de inscrição")
        keywords = []
        if len(palavras_limpas) > 1:
            keywords.append(" ".join(palavras_limpas[:3])) # "número de inscrição"
            keywords.append(" ".join(palavras_limpas[:2])) # "número de" (menos útil)
        if palavras_limpas:
            keywords.append(palavras_limpas[0]) # "número"
        
        # Remove duplicatas e prioriza os mais longos
        keywords = sorted(list(set(keywords)), key=len, reverse=True)
        return keywords

    def _generate_smart_regex(self, field_name: str, description: str) -> str:
        """
        Gera uma Regex heurística (V18.3) baseada na descrição E na chave.
        """
        
        keywords = []
        
        # 1. Palavras-chave da Descrição (Sua ideia)
        if description:
            keywords.extend(self._get_keywords_from_description(description))
        
        # 2. Palavra-chave da Chave (Fallback da V18.1)
        # Substitui '_' por " " para buscar (ex: "data_base" -> "data base")
        key_keyword = field_name.replace("_", " ")
        keywords.append(key_keyword)
        
        # 3. A própria chave (para o caso de `data_base: ...`)
        keywords.append(field_name)
        
        # 4. Remove duplicatas (mantendo a ordem de prioridade)
        keywords_unicas = []
        for k in keywords:
            if k not in keywords_unicas:
                keywords_unicas.append(k)
        
        # 5. Constrói o Padrão de Busca (ex: "Número de inscrição" OU "inscrição")
        # Escapa caracteres de Regex
        patterns_escaped = [re.escape(k).replace(r"\ ", r"[\s_]+") for k in keywords_unicas]
        pattern_str = "|".join(patterns_escaped)

        # 6. Constrói a Regex Final
        # Tenta capturar um valor numérico se a chave/descrição sugerir
        if any(kw in field_name for kw in ["inscricao", "numero", "cep", "id"]):
            # Regex mais restritiva para números
            return f"(?i)(?:{pattern_str})\s*[:\\-]?\s*([0-9.,\\-/]+)"
        
        # Tenta capturar uma data
        if any(kw in field_name for kw in ["data", "date"]):
             return f"(?i)(?:{pattern_str})\s*[:\\-]?\s*(\d{2}/\d{2}/\d{4})"

        # Regex genérica (default): captura o resto da linha
        return f"(?i)(?:{pattern_str})\s*[:\\-]?\s*([^\n\r]+)"

    def extract(self, pdf_text: str, schema: Dict[str, str]) -> Dict[str, Optional[str]]:
        """
        Executa a extração heurística inteligente (V18.3).
        """
        extracted_data = {}
        logging.info("Acionando Módulo de Fallback Local (Heurístico V18.3 - Inteligente)...")

        for field_name, field_description in schema.items():
            
            # Gera a Regex V18.3 usando a descrição
            regex_pattern = self._generate_smart_regex(field_name, field_description or "")
            
            try:
                match = re.search(regex_pattern, pdf_text)
                
                if match:
                    value = match.group(1)
                    extracted_data[field_name] = value.strip() if value else None
                else:
                    # A heurística V18.3 falhou em encontrar
                    extracted_data[field_name] = None
                    
            except re.error as e:
                logging.error(f"HEURÍSTICA (V18.3): Erro de Regex para '{field_name}': {e}")
                extracted_data[field_name] = None

        logging.info("Fallback Heurístico (V18.3) concluído.")
        return extracted_data