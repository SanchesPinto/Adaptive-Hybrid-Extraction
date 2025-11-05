import json
import os
import logging
from typing import Dict, Any, Optional, Tuple

KNOWLEDGE_CACHE_DIR = "knowledge_repository_cache" 

class KnowledgeRepository:
    """
    Implementa o "Banco de Dados Local Permanente" (diferencial).
    
    Substitui o antigo ParserRepository. Este módulo salva e carrega
    o "conhecimento acumulado"  para um label.
    
    O conhecimento é um dict contendo:
    - "merged_schema": O schema completo que o sistema já viu.
    - "parser_v4": O parser V4 gerado (com "generated_regex", etc.).
    """
    
    def __init__(self, cache_dir=KNOWLEDGE_CACHE_DIR):
        self.cache_dir = cache_dir
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            logging.info(f"Repositório de Conhecimento criado em: {self.cache_dir}")

    def _get_knowledge_filepath(self, label: str) -> str:
        """Gera um nome de arquivo seguro para o label."""
        safe_filename = "".join(c for c in label if c.isalnum() or c in ('_', '-')).rstrip()
        return os.path.join(self.cache_dir, f"{safe_filename}.knowledge.json")

    def get_knowledge(self, label: str) -> Optional[Dict[str, Any]]:
        """
        Carrega o conhecimento acumulado (schema mesclado + parser) para um label.
        
        Retorna:
            O dict de conhecimento, ou None se for um Cache Miss.
        """
        filepath = self._get_knowledge_filepath(label)
        
        if not os.path.exists(filepath):
            logging.warning(f"KNOWLEDGE-MISS para o label: {label}")
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                knowledge = json.load(f)
                logging.info(f"KNOWLEDGE-HIT: Conhecimento encontrado para o label: {label}")
                return knowledge
        except json.JSONDecodeError:
            logging.error(f"CORRUPÇÃO: O arquivo de conhecimento para {label} está mal formatado.")
            return None

    def save_knowledge(self, label: str, knowledge_data: Dict[str, Any]):
        """
        Salva o conhecimento acumulado (schema mesclado + parser) para um label.
        """
        filepath = self._get_knowledge_filepath(label)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(knowledge_data, f, indent=2, ensure_ascii=False)
                logging.info(f"CONHECIMENTO ACUMULADO: Conhecimento salvo para o label: {label}")
        except IOError as e:
            logging.error(f"Falha ao salvar o conhecimento para {label}: {e}")
            