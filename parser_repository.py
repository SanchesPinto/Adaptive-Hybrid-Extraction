import json
import os
import logging

# Define o diretório padrão
PARSER_CACHE_DIR = "parser_repository_cache" 

class ParserRepository:
    
    def __init__(self, cache_dir=PARSER_CACHE_DIR):
        self.cache_dir = cache_dir
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            logging.info(f"Repositório de parsers criado em: {self.cache_dir}")

    def _get_parser_filepath(self, label: str) -> str:
        # ... (resto do código da classe) ...
        safe_filename = "".join(c for c in label if c.isalnum() or c in ('_', '-')).rstrip()
        return os.path.join(self.cache_dir, f"{safe_filename}.parser.json")

    def get_parser(self, label: str) -> dict | None:
        # ... (resto do código da classe) ...
        filepath = self._get_parser_filepath(label)
        
        if not os.path.exists(filepath):
            logging.warning(f"CACHE MISS para o label: {label}")
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                parser_data = json.load(f)
                logging.info(f"CACHE HIT: Parser encontrado para o label: {label}")
                return parser_data
        except json.JSONDecodeError:
            logging.error(f"CORRUPÇÃO: O parser para {label} está mal formatado. Tratando como Cache Miss.")
            return None

    def save_parser(self, label: str, parser_data: dict):
        # ... (resto do código da classe) ...
        filepath = self._get_parser_filepath(label)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(parser_data, f, indent=2, ensure_ascii=False)
                logging.info(f"CONHECIMENTO ACUMULADO: Novo parser salvo para o label: {label}")
        except IOError as e:
            logging.error(f"Falha ao salvar o parser para {label}: {e}")