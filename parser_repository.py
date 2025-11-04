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

    def _get_lock_filepath(self, label: str) -> str:
        """
        Gera um nome de arquivo seguro para o 'lock' de um label.
        Ex: 'parser_repository_cache/carteira_oab.parser.lock'
        """
        safe_filename = "".join(c for c in label if c.isalnum() or c in ('_', '-')).rstrip()
        return os.path.join(self.cache_dir, f"{safe_filename}.parser.lock")

    def is_generation_locked(self, label: str) -> bool:
        """
        Verifica se um 'lock' já existe, indicando que a geração
        do parser para este label JÁ ESTÁ EM ANDAMENTO.
        """
        lock_path = self._get_lock_filepath(label)
        return os.path.exists(lock_path)

    def create_lock(self, label: str):
        """
        Cria o arquivo .lock para sinalizar que a geração começou.
        """
        lock_path = self._get_lock_filepath(label)
        try:
            # Cria um arquivo vazio
            with open(lock_path, 'w') as f:
                pass
            logging.info(f"LOCK CRIADO: Geração do parser para '{label}' iniciada.")
        except IOError as e:
            logging.error(f"Falha ao criar lock para '{label}': {e}")

    def remove_lock(self, label: str):
        """
        Remove o arquivo .lock após a geração (seja sucesso ou falha).
        """
        lock_path = self._get_lock_filepath(label)
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
                logging.info(f"LOCK REMOVIDO: Geração do parser para '{label}' concluída.")
        except IOError as e:
            logging.error(f"Falha ao remover lock para '{label}': {e}")