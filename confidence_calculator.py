import logging
import re
from typing import Dict, Optional, Callable, Any

class ConfidenceCalculator:
    """
    Implementa o "Módulo 3.1: O Sistema de Confiança Robusto" (V16.1).
    
    [cite_start]Substitui a verificação ingênua de "está preenchido?" [cite: 17, 21-22] por um
    sistema de validação heurística por campo.
    
    Isso nos protege contra FALSOS POSITIVOS, onde o Módulo 2 (Regex)
    retorna lixo (ex: "inscricao" = "Seccional").
    """

    def __init__(self):
        # Cache de validadores para não precisar recriar as regras
        self._validator_cache: Dict[str, Dict[str, Callable]] = {}
        # Cache para dados cruzados (ex: 'nome' não pode ser igual a 'situacao')
        self._cross_data_cache: Dict[str, Any] = {}

    # --- REGEX DE VALIDAÇÃO GERAL ---
    
    def _is_numeric_like(self, value: str) -> bool:
        """Verifica se o valor parece numérico e não é um rótulo."""
        if not value: return False
        # Verifica se contém um dígito E não é um rótulo conhecido
        return bool(re.search(r'\d', value) and not self._is_generic_label(value))

    def _is_generic_label(self, value: str) -> bool:
        """Verifica se o valor é um rótulo genérico que vazou."""
        if not value: return False
        # Lista de rótulos comuns que podem vazar
        return value.lower().strip() in ["seccional", "subseção", "inscrição", "nome", "situação regular"]

    def _is_date(self, value: str) -> bool:
        """Verifica se o valor parece uma data (formato DD/MM/YYYY)."""
        if not value: return False
        return bool(re.match(r'^\d{2}/\d{2}/\d{4}$', value.strip()))

    def _is_in_enum(self, value: str, enum_list: list) -> bool:
        """Verifica se o valor está numa lista de valores esperados (case-insensitive)."""
        if not value: return False
        return value.lower().strip() in enum_list
    
    def _is_not_empty_and_not_label(self, value: str) -> bool:
        """Validador genérico: apenas não pode ser nulo ou um rótulo."""
        if not value: return False
        return not self._is_generic_label(value)

    # --- VALIDADORES ESPECIALIZADOS POR LABEL ---

    def _get_validation_rules(self, label: str) -> Dict[str, Callable]:
        """
        Factory que retorna o conjunto de regras de validação
        correto para um determinado 'label'.
        """
        if label in self._validator_cache:
            return self._validator_cache[label]

        rules = {}
        if label == "carteira_oab":
            rules = {
                # Validação Cruzada: nome não pode ser igual a 'situacao'
                "nome": lambda v: (
                    self._is_not_empty_and_not_label(v) and len(v) > 2 and 
                    v.lower().strip() != self._cross_data_cache.get("situacao", "").lower().strip()
                ),
                "inscricao": lambda v: self._is_numeric_like(v),
                "seccional": lambda v: bool(v and len(v.strip()) == 2 and not v.strip().isdigit()), # Ex: "PR"
                "subsecao": lambda v: self._is_not_empty_and_not_label(v) and len(v) > 5,
                "categoria": lambda v: self._is_in_enum(v, ["advogado", "advogada", "suplementar", "estagiario", "estagiaria"]),
                # Validação Cruzada: situacao não pode ser igual a 'nome'
                "situacao": lambda v: (
                    self._is_in_enum(v, ["situação regular"]) and 
                    v.lower().strip() != self._cross_data_cache.get("nome", "").lower().strip()
                )
                # 'endereco' e 'telefone' são muito variáveis, 
                # a checagem padrão (não nulo) será aplicada.
            }
        
        elif label == "tela_sistema":
            rules = {
                "data_base": lambda v: self._is_date(v),
                "data_verncimento": lambda v: self._is_date(v),
                "valor_parcela": lambda v: bool(v and re.search(r'[\d.,]', v)),
                "cidade": lambda v: bool(v and len(v) > 3),
                "data_referencia": lambda v: self._is_date(v)
            }
        
        # Cache para performance
        self._validator_cache[label] = rules
        return rules

    # --- MÉTODO PRINCIPAL ---

    def calculate_confidence(self, 
                             extracted_data: Dict[str, Optional[str]], 
                             schema_to_validate: Dict[str, str], # Recebe o schema_COMPLETO
                             label: str) -> float:
        """
        Calcula um score de confiança baseado em regras de validação V16.1.
        
        Args:
            extracted_data: Os dados retornados pelo Módulo 2 (ParserExecutor).
            schema_to_validate: O schema completo (merged_schema) a ser validado.
            label: O label do item (ex: 'carteira_oab') para carregar as regras.
                              
        Returns:
            Um score de 0.0 a 1.0.
        """
        
        if not schema_to_validate:
            logging.warning("Schema de validação está vazio. Retornando confiança 0.0")
            return 0.0

        total_fields_in_schema = len(schema_to_validate)
        validated_fields = 0
        
        logging.info("Iniciando Módulo 3.1: Cálculo de Confiança Robusto...")
        
        # Pega as regras de validação para este label
        validation_rules = self._get_validation_rules(label)
        
        # Preenche o cache de dados cruzados para as regras usarem
        self._cross_data_cache['nome'] = extracted_data.get('nome')
        self._cross_data_cache['situacao'] = extracted_data.get('situacao')

        for field_name in schema_to_validate.keys():
            value = extracted_data.get(field_name)

            # 1. O campo está vazio?
            if not value:
                logging.warning(f"Confiança: Campo '{field_name}' está ausente ou vazio.")
                continue # Vai para o próximo loop, não conta como validado

            # 2. Existe uma regra de validação específica para ele?
            validator = validation_rules.get(field_name)
            
            if validator:
                # 3. Se sim, execute a regra
                try:
                    if validator(value):
                        validated_fields += 1 # SUCESSO!
                    else:
                        # REGRA FALHOU! (Ex: "inscricao" = "Seccional")
                        logging.warning(f"Confiança: Campo '{field_name}' falhou na validação. Valor: '{value}'")
                except Exception as e:
                    logging.error(f"Confiança: Erro ao executar validador para {field_name}: {e}")
            else:
                # 4. Se não há regra, contamos como válido (confiança ingênua)
                #    (Ex: para 'endereco_profissional' que não tem regra)
                validated_fields += 1

        # Limpa o cache
        self._cross_data_cache = {}
        
        # Calcula o score final
        confidence_score = validated_fields / total_fields_in_schema
        
        logging.info(f"Módulo 3.1: {validated_fields} de {total_fields_in_schema} campos VALIDADOS.")
        logging.info(f"Módulo 3.1: Score de Confiança Final = {confidence_score:.2f}")
        
        return confidence_score