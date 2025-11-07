# test_v19.py
import spacy
import json
from heuristic_extractor import HeuristicExtractorV19

# 1. Carregue o modelo NLP (simulando o Manager)
try:
    nlp = spacy.load("pt_core_news_sm")
except IOError:
    print("Execute: python -m spacy download pt_core_news_sm")
    exit()

# 2. Use os dados do EXEMPLO 1 do desafio [cite: 110-129]
label_teste = "carteira_oab"
schema_teste = {
    "nome": "Nome do profissional",
    "inscricao": "Número de inscrição do profissional",
    "seccional": "Seccional do profissional",
    "subsecao": "Subseção à qual o profissional faz parte",
    "categoria": "Categoria, pode ser ADVOGADO...",
    "telefone_profissional": "Telefone do profissional",
    "situacao": "Situação do profissional"
}

# Texto extraído do EXEMPLO 1 [cite: 112-116]
pdf_text_teste = """
SON GOKU
Inscrição Seccional Subseção
101943 PR CONSELHO SECCIONAL-PARANÁ
SUPLEMENTAR
Endereco Profissional
Telefone Profissional
SITUAÇÃO REGULAR
"""

# 3. Compile e execute o Extrator V19.0
print("--- TESTE DE UNIDADE V19.0 ---")
print(f"Compilando parser para '{label_teste}'...")
extractor = HeuristicExtractorV19(schema_teste, nlp)

print("Executando extração local V19.0...")
heuristic_results = extractor.extract(pdf_text_teste)

# 4. Inspecione a saída BRUTA da V19.0
print("--- Resultado (Apenas V19.0) ---")
print(json.dumps(heuristic_results, indent=2, ensure_ascii=False))

# 5. Teste de Cache (bônus)
print("\n--- TESTE DE CACHE (Manager simulado) ---")
print("Segunda execução (deve ser instantânea, sem compilar)...")
# Em um cenário real, o Manager faria isso:
# extractor_cached = MANAGER.get_or_create_extractor(label_teste, schema_teste)
# results_cached = extractor_cached.extract(pdf_text_teste)
# Apenas verifique se os logs "Compilando..." não aparecem no seu main.py