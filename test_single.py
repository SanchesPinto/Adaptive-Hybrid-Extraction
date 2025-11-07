# test_single.py
from extractor_v18 import PDFDataExtractor
import os

# Inicializar
extractor = PDFDataExtractor(
    openai_api_key=os.getenv("OPENAI_API_KEY")
)

# Testar com PDF de exemplo
result = extractor.extract(
    pdf_path="examples/pdfs/carteira_oab_001.pdf",
    label="carteira_oab",
    extraction_schema={
        "nome": "Nome do profissional",
        "inscricao": "Número de inscrição",
        "seccional": "Seccional",
        "categoria": "Categoria",
        "situacao": "Situação do profissional"
    }
)

# Verificar resultado
print("✅ Sucesso!" if result['success'] else "❌ Falha!")
print(f"Tempo: {result['metadata']['time_seconds']:.2f}s")
print(f"Acurácia: {result['metadata']['confidence']:.1%}")
print(f"Dados extraídos:")
import json
print(json.dumps(result['data'], indent=2, ensure_ascii=False))