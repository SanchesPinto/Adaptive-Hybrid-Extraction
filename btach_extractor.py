#!/usr/bin/env python3
"""
CLI para processamento em lote de PDFs - V18
Uso: python batch_extractor.py --input data.json --output results.json
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
import time

# Importar o extrator V18
from extractor_v18 import PDFDataExtractor


class BatchProcessor:
    """Processador em lote para múltiplos PDFs"""
    
    def __init__(self, openai_api_key: str):
        self.extractor = PDFDataExtractor(openai_api_key=openai_api_key)
        self.results = []
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'total_time': 0,
            'total_cost': 0,
            'cache_hits': 0
        }
    
    def process_batch(self, batch_data: List[Dict[str, Any]], base_path: str = ".") -> List[Dict[str, Any]]:
        """
        Processa múltiplos documentos em série
        
        Args:
            batch_data: Lista de dicts com {pdf_path, label, schema}
            base_path: Caminho base para os PDFs
        
        Returns:
            Lista de resultados
        """
        print(f"\n{'='*80}")
        print(f"PROCESSAMENTO EM LOTE - {len(batch_data)} documentos")
        print(f"{'='*80}\n")
        
        self.results = []
        self.stats['total'] = len(batch_data)
        
        for idx, item in enumerate(batch_data, 1):
            print(f"\n[{idx}/{len(batch_data)}] Processando item...")
            
            try:
                # Extrair parâmetros
                pdf_path = os.path.join(base_path, item['pdf_path'])
                label = item['label']
                schema = item['schema']
                
                # Validar arquivo
                if not os.path.exists(pdf_path):
                    raise FileNotFoundError(f"PDF não encontrado: {pdf_path}")
                
                # Processar (em série, conforme requisito)
                result = self.extractor.extract(pdf_path, label, schema)
                
                # Adicionar identificador
                result['item_id'] = idx
                result['pdf_path'] = item['pdf_path']
                result['label'] = label
                
                # Atualizar estatísticas
                if result['success']:
                    self.stats['success'] += 1
                    self.stats['total_time'] += result['metadata']['time_seconds']
                    if result['metadata']['cache_hit']:
                        self.stats['cache_hits'] += 1
                    
                    # Estimar custo (baseado em tokens aproximados)
                    if not result['metadata']['cache_hit']:
                        estimated_cost = 0.00015  # Custo médio estimado
                        self.stats['total_cost'] += estimated_cost
                        result['metadata']['estimated_cost'] = estimated_cost
                else:
                    self.stats['failed'] += 1
                
                self.results.append(result)
                
                # Feedback visual
                status = "✓ SUCESSO" if result['success'] else "✗ FALHA"
                time_str = f"{result['metadata']['time_seconds']:.2f}s" if result['success'] else "N/A"
                print(f"[{idx}/{len(batch_data)}] {status} - Tempo: {time_str}")
                
            except Exception as e:
                print(f"[{idx}/{len(batch_data)}] ✗ ERRO: {str(e)}")
                self.stats['failed'] += 1
                self.results.append({
                    'item_id': idx,
                    'pdf_path': item.get('pdf_path', 'unknown'),
                    'label': item.get('label', 'unknown'),
                    'success': False,
                    'error': str(e),
                    'data': {}
                })
        
        self._print_summary()
        return self.results
    
    def _print_summary(self):
        """Imprime resumo estatístico"""
        print(f"\n{'='*80}")
        print("RESUMO DO PROCESSAMENTO")
        print(f"{'='*80}")
        print(f"Total de documentos:    {self.stats['total']}")
        print(f"Sucessos:               {self.stats['success']} ({self.stats['success']/self.stats['total']*100:.1f}%)")
        print(f"Falhas:                 {self.stats['failed']}")
        print(f"Cache hits:             {self.stats['cache_hits']}")
        print(f"Tempo total:            {self.stats['total_time']:.2f}s")
        
        if self.stats['success'] > 0:
            avg_time = self.stats['total_time'] / self.stats['success']
            print(f"Tempo médio:            {avg_time:.2f}s")
        
        print(f"Custo total estimado:   ${self.stats['total_cost']:.4f}")
        
        if self.stats['success'] > 0:
            avg_cost = self.stats['total_cost'] / self.stats['success']
            print(f"Custo médio:            ${avg_cost:.6f}")
        
        print(f"{'='*80}\n")
    
    def save_results(self, output_path: str):
        """Salva resultados em arquivo JSON"""
        output_data = {
            'results': self.results,
            'statistics': self.stats,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Resultados salvos em: {output_path}")


def load_batch_file(file_path: str) -> List[Dict[str, Any]]:
    """Carrega arquivo de batch em JSON"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Suportar dois formatos:
    # 1. Lista direta: [{pdf_path, label, schema}, ...]
    # 2. Objeto com chave 'items': {items: [{...}]}
    
    if isinstance(data, list):
        return data
    elif isinstance(data, dict) and 'items' in data:
        return data['items']
    else:
        raise ValueError("Formato de arquivo inválido. Use lista ou {items: [...]}")


def main():
    parser = argparse.ArgumentParser(
        description='PDF Data Extractor V18 - Processamento em Lote',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:

  # Processar batch de arquivo JSON
  python batch_extractor.py --input batch.json --output results.json

  # Especificar pasta base dos PDFs
  python batch_extractor.py --input batch.json --output results.json --base-path ./pdfs

  # Usar API key customizada
  export OPENAI_API_KEY=sk-...
  python batch_extractor.py --input batch.json --output results.json

Formato do arquivo de entrada (batch.json):
[
  {
    "pdf_path": "carteira_oab/exemplo1.pdf",
    "label": "carteira_oab",
    "schema": {
      "nome": "Nome do profissional",
      "inscricao": "Número de inscrição"
    }
  },
  {
    "pdf_path": "nota_fiscal/exemplo2.pdf",
    "label": "nota_fiscal",
    "schema": {
      "numero": "Número da nota",
      "valor": "Valor total"
    }
  }
]
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Arquivo JSON com lista de documentos a processar'
    )
    
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Arquivo JSON para salvar resultados'
    )
    
    parser.add_argument(
        '--base-path', '-b',
        default='.',
        help='Caminho base para os arquivos PDF (padrão: diretório atual)'
    )
    
    parser.add_argument(
        '--api-key',
        default=None,
        help='OpenAI API Key (ou usar variável OPENAI_API_KEY)'
    )
    
    args = parser.parse_args()
    
    # Validar API key
    api_key = args.api_key or os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("ERRO: API Key não fornecida!")
        print("Use --api-key ou defina a variável OPENAI_API_KEY")
        sys.exit(1)
    
    # Validar arquivo de entrada
    if not os.path.exists(args.input):
        print(f"ERRO: Arquivo de entrada não encontrado: {args.input}")
        sys.exit(1)
    
    try:
        # Carregar batch
        print(f"Carregando batch de: {args.input}")
        batch_data = load_batch_file(args.input)
        print(f"✓ {len(batch_data)} itens carregados")
        
        # Processar
        processor = BatchProcessor(openai_api_key=api_key)
        results = processor.process_batch(batch_data, base_path=args.base_path)
        
        # Salvar resultados
        processor.save_results(args.output)
        
        # Exit code baseado em sucesso
        if processor.stats['failed'] > 0:
            sys.exit(1)  # Falhas encontradas
        else:
            sys.exit(0)  # Sucesso total
        
    except Exception as e:
        print(f"\nERRO FATAL: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()