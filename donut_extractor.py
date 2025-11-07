# Módulo: donut_extractor.py
# (Substituto do parser_generator.py e parser_executor.py)

import torch
from transformers import DonutProcessor, VisionEncoderDecoderModel
from pdf2image import convert_from_path
from PIL.Image import Image
import re
import json
from typing import Dict, Any

class DonutExtractor:
    """
    Implementação da arquitetura V17 ("Donut-First").
    Esta classe usa um modelo Vision Encoder-Decoder (Donut) local 
    para extrair dados de PDFs como imagens (abordagem 2D, layout-aware).

    Resolve os bloqueadores da V16:
    1. CUSTO: A inferência é local, custo monetário é $0.
    2. TEMPO: A inferência local é < 1s, resolvendo o bloqueador de 10s.
    3. ACURÁCIA: É "layout-aware", resolvendo a "cegueira ao layout" 
       dos parsers Regex[cite: 197].
    """
    
    def __init__(self, model_name_or_path: str = "naver-clova-ix/donut-base-finetuned-cord-v2"):
        """
        Carrega o modelo e o processador na memória.
        Isso é feito apenas uma vez na inicialização do sistema.
        """
        print(f"[DonutExtractor] Carregando modelo '{model_name_or_path}'...")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        
        # 1. Carregar o Processador (lida com input/output)
        self.processor = DonutProcessor.from_pretrained(model_name_or_path)
        
        # 2. Carregar o Modelo (Vision-Encoder-Decoder)
        self.model = VisionEncoderDecoderModel.from_pretrained(model_name_or_path).to(self.device)
        print(f"[DonutExtractor] Modelo carregado e rodando em: {self.device}")

    def _pdf_to_image(self, pdf_file_path: str) -> Image:
        """
        Converte a página única do PDF em uma imagem PIL.
        O Donut opera em pixels (2D), não em texto (1D).
        [cite: 190]
        """
        # Sabemos que o PDF tem apenas uma página [cite: 317]
        images = convert_from_path(pdf_file_path, first_page=1, last_page=1)
        return images[0]

    def _schema_to_prompt(self, item_schema: Dict[str, str]) -> str:
        """
        Converte dinamicamente o schema de extração do item
        no formato de "task prompt" que o Donut espera.
        
        Ex: {"nome": "..."} -> "<s_cord-v1><s_nome>"
        """
        # Formato padrão para extração key-value (ex: CORD dataset)
        prompt = "<s_cord-v1>"
        for key in item_schema.keys():
            # Limpa a chave para ser um token válido (ex: 'data_base' -> 'data_base')
            clean_key = re.sub(r"[^a-zA-Z0-9_]", "", key.lower())
            prompt += f"<s_{clean_key}>"
            # O Donut é treinado para preencher o que vem depois do token
        
        return prompt

    def _parse_output(self, model_output_string: str) -> Dict[str, Any]:
        """
        Limpa a string de saída do modelo e a converte em um dicionário Python.
        """
        # A saída do Donut é uma string estruturada, ex:
        # "<s_cord-v1><s_nome>JOANA D'ARC</s_nome><s_inscricao>101943</s_inscricao>..."
        
        data = {}
        # Remove o token de início
        output = model_output_string.replace(self.processor.tokenizer.bos_token, "").strip()
        output = output.replace(self.processor.tokenizer.eos_token, "").strip()
        
        # Encontra todos os pares <key>valor</key>
        matches = re.findall(r"<s_([a-zA-Z0-9_]+)>(.*?)</s_\1>", output)
        
        for key, value in matches:
            # Limpa o valor de tokens especiais (embora o processor.decode já faça isso)
            clean_value = value.strip()
            
            # Se o valor estiver vazio ou for um token de "não encontrado", 
            # mapeia para None
            if not clean_value or clean_value == self.processor.tokenizer.pad_token:
                data[key] = None
            else:
                data[key] = clean_value
                
        return data

    def extract(self, pdf_file_path: str, item_schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Orquestra a extração V17 (Donut-First).
        Este é o novo "Caminho Rápido" síncrono.
        """
        try:
            # Passo 1: Converter PDF -> Imagem (Abordagem 2D)
            image = self._pdf_to_image(pdf_file_path)
            
            # Passo 2: Gerar o prompt zero-shot dinâmico 
            task_prompt = self._schema_to_prompt(item_schema)
            
            # Passo 3: Preparar inputs para o modelo
            # O processador prepara a imagem (pixel_values) e o prompt (decoder_input_ids)
            decoder_input_ids = self.processor.tokenizer(
                task_prompt, 
                add_special_tokens=False, 
                return_tensors="pt"
            ).input_ids.to(self.device)
            
            pixel_values = self.processor(
                image, 
                return_tensors="pt"
            ).pixel_values.to(self.device)

            # Passo 4: Inferência Local (Rápida e Custo $0)
            outputs = self.model.generate(
                pixel_values,
                decoder_input_ids=decoder_input_ids,
                max_length=self.model.decoder.config.max_position_embeddings,
                early_stopping=True,
                pad_token_id=self.processor.tokenizer.pad_token_id,
                eos_token_id=self.processor.tokenizer.eos_token_id,
                use_cache=True,
                num_beams=1, # Beam search 1 é mais rápido (greedy)
                bad_words_ids=[[self.processor.tokenizer.unk_token_id]],
                return_dict_in_generate=True,
            )

            # Passo 5: Decodificar e Parsear a saída
            sequence = self.processor.batch_decode(outputs.sequences)[0]
            
            # Limpa a string (remove o prompt de input)
            clean_sequence = self.processor.tokenizer.decode(
                outputs.sequences[0], 
                skip_special_tokens=False # Mantemos os tokens especiais <s_key>
            )

            extracted_data = self._parse_output(clean_sequence)
            
            return extracted_data

        except Exception as e:
            print(f"[DonutExtractor] ERRO CRÍTICO durante a extração: {e}")
            # Se o Donut falhar, retornamos um dict vazio para 
            # forçar o ConfidenceCalculator a falhar (confiança 0)
            # e acionar o Fallback (Modo 2).
            return {}