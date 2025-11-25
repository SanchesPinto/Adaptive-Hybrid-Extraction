# Desafio de Extração de PDF - Fellowship AI (V22.1)

Esta solução implementa uma arquitetura híbrida, de refinamento iterativo, projetada para balancear as quatro restrições conflitantes do desafio: **Tempo** (< 10s), **Acurácia** (> 80%), **Custo** (mínimo) e **Adaptabilidade** (acúmulo de conhecimento).

## 1. O Desafio Mapeado: O "Beco Sem Saída"

Testes iniciais (logs V16-V20) provaram que nenhuma abordagem simples atende a todas as restrições:

1.  **O Problema do Tempo:** Uma chamada síncrona direta ao `gpt-5-mini` é a única abordagem 100% precisa, mas nossos logs provaram que ela é muito volátil e estoura o limite de 10s (ex: 12.99s, 10.98s nos logs de teste).
2.  **O Problema da Acurácia:** Uma heurística local (ex: `HeuristicExtractor` V18.3) é extremamente rápida (< 0.1s), mas sua acurácia é muito baixa, pois falha em layouts posicionais ou retorna dados incorretos ("envenenados").
3.  **O Problema do "Conhecimento":** O `gpt-5-mini` é um extrator de dados excelente, mas um *péssimo* gerador de Regex e Regras. Nossos logs de `09:37:22` provaram que o `ParserGenerator` (baseado em LLM) gerou Regex inválidas (ex: `Padrão não possui grupo de captura ()`) e que regras de validação fracas levaram a `Confiança Alta (1.00)` para dados 57% errados.

## 2. A Solução: Arquitetura V22.1 (Refinamento Iterativo)

Para resolver esse "beco sem saída", a solução implementa uma arquitetura de "acúmulo de conhecimento" que usa o `gpt-5-mini` síncrono (para acurácia) apenas quando necessário, protegido por um "watchdog" de tempo cumulativo.

A lógica de orquestração (`main.py`) gerencia quatro caminhos distintos:

### Caminho 1: Cache Miss (Primeira vez que vê um `label`)

Quando um `label` é visto pela primeira vez, o foco é o **Tempo** e o **Acúmulo de Conhecimento**.

1.  **Tarefa Síncrona (Heurística):** O `HeuristicExtractor` (V18.3) é executado. Ele é rápido (< 0.1s), mas "ingênuo" (retorna muitos `nulls`).
2.  **Validação Síncrona:** O `main.py` valida a taxa de falha (ex: `failure_rate < HEURISTIC_FAILURE_THRESHOLD`).
3.  **Decisão Síncrona (Watchdog):**
    * **Se a heurística for boa:** Retorna os dados (Tempo < 0.1s).
    * **Se a heurística for ruim:** (ex: falha de 86%) O sistema aciona o `FallbackExtractor.extract_all` (LLM de alta acurácia) em uma thread (`_run_llm_in_thread`), protegido por um **watchdog de tempo cumulativo**.
4.  **Tarefa Assíncrona (Acúmulo de Conhecimento):**
    * Paralelamente, uma thread (`_run_parser_generation_task`) é disparada para construir o "conhecimento" V1.
    * **Chamada 1 (LLM):** `FallbackExtractor.extract_all` obtém o "gabarito" (dados perfeitos).
    * **Chamada 2 (LLM):** `ParserGenerator` V22.0 usa o gabarito para gerar Regex (corrigindo o bug do grupo de captura).
    * **Chamada 3 (Local):** `ValidationGenerator` V22.1 faz engenharia reversa do *gabarito* para gerar regras de validação fortes (corrigindo o bug de `TypeError` e o bug de `enum`).
    * O pacote (`parser`, `validation_rules`) é salvo no `ParserRepository`.

### Caminho 2: Cache Hit (Confiança Alta)

Quando um `label` já conhecido é visto:

1.  O `ParserExecutor` (local, < 0.1s) executa as Regex geradas.
2.  O `ConfidenceCalculator` (local) usa as Regras V22.1 (fortes) para validar os dados.
3.  Se `confiança > 0.8`, os dados são retornados.
4.  **Resultado:** Custo $0, Tempo < 0.1s. (A acurácia depende do parser gerado pelo LLM).

### Caminho 3: Cache Hit (Confiança Baixa) - O "Refinamento"

Este é o núcleo da "criatividade" do projeto. O sistema aprende com seus próprios erros.

1.  O `ParserExecutor` falha (as Regex do LLM eram ruins).
2.  O `ConfidenceCalculator` (com as regras V22.1 fortes) detecta a falha (ex: `Score: 0.62`).
3.  **Tarefa Síncrona (Watchdog):** O `FallbackExtractor.extract_missing` é chamado para *corrigir* os campos faltantes, protegido pelo **watchdog de tempo cumulativo**.
4.  **Tarefa Assíncrona (Refinamento):** O sistema dispara uma *nova* thread (`_run_parser_REFINEMENT_task`) que usa esses dados corrigidos como um *novo gabarito* para gerar um pacote V2 (melhorado) de `parser` e `validation_rules`, substituindo o conhecimento antigo.

### O "Watchdog" de Tempo Cumulativo

A falha do Item 4 (12.99s) provou que o LLM pode estourar 10s. Nossa solução (`main.py`) usa um "orçamento de tempo" de lote.
* **Limite do Lote:** (Nº de Itens * 10s).
* **Orçamento do Item:** (Limite do Lote - Tempo Acumulado Gasto).
* **Resultado:** Itens rápidos (0.1s) "economizam" tempo, permitindo que os itens que precisam do LLM usem (ex:) 13.09s (como o Item 6), mantendo a média total do lote (40.41s / 6 = 6.73s) bem abaixo do limite.

## 3. Como Utilizar

1.  Clone o repositório.
2.  Instale as dependências: `pip install -r requirements.txt`
3.  Crie um arquivo `.env` na raiz e adicione sua API key:
    ```
    OPENAI_API_KEY=sk-....
    ```
4.  (Opcional) Limpe o cache de conhecimento: `rm -rf parser_repository_cache/`
5.  Adicione os PDFs de teste a uma pasta `/files` (o `main.py` a utiliza).
6.  Configure o `dataset.json` para apontar para os arquivos PDF.
7.  Execute o processamento em lote:
    ```bash
    python3 main.py
    ```

## 4. Interface do Usuário (Diferencial)

Como um diferencial, uma interface de usuário funcional foi prototipada utilizando a plataforma low-code lovable.dev.

**Nota Importante:** Devido à complexidade da arquitetura de processamento em lote (V22.1) e ao curto prazo de entrega, **a UI ainda não foi integrada ao backend principal.**

O código exportado do protótipo da UI está incluído na pasta `/ui-prototype` para demonstrar a intenção e a funcionalidade visual.
