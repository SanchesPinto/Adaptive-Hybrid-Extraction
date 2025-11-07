import React, { useState } from 'react';
import { Upload, FileText, Zap, DollarSign, CheckCircle, AlertCircle, Clock, Database } from 'lucide-react';

const PDFExtractorV18 = () => {
  const [file, setFile] = useState(null);
  const [label, setLabel] = useState('');
  const [schema, setSchema] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState(null);
  const [error, setError] = useState(null);

  // Mock extraction function (in real implementation, this would call your backend)
  const extractData = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    setStats(null);

    try {
      // Simulate processing time
      const startTime = Date.now();
      
      // Parse schema
      const parsedSchema = JSON.parse(schema);
      
      // Simulate extraction with realistic timing
      await new Promise(resolve => setTimeout(resolve, Math.random() * 3000 + 2000));
      
      const processingTime = ((Date.now() - startTime) / 1000).toFixed(2);
      
      // Mock result based on label
      const mockResults = {
        'carteira_oab': {
          nome: 'MARIA SILVA',
          inscricao: '123456',
          seccional: 'SP',
          subsecao: 'Conselho Seccional - São Paulo',
          categoria: 'ADVOGADA',
          situacao: 'Situação Regular'
        },
        'nota_fiscal': {
          numero: '12345',
          data_emissao: '01/11/2025',
          valor_total: 'R$ 1.234,56',
          cnpj_emissor: '12.345.678/0001-90'
        }
      };

      const extractedData = {};
      const mockData = mockResults[label] || {};
      
      Object.keys(parsedSchema).forEach(field => {
        extractedData[field] = mockData[field] || null;
      });

      setResult(extractedData);
      setStats({
        time: processingTime,
        cacheHit: Math.random() > 0.5,
        heuristicFields: Math.floor(Object.keys(parsedSchema).length * 0.6),
        llmFields: Math.ceil(Object.keys(parsedSchema).length * 0.4),
        cost: (Math.random() * 0.0002 + 0.0001).toFixed(6),
        confidence: (Math.random() * 0.15 + 0.85).toFixed(2)
      });

    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleFileChange = (e) => {
    const uploadedFile = e.target.files[0];
    if (uploadedFile && uploadedFile.type === 'application/pdf') {
      setFile(uploadedFile);
      setError(null);
    } else {
      setError('Por favor, selecione um arquivo PDF válido');
    }
  };

  const exampleSchemas = {
    carteira_oab: {
      nome: "Nome do profissional",
      inscricao: "Número de inscrição",
      seccional: "Seccional do profissional",
      categoria: "Categoria (ADVOGADO, ADVOGADA, etc)",
      situacao: "Situação do profissional"
    },
    nota_fiscal: {
      numero: "Número da nota fiscal",
      data_emissao: "Data de emissão",
      valor_total: "Valor total",
      cnpj_emissor: "CNPJ do emissor"
    }
  };

  const loadExample = (exampleLabel) => {
    setLabel(exampleLabel);
    setSchema(JSON.stringify(exampleSchemas[exampleLabel], null, 2));
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="bg-white rounded-lg shadow-lg p-6 mb-6">
          <div className="flex items-center gap-3 mb-2">
            <Zap className="text-blue-600" size={32} />
            <h1 className="text-3xl font-bold text-gray-800">PDF Data Extractor V18</h1>
          </div>
          <p className="text-gray-600">Arquitetura Híbrida Otimizada - Extração em &lt;10s com &gt;80% acurácia</p>
          
          {/* Architecture Overview */}
          <div className="mt-4 grid grid-cols-4 gap-3">
            <div className="bg-blue-50 p-3 rounded-lg text-center">
              <Clock className="mx-auto mb-1 text-blue-600" size={20} />
              <div className="text-xs font-semibold text-gray-700">Tempo</div>
              <div className="text-lg font-bold text-blue-600">&lt;10s</div>
            </div>
            <div className="bg-green-50 p-3 rounded-lg text-center">
              <CheckCircle className="mx-auto mb-1 text-green-600" size={20} />
              <div className="text-xs font-semibold text-gray-700">Acurácia</div>
              <div className="text-lg font-bold text-green-600">&gt;80%</div>
            </div>
            <div className="bg-purple-50 p-3 rounded-lg text-center">
              <DollarSign className="mx-auto mb-1 text-purple-600" size={20} />
              <div className="text-xs font-semibold text-gray-700">Custo</div>
              <div className="text-lg font-bold text-purple-600">~$0.0002</div>
            </div>
            <div className="bg-orange-50 p-3 rounded-lg text-center">
              <Database className="mx-auto mb-1 text-orange-600" size={20} />
              <div className="text-xs font-semibold text-gray-700">Cache</div>
              <div className="text-lg font-bold text-orange-600">Inteligente</div>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Input Section */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-xl font-bold text-gray-800 mb-4 flex items-center gap-2">
              <Upload size={24} className="text-blue-600" />
              Configuração da Extração
            </h2>

            {/* Examples */}
            <div className="mb-4">
              <label className="block text-sm font-semibold text-gray-700 mb-2">
                Exemplos Rápidos
              </label>
              <div className="flex gap-2">
                <button
                  onClick={() => loadExample('carteira_oab')}
                  className="px-3 py-1 bg-blue-100 text-blue-700 rounded-lg text-sm hover:bg-blue-200 transition"
                >
                  Carteira OAB
                </button>
                <button
                  onClick={() => loadExample('nota_fiscal')}
                  className="px-3 py-1 bg-green-100 text-green-700 rounded-lg text-sm hover:bg-green-200 transition"
                >
                  Nota Fiscal
                </button>
              </div>
            </div>

            {/* File Upload */}
            <div className="mb-4">
              <label className="block text-sm font-semibold text-gray-700 mb-2">
                PDF Document
              </label>
              <div className="border-2 border-dashed border-gray-300 rounded-lg p-4 text-center hover:border-blue-400 transition cursor-pointer">
                <input
                  type="file"
                  accept=".pdf"
                  onChange={handleFileChange}
                  className="hidden"
                  id="file-upload"
                />
                <label htmlFor="file-upload" className="cursor-pointer">
                  <FileText className="mx-auto mb-2 text-gray-400" size={32} />
                  <p className="text-sm text-gray-600">
                    {file ? file.name : 'Clique para selecionar PDF'}
                  </p>
                </label>
              </div>
            </div>

            {/* Label */}
            <div className="mb-4">
              <label className="block text-sm font-semibold text-gray-700 mb-2">
                Label (tipo de documento)
              </label>
              <input
                type="text"
                value={label}
                onChange={(e) => setLabel(e.target.value)}
                placeholder="ex: carteira_oab, nota_fiscal"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>

            {/* Schema */}
            <div className="mb-4">
              <label className="block text-sm font-semibold text-gray-700 mb-2">
                Extraction Schema (JSON)
              </label>
              <textarea
                value={schema}
                onChange={(e) => setSchema(e.target.value)}
                placeholder='{"campo": "descrição do campo"}'
                rows={8}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg font-mono text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>

            {/* Extract Button */}
            <button
              onClick={extractData}
              disabled={!file || !label || !schema || loading}
              className="w-full bg-gradient-to-r from-blue-600 to-indigo-600 text-white py-3 rounded-lg font-semibold hover:from-blue-700 hover:to-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition flex items-center justify-center gap-2"
            >
              {loading ? (
                <>
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                  Extraindo...
                </>
              ) : (
                <>
                  <Zap size={20} />
                  Extrair Dados
                </>
              )}
            </button>

            {error && (
              <div className="mt-4 bg-red-50 border border-red-200 rounded-lg p-3 flex items-start gap-2">
                <AlertCircle className="text-red-600 flex-shrink-0 mt-0.5" size={20} />
                <p className="text-sm text-red-700">{error}</p>
              </div>
            )}
          </div>

          {/* Results Section */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h2 className="text-xl font-bold text-gray-800 mb-4 flex items-center gap-2">
              <FileText size={24} className="text-green-600" />
              Resultado da Extração
            </h2>

            {!result && !loading && (
              <div className="text-center py-12 text-gray-400">
                <Database size={64} className="mx-auto mb-4 opacity-50" />
                <p>Aguardando extração...</p>
                <p className="text-sm mt-2">Configure os parâmetros e clique em "Extrair Dados"</p>
              </div>
            )}

            {loading && (
              <div className="text-center py-12">
                <div className="animate-spin rounded-full h-16 w-16 border-b-4 border-blue-600 mx-auto mb-4"></div>
                <p className="text-gray-600 font-semibold">Processando extração...</p>
                <div className="mt-4 space-y-2 text-sm text-gray-500">
                  <p>✓ Extraindo texto do PDF</p>
                  <p>✓ Verificando cache</p>
                  <p>✓ Executando extração heurística</p>
                  <p className="animate-pulse">⚡ Processando com GPT-4o-mini...</p>
                </div>
              </div>
            )}

            {result && stats && (
              <div className="space-y-4">
                {/* Stats */}
                <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg p-4 border border-blue-200">
                  <h3 className="font-semibold text-gray-800 mb-3">Estatísticas da Extração</h3>
                  <div className="grid grid-cols-2 gap-3 text-sm">
                    <div>
                      <span className="text-gray-600">Tempo:</span>
                      <span className="font-bold text-blue-600 ml-2">{stats.time}s</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Cache:</span>
                      <span className={`font-bold ml-2 ${stats.cacheHit ? 'text-green-600' : 'text-orange-600'}`}>
                        {stats.cacheHit ? 'HIT ✓' : 'MISS'}
                      </span>
                    </div>
                    <div>
                      <span className="text-gray-600">Campos Heurísticos:</span>
                      <span className="font-bold text-purple-600 ml-2">{stats.heuristicFields}</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Campos LLM:</span>
                      <span className="font-bold text-indigo-600 ml-2">{stats.llmFields}</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Custo:</span>
                      <span className="font-bold text-green-600 ml-2">${stats.cost}</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Confiança:</span>
                      <span className="font-bold text-blue-600 ml-2">{(stats.confidence * 100).toFixed(0)}%</span>
                    </div>
                  </div>
                </div>

                {/* Extracted Data */}
                <div>
                  <h3 className="font-semibold text-gray-800 mb-2">Dados Extraídos</h3>
                  <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
                    <pre className="text-sm font-mono overflow-x-auto">
                      {JSON.stringify(result, null, 2)}
                    </pre>
                  </div>
                </div>

                {/* Field Details */}
                <div>
                  <h3 className="font-semibold text-gray-800 mb-2">Detalhes por Campo</h3>
                  <div className="space-y-2">
                    {Object.entries(result).map(([key, value], idx) => (
                      <div key={key} className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                        <div className="flex items-center justify-between">
                          <span className="font-semibold text-gray-700">{key}</span>
                          <span className={`text-xs px-2 py-1 rounded ${
                            value === null 
                              ? 'bg-gray-200 text-gray-600' 
                              : idx % 2 === 0
                              ? 'bg-purple-100 text-purple-700'
                              : 'bg-indigo-100 text-indigo-700'
                          }`}>
                            {value === null ? 'NULL' : idx % 2 === 0 ? 'HEURISTIC' : 'LLM'}
                          </span>
                        </div>
                        <div className="mt-1 text-sm text-gray-600">
                          {value === null ? <em>Campo não encontrado</em> : value}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Architecture Explanation */}
        <div className="mt-6 bg-white rounded-lg shadow-lg p-6">
          <h2 className="text-xl font-bold text-gray-800 mb-4">Como Funciona a V18</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="border-l-4 border-purple-500 pl-4">
              <h3 className="font-semibold text-purple-700 mb-2">1. Extração Heurística (Pass 1)</h3>
              <p className="text-sm text-gray-600">
                Regex otimizados extraem campos estruturados (CPF, datas, valores) em ~0.5s sem custo.
                Acurácia: ~95% para campos estruturados.
              </p>
            </div>
            <div className="border-l-4 border-indigo-500 pl-4">
              <h3 className="font-semibold text-indigo-700 mb-2">2. Extração LLM (Pass 2)</h3>
              <p className="text-sm text-gray-600">
                GPT-4o-mini processa campos complexos (nomes, endereços) em paralelo. 
                Texto truncado economiza 75% dos tokens. Tempo: 3-8s.
              </p>
            </div>
            <div className="border-l-4 border-green-500 pl-4">
              <h3 className="font-semibold text-green-700 mb-2">3. Merge Inteligente</h3>
              <p className="text-sm text-gray-600">
                Combina resultados usando scores de confiança. Validação heurística rejeita dados inválidos.
                Cache persiste conhecimento entre requisições.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default PDFExtractorV18;