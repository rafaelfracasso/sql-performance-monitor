#!/usr/bin/env python3
"""
Teste do LLM Analyzer com Gemini API.
"""
import os
from dotenv import load_dotenv

load_dotenv()

def test_llm_analyzer():
    """Testa análise de query com LLM."""
    print("=" * 80)
    print("TESTE: LLM Analyzer com Gemini")
    print("=" * 80)

    from sql_monitor.utils.llm_analyzer import LLMAnalyzer

    # Verificar API key
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key or api_key == 'your_gemini_api_key_here':
        print("\n⚠️  API Key Gemini não configurada no .env")
        print("   Configure GEMINI_API_KEY para executar este teste")
        return False

    print(f"\n✓ API Key encontrada: {api_key[:10]}...")

    # Criar analyzer
    config = {
        'api_key': api_key,
        'model': 'gemini-2.0-flash-exp',
        'max_tokens': 2000
    }

    analyzer = LLMAnalyzer(config)
    print("✓ LLM Analyzer criado")

    # Query de teste (problemática)
    test_query = """
        SELECT o.OrderID, c.CustomerName, SUM(od.Quantity * od.UnitPrice) as TotalAmount
        FROM Orders o
        JOIN Customers c ON o.CustomerID = c.CustomerID
        JOIN OrderDetails od ON o.OrderID = od.OrderID
        WHERE o.OrderDate >= @p1_DATE
        GROUP BY o.OrderID, c.CustomerName
        ORDER BY TotalAmount DESC
    """

    # DDL da tabela
    table_ddl = """CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATETIME,
    Status VARCHAR(50)
);"""

    # Índices existentes formatados
    existing_indexes = """
Índices Existentes:
1. PK_Orders (PRIMARY KEY)
   - Columns: OrderID
   - Unique: Yes

2. IX_Orders_CustomerID
   - Columns: CustomerID
   - Unique: No
"""

    # Métricas
    metrics = {
        'duration_seconds': 45,
        'cpu_time_ms': 35000,
        'logical_reads': 500000,
        'physical_reads': 25000,
        'execution_count': 1500,
        'has_table_scan': True
    }

    # Placeholder map
    placeholder_map = """@p1_DATE: type=DATE, example='2024-01-01'"""

    print("\n" + "=" * 80)
    print("Analisando query problemática...")
    print("=" * 80)
    print(f"\nQuery: {test_query[:100]}...")
    print(f"Duração: {metrics['duration_seconds']}s")
    print(f"CPU: {metrics['cpu_time_ms']}ms")
    print(f"Logical Reads: {metrics['logical_reads']:,}")
    print(f"Execuções: {metrics['execution_count']}")

    try:
        print("\n🤖 Enviando para análise do Gemini...")

        analysis = analyzer.analyze_query_performance(
            sanitized_query=test_query,
            placeholder_map=placeholder_map,
            table_ddl=table_ddl,
            existing_indexes=existing_indexes,
            metrics=metrics,
            query_plan=None
        )

        if analysis:
            print("\n✓ Análise recebida com sucesso!")
            print("\n" + "=" * 80)
            print("RESULTADO DA ANÁLISE")
            print("=" * 80)

            # Mostrar análise
            if isinstance(analysis, dict):
                if 'summary' in analysis:
                    print(f"\n📊 Resumo:\n{analysis['summary'][:300]}...")
                if 'issues' in analysis:
                    print(f"\n⚠️  Problemas encontrados: {len(analysis['issues'])}")
                    for i, issue in enumerate(analysis['issues'][:3], 1):
                        print(f"   {i}. {issue[:100]}...")
                if 'recommendations' in analysis:
                    print(f"\n💡 Recomendações: {len(analysis['recommendations'])}")
                    for i, rec in enumerate(analysis['recommendations'][:3], 1):
                        print(f"   {i}. {rec[:100]}...")
            else:
                print(f"\n{analysis[:500]}...")

            print("\n✅ LLM Analyzer funcionando corretamente!")
            return True

        else:
            print("\n✗ Análise retornou vazia")
            return False

    except Exception as e:
        print(f"\n✗ Erro na análise: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_llm_batch_analysis():
    """Testa análise em lote."""
    print("\n" + "=" * 80)
    print("TESTE: Análise em Lote")
    print("=" * 80)

    from sql_monitor.utils.llm_analyzer import LLMAnalyzer

    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key or api_key == 'your_gemini_api_key_here':
        print("\n⚠️  Pulando teste (API key não configurada)")
        return True

    config = {
        'api_key': api_key,
        'model': 'gemini-2.0-flash-exp',
        'max_tokens': 1000
    }

    analyzer = LLMAnalyzer(config)

    # 3 queries de teste
    queries = [
        {
            'query': 'SELECT * FROM Users WHERE Status = @p1_INT',
            'metrics': {'duration_seconds': 5, 'cpu_time_ms': 3000, 'logical_reads': 5000}
        },
        {
            'query': 'SELECT COUNT(*) FROM Orders WHERE OrderDate > @p1_DATE',
            'metrics': {'duration_seconds': 10, 'cpu_time_ms': 8000, 'logical_reads': 15000}
        },
        {
            'query': 'SELECT p.*, c.* FROM Products p JOIN Categories c ON p.CategoryID = c.ID',
            'metrics': {'duration_seconds': 15, 'cpu_time_ms': 12000, 'logical_reads': 25000}
        }
    ]

    print(f"\n📊 Analisando {len(queries)} queries...")

    success_count = 0
    for i, query_data in enumerate(queries, 1):
        print(f"\n{i}. {query_data['query'][:60]}...")

        try:
            analysis = analyzer.analyze_query_performance(
                sanitized_query=query_data['query'],
                placeholder_map="@p1: type=INT",
                table_ddl="CREATE TABLE Test (id INT)",
                existing_indexes="No indexes",
                metrics=query_data['metrics'],
                query_plan=None
            )

            if analysis:
                print(f"   ✓ Análise OK")
                success_count += 1
            else:
                print(f"   ⚠️  Análise vazia")

        except Exception as e:
            print(f"   ✗ Erro: {e}")

    print(f"\n✓ Análises concluídas: {success_count}/{len(queries)}")
    return success_count >= 2  # Pelo menos 2 de 3


if __name__ == '__main__':
    print("\n🚀 Iniciando testes do LLM Analyzer...\n")

    results = []

    # Teste 1: Análise única
    results.append(("Análise LLM Única", test_llm_analyzer()))

    # Teste 2: Análise em lote
    results.append(("Análise LLM em Lote", test_llm_batch_analysis()))

    # Resumo
    print("\n" + "=" * 80)
    print("RESUMO DOS TESTES LLM ANALYZER")
    print("=" * 80)

    for name, success in results:
        status = "✓ PASSOU" if success else "✗ FALHOU"
        print(f"{status}: {name}")

    total = len(results)
    passed = sum(1 for _, success in results if success)
    print(f"\nTotal: {passed}/{total} testes passaram")

    if passed == total:
        print("\n✅ TODOS OS TESTES PASSARAM!")
    else:
        print(f"\n⚠️  {total - passed} teste(s) falharam")
