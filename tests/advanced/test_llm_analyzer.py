#!/usr/bin/env python3
"""
Teste do LLM Analyzer com Groq API.
"""
import os
from dotenv import load_dotenv

load_dotenv()

def test_llm_analyzer():
    """Testa análise de query com LLM."""
    print("=" * 80)
    print("TESTE: LLM Analyzer com Groq")
    print("=" * 80)

    from sql_monitor.utils.llm_analyzer import LLMAnalyzer

    # Verificar API key
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key or api_key == 'your_groq_api_key_here':
        print("\n  API Key Groq nao configurada no .env")
        print("   Configure GROQ_API_KEY para executar este teste")
        return False

    print(f"\n✓ API Key encontrada: {api_key[:10]}...")

    # Criar analyzer
    config = {
        'api_key': api_key,
        'model': 'llama-3.3-70b-versatile',
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
Indices Existentes:
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
    print(f"Duracao: {metrics['duration_seconds']}s")
    print(f"CPU: {metrics['cpu_time_ms']}ms")
    print(f"Logical Reads: {metrics['logical_reads']:,}")
    print(f"Execucoes: {metrics['execution_count']}")

    try:
        print("\nEnviando para analise do Groq...")

        analysis = analyzer.analyze_query_performance(
            sanitized_query=test_query,
            placeholder_map=placeholder_map,
            table_ddl=table_ddl,
            existing_indexes=existing_indexes,
            metrics=metrics,
            query_plan=None
        )

        if analysis:
            print("\n✓ Analise recebida com sucesso!")
            print("\n" + "=" * 80)
            print("RESULTADO DA ANALISE")
            print("=" * 80)

            if isinstance(analysis, dict):
                if 'summary' in analysis:
                    print(f"\nResumo:\n{analysis['summary'][:300]}...")
                if 'issues' in analysis:
                    print(f"\nProblemas encontrados: {len(analysis['issues'])}")
                    for i, issue in enumerate(analysis['issues'][:3], 1):
                        print(f"   {i}. {issue[:100]}...")
                if 'recommendations' in analysis:
                    print(f"\nRecomendacoes: {len(analysis['recommendations'])}")
                    for i, rec in enumerate(analysis['recommendations'][:3], 1):
                        print(f"   {i}. {rec[:100]}...")
            else:
                print(f"\n{analysis[:500]}...")

            print("\n✓ LLM Analyzer funcionando corretamente!")
            return True

        else:
            print("\n✗ Analise retornou vazia")
            return False

    except Exception as e:
        print(f"\n✗ Erro na analise: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_llm_batch_analysis():
    """Testa análise em lote."""
    print("\n" + "=" * 80)
    print("TESTE: Analise em Lote")
    print("=" * 80)

    from sql_monitor.utils.llm_analyzer import LLMAnalyzer

    api_key = os.getenv('GROQ_API_KEY')
    if not api_key or api_key == 'your_groq_api_key_here':
        print("\n  Pulando teste (API key nao configurada)")
        return True

    config = {
        'api_key': api_key,
        'model': 'llama-3.3-70b-versatile',
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
            'metrics': {'duration_seconds': 3, 'cpu_time_ms': 2000, 'logical_reads': 3000}
        }
    ]

    print(f"\nAnalisando {len(queries)} queries...")

    success_count = 0
    for i, q in enumerate(queries, 1):
        try:
            result = analyzer.analyze_query_performance(
                sanitized_query=q['query'],
                placeholder_map='',
                table_ddl='',
                existing_indexes='',
                metrics=q['metrics'],
                query_plan=None
            )
            if result:
                print(f"   {i}. OK - {q['query'][:60]}...")
                success_count += 1
        except Exception as e:
            print(f"   {i}. ERRO: {e}")

    print(f"\nResultado: {success_count}/{len(queries)} analises bem-sucedidas")
    return success_count > 0


if __name__ == "__main__":
    test_llm_analyzer()
    test_llm_batch_analysis()
