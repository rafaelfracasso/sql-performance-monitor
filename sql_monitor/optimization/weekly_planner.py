"""
Weekly Optimization Planner - Agente que analisa dados semanais e gera plano de otimização.

Este agente:
1. Roda 1x por semana (configurável)
2. Analisa histórico de métricas no DuckDB
3. Identifica padrões e oportunidades de otimização
4. Gera plano de execução com scripts SQL
5. Agenda execução para madrugada (horário de baixo impacto)
6. Gera relatório de impacto estimado
"""
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from ..utils.metrics_store import MetricsStore
from ..core.database_types import DatabaseType


class WeeklyOptimizationPlanner:
    """
    Analisa dados históricos e gera plano semanal de otimizações.

    Features:
    - Análise de queries mais frequentes (> 10 ocorrências)
    - Identificação de missing indexes por padrão
    - Sugestão de reescrita de queries
    - Detecção de estatísticas desatualizadas
    - Plano de manutenção (REBUILD, REORG, VACUUM)
    - Estimativa de impacto e ROI
    """

    def __init__(
        self,
        metrics_store: MetricsStore,
        config: Dict[str, Any],
        output_dir: str = "optimization_plans"
    ):
        """
        Inicializa o planejador semanal.

        Args:
            metrics_store: Store de métricas DuckDB
            config: Configuração completa (config.json)
            output_dir: Diretório para salvar planos gerados
        """
        self.metrics_store = metrics_store
        self.config = config
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Thresholds para análise
        self.min_occurrences = config.get('weekly_optimizer', {}).get('min_occurrences', 10)
        self.min_avg_duration_ms = config.get('weekly_optimizer', {}).get('min_avg_duration_ms', 1000)
        self.analysis_days = config.get('weekly_optimizer', {}).get('analysis_days', 7)

    def generate_weekly_plan(self) -> Dict[str, Any]:
        """
        Gera plano completo de otimização semanal.

        Returns:
            Dicionário com o plano completo de otimizações por instância
        """
        print("\n" + "=" * 80)
        print("🔍 INICIANDO ANÁLISE SEMANAL DE OTIMIZAÇÃO")
        print("=" * 80)

        plan = {
            'generated_at': datetime.now().isoformat(),
            'analysis_period_days': self.analysis_days,
            'instances': {},
            'summary': {
                'total_optimizations': 0,
                'estimated_improvement_percent': 0,
                'execution_time_estimate_minutes': 0
            }
        }

        # Obter instâncias ativas
        instances = self._get_active_instances()
        print(f"\n📊 Analisando {len(instances)} instâncias ativas...")

        for instance in instances:
            instance_name = instance['name']
            db_type = instance['type']

            print(f"\n🔧 Analisando {instance_name} ({db_type})...")

            instance_plan = self._analyze_instance(instance_name, db_type)

            if instance_plan['optimizations']:
                plan['instances'][instance_name] = instance_plan
                plan['summary']['total_optimizations'] += len(instance_plan['optimizations'])

        # Calcular estatísticas do plano
        plan['summary'] = self._calculate_plan_summary(plan)

        # Salvar plano em JSON
        plan_file = self._save_plan(plan)
        print(f"\n✅ Plano salvo em: {plan_file}")

        return plan

    def _get_active_instances(self) -> List[Dict[str, str]]:
        """
        Retorna lista de instâncias ativas na última semana.

        Returns:
            Lista de dicionários com nome e tipo de instância
        """
        cutoff = datetime.now() - timedelta(days=self.analysis_days)
        conn = self.metrics_store._get_connection()

        results = conn.execute("""
            SELECT DISTINCT instance_name, db_type
            FROM monitoring_cycles
            WHERE cycle_started_at >= ?
            ORDER BY instance_name
        """, [cutoff]).fetchall()

        return [{'name': row[0], 'type': row[1]} for row in results]

    def _analyze_instance(self, instance_name: str, db_type: str) -> Dict[str, Any]:
        """
        Analisa uma instância e gera plano de otimização.

        Args:
            instance_name: Nome da instância
            db_type: Tipo do banco (sqlserver, postgresql, hana)

        Returns:
            Dicionário com plano de otimização da instância
        """
        plan = {
            'instance_name': instance_name,
            'db_type': db_type,
            'analyzed_at': datetime.now().isoformat(),
            'optimizations': []
        }

        # 1. Análise de queries frequentes e lentas
        frequent_slow_queries = self._find_frequent_slow_queries(instance_name)
        for query_info in frequent_slow_queries:
            opt = self._create_query_optimization(query_info, db_type)
            if opt:
                plan['optimizations'].append(opt)

        # 2. Análise de missing indexes
        missing_indexes = self._find_missing_indexes_patterns(instance_name, db_type)
        for index_info in missing_indexes:
            opt = self._create_index_optimization(index_info, db_type)
            if opt:
                plan['optimizations'].append(opt)

        # 3. Análise de tabelas com estatísticas desatualizadas
        stale_stats = self._find_stale_statistics(instance_name, db_type)
        for stats_info in stale_stats:
            opt = self._create_statistics_optimization(stats_info, db_type)
            if opt:
                plan['optimizations'].append(opt)

        # 4. Plano de manutenção
        maintenance = self._create_maintenance_plan(instance_name, db_type)
        if maintenance:
            plan['optimizations'].extend(maintenance)

        # Priorizar otimizações por impacto
        plan['optimizations'] = self._prioritize_optimizations(plan['optimizations'])

        return plan

    def _find_frequent_slow_queries(self, instance_name: str) -> List[Dict[str, Any]]:
        """
        Encontra queries frequentes e lentas (candidatas a otimização).

        Args:
            instance_name: Nome da instância

        Returns:
            Lista de queries candidatas
        """
        cutoff = datetime.now() - timedelta(days=self.analysis_days)
        conn = self.metrics_store._get_connection()

        results = conn.execute("""
            SELECT
                qc.query_hash,
                qc.database_name,
                qc.schema_name,
                qc.table_name,
                qc.sanitized_query,
                COUNT(*) as occurrences,
                AVG(qm.cpu_time_ms) as avg_cpu_ms,
                AVG(qm.duration_ms) as avg_duration_ms,
                AVG(qm.logical_reads) as avg_logical_reads,
                MAX(qm.cpu_time_ms) as max_cpu_ms,
                la.severity,
                la.recommendations,
                la.analysis_text
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash
            LEFT JOIN llm_analyses la ON qc.query_hash = la.query_hash
            WHERE qc.instance_name = ?
              AND qc.collected_at >= ?
            GROUP BY qc.query_hash, qc.database_name, qc.schema_name, qc.table_name,
                     qc.sanitized_query, la.severity, la.recommendations, la.analysis_text
            HAVING occurrences >= ?
               AND avg_duration_ms >= ?
            ORDER BY (occurrences * avg_duration_ms) DESC
            LIMIT 20
        """, [instance_name, cutoff, self.min_occurrences, self.min_avg_duration_ms]).fetchall()

        return [
            {
                'query_hash': row[0],
                'database': row[1],
                'schema': row[2],
                'table': row[3],
                'sanitized_query': row[4],
                'occurrences': row[5],
                'avg_cpu_ms': row[6],
                'avg_duration_ms': row[7],
                'avg_logical_reads': row[8],
                'max_cpu_ms': row[9],
                'severity': row[10],
                'recommendations': row[11],
                'analysis': row[12]
            }
            for row in results
        ]

    def _find_missing_indexes_patterns(
        self,
        instance_name: str,
        db_type: str
    ) -> List[Dict[str, Any]]:
        """
        Identifica padrões de missing indexes baseado em queries frequentes.

        Args:
            instance_name: Nome da instância
            db_type: Tipo do banco

        Returns:
            Lista de sugestões de índices
        """
        cutoff = datetime.now() - timedelta(days=self.analysis_days)
        conn = self.metrics_store._get_connection()

        # Queries com table scan ou muitas leituras
        results = conn.execute("""
            SELECT
                qc.database_name,
                qc.schema_name,
                qc.table_name,
                COUNT(DISTINCT qc.query_hash) as query_count,
                AVG(qm.logical_reads) as avg_reads,
                COUNT(*) as total_occurrences
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash
            WHERE qc.instance_name = ?
              AND qc.collected_at >= ?
              AND (qc.query_type = 'table_scan' OR qm.logical_reads > 50000)
            GROUP BY qc.database_name, qc.schema_name, qc.table_name
            HAVING query_count >= 3
            ORDER BY (query_count * avg_reads) DESC
            LIMIT 10
        """, [instance_name, cutoff]).fetchall()

        return [
            {
                'database': row[0],
                'schema': row[1],
                'table': row[2],
                'query_count': row[3],
                'avg_reads': row[4],
                'occurrences': row[5]
            }
            for row in results
        ]

    def _find_stale_statistics(
        self,
        instance_name: str,
        db_type: str
    ) -> List[Dict[str, Any]]:
        """
        Identifica tabelas com estatísticas potencialmente desatualizadas.

        Args:
            instance_name: Nome da instância
            db_type: Tipo do banco

        Returns:
            Lista de tabelas que precisam atualização de estatísticas
        """
        # Tabelas com muitas queries problemáticas podem ter stats desatualizadas
        cutoff = datetime.now() - timedelta(days=self.analysis_days)
        conn = self.metrics_store._get_connection()

        results = conn.execute("""
            SELECT
                qc.database_name,
                qc.schema_name,
                qc.table_name,
                COUNT(DISTINCT qc.query_hash) as problem_query_count,
                COUNT(*) as total_problems
            FROM queries_collected qc
            WHERE qc.instance_name = ?
              AND qc.collected_at >= ?
            GROUP BY qc.database_name, qc.schema_name, qc.table_name
            HAVING problem_query_count >= 5
            ORDER BY problem_query_count DESC
            LIMIT 15
        """, [instance_name, cutoff]).fetchall()

        return [
            {
                'database': row[0],
                'schema': row[1],
                'table': row[2],
                'problem_query_count': row[3],
                'total_problems': row[4]
            }
            for row in results
        ]

    def _create_maintenance_plan(
        self,
        instance_name: str,
        db_type: str
    ) -> List[Dict[str, Any]]:
        """
        Cria plano de manutenção periódica.

        Args:
            instance_name: Nome da instância
            db_type: Tipo do banco

        Returns:
            Lista de tarefas de manutenção
        """
        maintenance_tasks = []

        # Baseado no tipo de banco, criar tarefas específicas
        if db_type == 'sqlserver':
            maintenance_tasks.append({
                'type': 'maintenance',
                'action': 'rebuild_indexes',
                'priority': 'medium',
                'description': 'Rebuild de índices fragmentados',
                'estimated_duration_minutes': 30,
                'estimated_improvement_percent': 10,
                'sql_script': self._generate_sqlserver_index_maintenance()
            })

            maintenance_tasks.append({
                'type': 'maintenance',
                'action': 'update_statistics',
                'priority': 'high',
                'description': 'Atualização de estatísticas',
                'estimated_duration_minutes': 15,
                'estimated_improvement_percent': 15,
                'sql_script': self._generate_sqlserver_stats_maintenance()
            })

        elif db_type == 'postgresql':
            maintenance_tasks.append({
                'type': 'maintenance',
                'action': 'vacuum_analyze',
                'priority': 'high',
                'description': 'VACUUM e ANALYZE em tabelas principais',
                'estimated_duration_minutes': 20,
                'estimated_improvement_percent': 12,
                'sql_script': self._generate_postgresql_maintenance()
            })

        elif db_type == 'hana':
            maintenance_tasks.append({
                'type': 'maintenance',
                'action': 'delta_merge',
                'priority': 'medium',
                'description': 'Delta merge em tabelas column-store',
                'estimated_duration_minutes': 25,
                'estimated_improvement_percent': 10,
                'sql_script': self._generate_hana_maintenance()
            })

        return maintenance_tasks

    def _create_query_optimization(
        self,
        query_info: Dict[str, Any],
        db_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Cria otimização para uma query específica.

        Args:
            query_info: Informações da query
            db_type: Tipo do banco

        Returns:
            Dicionário com plano de otimização ou None
        """
        # Calcular impacto estimado (ocorrências * tempo economizado)
        potential_saving_ms = query_info['avg_duration_ms'] * 0.5  # Assumir 50% de melhoria
        total_impact_ms = potential_saving_ms * query_info['occurrences']
        impact_hours = total_impact_ms / (1000 * 3600)

        return {
            'type': 'query_optimization',
            'priority': self._calculate_priority(query_info),
            'query_hash': query_info['query_hash'],
            'table': f"{query_info['database']}.{query_info['schema']}.{query_info['table']}",
            'description': f"Otimizar query com {query_info['occurrences']} ocorrências",
            'current_avg_duration_ms': query_info['avg_duration_ms'],
            'current_avg_cpu_ms': query_info['avg_cpu_ms'],
            'occurrences_per_week': query_info['occurrences'],
            'estimated_improvement_percent': 50,
            'estimated_time_saved_hours': round(impact_hours, 2),
            'severity': query_info['severity'] or 'medium',
            'llm_recommendations': query_info['recommendations'],
            'llm_analysis': query_info['analysis'],
            'action_required': 'review_and_rewrite',
            'notes': 'Revisar análise LLM e aplicar recomendações'
        }

    def _create_index_optimization(
        self,
        index_info: Dict[str, Any],
        db_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Cria otimização de índice.

        Args:
            index_info: Informações do índice sugerido
            db_type: Tipo do banco

        Returns:
            Dicionário com plano de criação de índice
        """
        # Gerar script SQL baseado no tipo de banco
        sql_script = self._generate_index_creation_script(index_info, db_type)

        # Estimar impacto baseado em número de queries afetadas
        estimated_improvement = min(70, index_info['query_count'] * 10)

        return {
            'type': 'create_index',
            'priority': 'high' if index_info['query_count'] >= 5 else 'medium',
            'table': f"{index_info['database']}.{index_info['schema']}.{index_info['table']}",
            'description': f"Criar índice para otimizar {index_info['query_count']} queries",
            'affected_queries': index_info['query_count'],
            'avg_logical_reads': index_info['avg_reads'],
            'estimated_improvement_percent': estimated_improvement,
            'estimated_duration_minutes': 5,
            'sql_script': sql_script,
            'action_required': 'execute_ddl',
            'notes': 'Verificar impacto no storage antes de executar'
        }

    def _create_statistics_optimization(
        self,
        stats_info: Dict[str, Any],
        db_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Cria otimização de estatísticas.

        Args:
            stats_info: Informações da tabela
            db_type: Tipo do banco

        Returns:
            Dicionário com plano de atualização de estatísticas
        """
        sql_script = self._generate_update_stats_script(stats_info, db_type)

        return {
            'type': 'update_statistics',
            'priority': 'high',
            'table': f"{stats_info['database']}.{stats_info['schema']}.{stats_info['table']}",
            'description': f"Atualizar estatísticas (tabela com {stats_info['problem_query_count']} queries problemáticas)",
            'problem_queries': stats_info['problem_query_count'],
            'estimated_improvement_percent': 20,
            'estimated_duration_minutes': 2,
            'sql_script': sql_script,
            'action_required': 'execute_maintenance',
            'notes': 'Estatísticas desatualizadas causam planos de execução ruins'
        }

    def _calculate_priority(self, query_info: Dict[str, Any]) -> str:
        """
        Calcula prioridade baseada em severidade e impacto.

        Args:
            query_info: Informações da query

        Returns:
            Prioridade: critical, high, medium, low
        """
        severity = query_info.get('severity', 'medium')
        occurrences = query_info['occurrences']
        avg_duration = query_info['avg_duration_ms']

        # Impacto total
        impact_score = occurrences * avg_duration

        if severity == 'critical' or impact_score > 500000:
            return 'critical'
        elif severity == 'high' or impact_score > 100000:
            return 'high'
        elif impact_score > 30000:
            return 'medium'
        else:
            return 'low'

    def _prioritize_optimizations(
        self,
        optimizations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Ordena otimizações por prioridade e impacto.

        Args:
            optimizations: Lista de otimizações

        Returns:
            Lista ordenada
        """
        priority_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}

        return sorted(
            optimizations,
            key=lambda x: (
                priority_order.get(x.get('priority', 'low'), 3),
                -x.get('estimated_improvement_percent', 0)
            )
        )

    def _calculate_plan_summary(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcula estatísticas agregadas do plano.

        Args:
            plan: Plano completo

        Returns:
            Dicionário com resumo
        """
        total_optimizations = 0
        total_duration = 0
        total_improvement = 0

        for instance_plan in plan['instances'].values():
            for opt in instance_plan['optimizations']:
                total_optimizations += 1
                total_duration += opt.get('estimated_duration_minutes', 0)
                total_improvement += opt.get('estimated_improvement_percent', 0)

        avg_improvement = total_improvement / total_optimizations if total_optimizations > 0 else 0

        return {
            'total_optimizations': total_optimizations,
            'estimated_improvement_percent': round(avg_improvement, 1),
            'execution_time_estimate_minutes': total_duration
        }

    def _save_plan(self, plan: Dict[str, Any]) -> Path:
        """
        Salva plano em arquivo JSON.

        Args:
            plan: Plano completo

        Returns:
            Path do arquivo salvo
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"optimization_plan_{timestamp}.json"
        filepath = self.output_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(plan, f, indent=2, ensure_ascii=False)

        return filepath

    # ========== GERADORES DE SCRIPTS SQL ==========

    def _generate_index_creation_script(
        self,
        index_info: Dict[str, Any],
        db_type: str
    ) -> str:
        """Gera script de criação de índice."""
        table_full = f"{index_info['database']}.{index_info['schema']}.{index_info['table']}"

        if db_type == 'sqlserver':
            return f"""
-- IMPORTANTE: Analisar queries específicas antes de executar
-- Este é um template que precisa ser ajustado com as colunas corretas

-- Verificar índices existentes
EXEC sp_helpindex '{index_info['schema']}.{index_info['table']}';

-- Criar índice (AJUSTAR COLUNAS!)
-- CREATE NONCLUSTERED INDEX IX_{index_info['table']}_COLS
-- ON {index_info['schema']}.{index_info['table']} (Col1, Col2)
-- INCLUDE (Col3, Col4);

-- TODO: Analisar queries do DuckDB e identificar colunas nos WHERE/JOIN
"""
        elif db_type == 'postgresql':
            return f"""
-- Verificar índices existentes
SELECT * FROM pg_indexes WHERE tablename = '{index_info['table']}';

-- Criar índice (AJUSTAR COLUNAS!)
-- CREATE INDEX CONCURRENTLY idx_{index_info['table']}_cols
-- ON {index_info['schema']}.{index_info['table']} (col1, col2);
"""
        elif db_type == 'hana':
            return f"""
-- Verificar índices existentes
SELECT * FROM SYS.INDEXES WHERE TABLE_NAME = '{index_info['table']}';

-- Criar índice
-- CREATE INDEX idx_{index_info['table']}_cols
-- ON {index_info['schema']}.{index_info['table']} (col1, col2);
"""
        return "-- Script não disponível para este tipo de banco"

    def _generate_update_stats_script(
        self,
        stats_info: Dict[str, Any],
        db_type: str
    ) -> str:
        """Gera script de atualização de estatísticas."""
        if db_type == 'sqlserver':
            return f"""
-- Atualizar estatísticas com FULLSCAN
UPDATE STATISTICS {stats_info['schema']}.{stats_info['table']} WITH FULLSCAN;
"""
        elif db_type == 'postgresql':
            return f"""
-- Analisar tabela
ANALYZE {stats_info['schema']}.{stats_info['table']};
"""
        elif db_type == 'hana':
            return f"""
-- Atualizar estatísticas
UPDATE STATISTICS FOR {stats_info['schema']}.{stats_info['table']};
"""
        return ""

    def _generate_sqlserver_index_maintenance(self) -> str:
        """Gera script de manutenção de índices SQL Server."""
        return """
-- Rebuild de índices fragmentados (>30%)
DECLARE @TableName NVARCHAR(255);
DECLARE @IndexName NVARCHAR(255);
DECLARE @SQL NVARCHAR(MAX);

DECLARE index_cursor CURSOR FOR
SELECT
    OBJECT_NAME(ips.object_id) AS TableName,
    i.name AS IndexName
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE ips.avg_fragmentation_in_percent > 30
  AND ips.page_count > 1000;

OPEN index_cursor;
FETCH NEXT FROM index_cursor INTO @TableName, @IndexName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = 'ALTER INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@TableName) + ' REBUILD;';
    PRINT @SQL;
    -- EXEC sp_executesql @SQL;  -- Descomentar para executar
    FETCH NEXT FROM index_cursor INTO @TableName, @IndexName;
END;

CLOSE index_cursor;
DEALLOCATE index_cursor;
"""

    def _generate_sqlserver_stats_maintenance(self) -> str:
        """Gera script de manutenção de estatísticas SQL Server."""
        return """
-- Atualizar estatísticas de todas as tabelas
EXEC sp_MSforeachtable 'UPDATE STATISTICS ? WITH FULLSCAN';
"""

    def _generate_postgresql_maintenance(self) -> str:
        """Gera script de manutenção PostgreSQL."""
        return """
-- VACUUM e ANALYZE
VACUUM (ANALYZE, VERBOSE);
"""

    def _generate_hana_maintenance(self) -> str:
        """Gera script de manutenção SAP HANA."""
        return """
-- Delta merge em tabelas column-store
MERGE DELTA OF "_SYS_BIC"."your.package/YourView";
"""
