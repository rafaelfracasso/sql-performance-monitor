#!/usr/bin/env python3
"""
Script para validar arquivos de configuração sem executar o monitor.

Uso:
    python validate_config.py
    python validate_config.py --config custom_config.json
    python validate_config.py --databases custom_databases.json
"""
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Carregar .env
load_dotenv()

from sql_monitor.config import validate_config_file, validate_databases_file


def print_success(message: str):
    """Imprime mensagem de sucesso."""
    print(f"✅ {message}")


def print_error(message: str):
    """Imprime mensagem de erro."""
    print(f"❌ {message}")


def print_warning(message: str):
    """Imprime mensagem de aviso."""
    print(f"⚠️  {message}")


def validate_config(config_path: str) -> bool:
    """
    Valida config.json.

    Args:
        config_path: Caminho para config.json.

    Returns:
        True se válido, False caso contrário.
    """
    print(f"\n{'='*80}")
    print(f"Validando: {config_path}")
    print(f"{'='*80}\n")

    try:
        config = validate_config_file(config_path)
        print_success("config.json está válido!")

        # Mostrar resumo
        print("\n📊 Resumo da Configuração:")
        print(f"  • Intervalo de monitoramento: {config.monitor.interval_seconds}s")
        print(f"  • Modelo LLM: {config.llm.model}")
        print(f"  • Temperatura: {config.llm.temperature}")
        print(f"  • Max requests/dia: {config.llm.rate_limit.max_requests_per_day}")
        print(f"  • Cache habilitado: {'Sim' if config.query_cache.enabled else 'Não'}")
        print(f"  • TTL do cache: {config.query_cache.ttl_hours}h")
        print(f"  • Teams habilitado: {'Sim' if config.teams.enabled else 'Não'}")
        print(f"  • Nível de log: {config.structured_logging.level}")
        print(f"  • Formato de log: {config.structured_logging.format}")

        # Avisos
        if config.llm.rate_limit.max_requests_per_day < 100:
            print_warning(f"Rate limit baixo: {config.llm.rate_limit.max_requests_per_day} requests/dia")

        if config.teams.enabled and not config.teams.webhook_url:
            print_error("Teams habilitado mas webhook_url não configurado")
            return False

        return True

    except FileNotFoundError as e:
        print_error(str(e))
        print(f"\n💡 Dica: Certifique-se de que {config_path} existe")
        return False

    except ValueError as e:
        print_error(f"Erro de validação:")
        print(f"\n{e}\n")
        print("💡 Dica: Corrija os erros acima e tente novamente")
        return False

    except Exception as e:
        print_error(f"Erro inesperado: {e}")
        return False


def validate_databases(databases_path: str) -> bool:
    """
    Valida databases.json.

    Args:
        databases_path: Caminho para databases.json.

    Returns:
        True se válido, False caso contrário.
    """
    print(f"\n{'='*80}")
    print(f"Validando: {databases_path}")
    print(f"{'='*80}\n")

    try:
        databases = validate_databases_file(databases_path)
        print_success("databases.json está válido!")

        # Mostrar resumo
        total = len(databases.databases)
        enabled = sum(1 for db in databases.databases if db.enabled)
        disabled = total - enabled

        print(f"\n📊 Resumo de Databases:")
        print(f"  • Total configurados: {total}")
        print(f"  • Habilitados: {enabled}")
        print(f"  • Desabilitados: {disabled}")

        print(f"\n📋 Databases Habilitados:")
        for db in databases.databases:
            if db.enabled:
                status = "🟢"
                # Verificar se senha é variável de ambiente
                has_env_var = db.credentials.password.startswith('${')
                security = "🔒" if has_env_var else "⚠️"
                print(f"  {status} {security} {db.name} ({db.type}) - {db.credentials.server}:{db.credentials.port}")

                if not has_env_var:
                    print_warning(f"    Senha em plaintext detectada em '{db.name}'")
                    print(f"      Recomendado: use ${{VAR_NAME}} para maior segurança")

        if disabled > 0:
            print(f"\n📋 Databases Desabilitados:")
            for db in databases.databases:
                if not db.enabled:
                    print(f"  🔴 {db.name} ({db.type})")

        # Avisos
        if enabled == 0:
            print_error("Nenhum database habilitado!")
            print("💡 Dica: Configure pelo menos um database com 'enabled': true")
            return False

        return True

    except FileNotFoundError as e:
        print_error(str(e))
        print(f"\n💡 Dica: Copie config/databases.json.example para {databases_path}")
        return False

    except ValueError as e:
        print_error(f"Erro de validação:")
        print(f"\n{e}\n")
        print("💡 Dicas:")
        print("  • Verifique se 'type' está correto (SQLSERVER, POSTGRESQL, HANA)")
        print("  • Verifique se todos os campos obrigatórios estão preenchidos")
        print("  • Verifique se a porta é um número válido (1-65535)")
        return False

    except Exception as e:
        print_error(f"Erro inesperado: {e}")
        return False


def main():
    """Função principal."""
    parser = argparse.ArgumentParser(
        description="Valida arquivos de configuração do Multi-Database Monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python validate_config.py                    # Valida config.json e databases.json
  python validate_config.py --config custom.json
  python validate_config.py --databases custom_db.json
  python validate_config.py --config custom.json --databases custom_db.json
        """
    )

    parser.add_argument(
        '--config',
        default='config.json',
        help='Caminho para config.json (padrão: config.json)'
    )

    parser.add_argument(
        '--databases',
        default='config/databases.json',
        help='Caminho para databases.json (padrão: config/databases.json)'
    )

    args = parser.parse_args()

    print("\n" + "="*80)
    print("VALIDADOR DE CONFIGURAÇÃO - Multi-Database Monitor")
    print("="*80)

    # Validar ambos os arquivos
    config_valid = validate_config(args.config)
    databases_valid = validate_databases(args.databases)

    # Resultado final
    print(f"\n{'='*80}")
    print("RESULTADO DA VALIDAÇÃO")
    print(f"{'='*80}\n")

    if config_valid and databases_valid:
        print_success("Todas as configurações estão válidas!")
        print("\n🚀 Você está pronto para executar o monitor:")
        print("   python main.py")
        return 0
    else:
        print_error("Foram encontrados erros nas configurações")
        print("\n🔧 Corrija os erros acima e execute novamente:")
        print("   python validate_config.py")
        return 1


if __name__ == "__main__":
    sys.exit(main())
