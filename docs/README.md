# Documentação - SQL Performance Monitor

Documentação completa do projeto SQL Performance Monitor.

## 📚 Índice

### 🚀 Primeiros Passos
- **[../README.md](../README.md)** - Visão geral do projeto e início rápido
- **[INSTALL.md](INSTALL.md)** - Guia de instalação e configuração inicial

### ⚙️ Configuração
- **[CONFIGURATION.md](CONFIGURATION.md)** - Configurações detalhadas (config.json, databases.json)
- **[SECURITY.md](SECURITY.md)** - Segurança e gerenciamento de credenciais
- **[PERMISSIONS.md](PERMISSIONS.md)** - Permissões necessárias nos bancos de dados

### 🔍 Monitoramento
- **[OBSERVABILITY.md](OBSERVABILITY.md)** - Observabilidade, métricas e monitoramento
- **[LOGGING.md](LOGGING.md)** - Sistema de logging estruturado
- **[WEEKLY_OPTIMIZER.md](WEEKLY_OPTIMIZER.md)** - Otimizador semanal automático

### 🧪 Testes
- **[TESTING.md](TESTING.md)** - Guia completo de testes
- **[TEST_RESULTS.md](TEST_RESULTS.md)** - Resultados da última execução de testes

### 📝 Histórico
- **[CHANGELOG.md](CHANGELOG.md)** - Histórico de mudanças e versões
- **[TASKS.md](TASKS.md)** - Tarefas e progresso do projeto

---

## 📖 Documentação por Categoria

### Para Iniciantes

**Começar aqui**:
1. [../README.md](../README.md) - Visão geral
2. [INSTALL.md](INSTALL.md) - Instalação
3. [CONFIGURATION.md](CONFIGURATION.md) - Configuração básica

### Para Administradores de Banco

**Configurar permissões e monitoramento**:
1. [PERMISSIONS.md](PERMISSIONS.md) - Permissões necessárias
2. [SECURITY.md](SECURITY.md) - Segurança e credenciais
3. [OBSERVABILITY.md](OBSERVABILITY.md) - Métricas e alertas

### Para Desenvolvedores

**Contribuir com o projeto**:
1. [TESTING.md](TESTING.md) - Como executar testes
2. [LOGGING.md](LOGGING.md) - Sistema de logging
3. [CHANGELOG.md](CHANGELOG.md) - Histórico de mudanças

### Para DevOps

**Deploy e produção**:
1. [INSTALL.md](INSTALL.md) - Instalação em produção
2. [SECURITY.md](SECURITY.md) - Boas práticas de segurança
3. [OBSERVABILITY.md](OBSERVABILITY.md) - Monitoramento do sistema

---

## 🎯 Guias Rápidos

### Como configurar um novo banco de dados

1. Adicione as credenciais no `.env`:
   ```bash
   SQL_SERVER=seu_servidor
   SQL_PASSWORD=sua_senha
   ```

2. Configure em `config/databases.json`:
   ```json
   {
     "databases": [
       {
         "enabled": true,
         "name": "Produção SQL Server",
         "type": "SQLSERVER",
         "credentials": {
           "server": "${SQL_SERVER}",
           "password": "${SQL_PASSWORD}"
         }
       }
     ]
   }
   ```

3. Valide a configuração:
   ```bash
   python scripts/validate_config.py
   ```

4. Execute:
   ```bash
   python main.py
   ```

**Mais detalhes**: [CONFIGURATION.md](CONFIGURATION.md)

---

### Como habilitar logging JSON

1. Edite `config.json`:
   ```json
   {
     "logging": {
       "format": "json",
       "level": "INFO"
     }
   }
   ```

2. Reinicie o sistema:
   ```bash
   python main.py
   ```

**Mais detalhes**: [LOGGING.md](LOGGING.md)

---

### Como executar testes

1. Instale dependências:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure credenciais (opcional para testes de integração):
   ```bash
   cp .env.example .env
   # Edite .env
   ```

3. Execute testes:
   ```bash
   # Todos os testes
   python -m pytest tests/

   # Apenas unitários (não requerem banco)
   python -m pytest tests/unit/
   ```

**Mais detalhes**: [TESTING.md](TESTING.md)

---

## 📊 Arquivos de Documentação

### CHANGELOG.md
Histórico de todas as mudanças, versões e features adicionadas ao projeto.

**Quando consultar**: Para saber o que mudou entre versões.

### CONFIGURATION.md
Guia detalhado de todas as configurações disponíveis nos arquivos `config.json` e `config/databases.json`.

**Quando consultar**: Para configurar o sistema ou entender uma opção específica.

### INSTALL.md
Passo a passo de instalação do sistema em diferentes ambientes (desenvolvimento, produção, Docker).

**Quando consultar**: Na primeira instalação ou ao fazer deploy.

### LOGGING.md
Documentação completa do sistema de logging estruturado, formatos disponíveis e integração com ferramentas de análise.

**Quando consultar**: Para configurar logs ou integrar com ELK/Splunk.

### OBSERVABILITY.md
Métricas, monitoramento e observabilidade do sistema usando DuckDB e potencial integração com Prometheus.

**Quando consultar**: Para entender métricas ou configurar alertas.

### PERMISSIONS.md
Lista completa de permissões necessárias em cada banco de dados (SQL Server, PostgreSQL, SAP HANA).

**Quando consultar**: Ao configurar usuários de banco ou resolver erros de permissão.

### SECURITY.md
Boas práticas de segurança, gerenciamento de credenciais e integração com AWS Secrets Manager / Azure Key Vault.

**Quando consultar**: Para configurar credenciais de forma segura.

### TASKS.md
Tarefas pendentes e progresso do projeto (FASE 1-7).

**Quando consultar**: Para ver o que já foi implementado e o que falta.

### TESTING.md
Guia completo de testes: estrutura, como executar, troubleshooting e cobertura.

**Quando consultar**: Antes de executar testes ou contribuir com novos testes.

### TEST_RESULTS.md
Resultados da última execução dos testes, incluindo componentes validados e problemas encontrados.

**Quando consultar**: Para ver o estado atual dos testes.

### WEEKLY_OPTIMIZER.md
Documentação do otimizador semanal automático e sistema de aprovação de mudanças.

**Quando consultar**: Para entender o processo de otimização automática.

---

## 🔗 Links Externos

- **Repositório**: https://github.com/rafaelfracasso/sql-performance-monitor
- **Issues**: https://github.com/rafaelfracasso/sql-performance-monitor/issues
- **Pull Requests**: https://github.com/rafaelfracasso/sql-performance-monitor/pulls

---

## 🤝 Contribuindo

Para contribuir com a documentação:

1. **Encontrou um erro?** Abra uma issue
2. **Quer adicionar algo?** Faça um PR
3. **Tem dúvidas?** Consulte [../README.md](../README.md)

### Convenções de Documentação

- Use Markdown (GitHub-flavored)
- Mantenha linhas < 120 caracteres
- Use emojis para seções (📚 🎯 ⚙️ etc)
- Inclua exemplos práticos
- Mantenha atualizado (adicione data de atualização)

---

**Última atualização**: 2026-01-07
