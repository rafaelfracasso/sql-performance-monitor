# Guia de Segurança - Database Performance Monitor

Este documento descreve as práticas de segurança implementadas no projeto e como configurar credenciais de forma segura.

## 🔐 Sistema de Secrets Management

### Problema: Senhas em Plaintext

**NUNCA** armazene senhas em plaintext em arquivos de configuração:

```json
❌ ERRADO - Vulnerabilidade de segurança:
{
  "credentials": {
    "password": "minha_senha_123"
  }
}
```

### Solução: Variáveis de Ambiente

Use variáveis de ambiente para armazenar credenciais sensíveis:

```json
✅ CORRETO - Seguro:
{
  "credentials": {
    "password": "${SQL_SERVER_PROD_PASSWORD}"
  }
}
```

## 🚀 Como Configurar Credenciais

### 1. Configure Variáveis de Ambiente

**Opção A: Arquivo .env (Desenvolvimento)**

```bash
# Copie o template
cp .env.example .env

# Edite o arquivo .env e adicione suas senhas
nano .env
```

Exemplo de `.env`:
```env
GEMINI_API_KEY=sua_chave_gemini_aqui

# SQL Server
SQL_SERVER_PROD_PASSWORD=Senha@Forte123!
SQL_SERVER_DEV_PASSWORD=Senha@Forte456!

# PostgreSQL
POSTGRESQL_PROD_PASSWORD=OutraSenha@789!
```

**Opção B: Export (Linux/Mac - Temporário)**

```bash
export SQL_SERVER_PROD_PASSWORD='Senha@Forte123!'
export POSTGRESQL_PROD_PASSWORD='OutraSenha@789!'
export GEMINI_API_KEY='sua_chave_gemini'
```

**Opção C: Variáveis de Sistema (Produção - Permanente)**

```bash
# Linux/Mac - Adicione ao ~/.bashrc ou ~/.profile
echo 'export SQL_SERVER_PROD_PASSWORD="Senha@Forte123!"' >> ~/.bashrc
source ~/.bashrc

# Windows - Use setx
setx SQL_SERVER_PROD_PASSWORD "Senha@Forte123!"
```

### 2. Configure databases.json

```json
{
  "databases": [
    {
      "name": "SQL Server - Produção",
      "type": "SQLSERVER",
      "enabled": true,
      "credentials": {
        "server": "sqlserver-prod.example.com",
        "port": "1433",
        "database": "master",
        "username": "monitor_user",
        "password": "${SQL_SERVER_PROD_PASSWORD}",  // Referencia variável de ambiente
        "driver": "ODBC Driver 18 for SQL Server"
      }
    }
  ]
}
```

### 3. Execute o Monitor

```bash
python main.py
```

O sistema automaticamente:
1. Carrega variáveis de ambiente do arquivo `.env` (se existir)
2. Resolve referências `${VAR_NAME}` em `databases.json`
3. Valida se todas as variáveis necessárias existem
4. **Gera warning** se detectar senhas em plaintext

## 📋 Sintaxe de Referências

### Formato Básico

```
${NOME_DA_VARIAVEL}
```

### Exemplos

```json
{
  "password": "${SQL_PASSWORD}",          // ✅ Simples
  "username": "${DB_USER}",               // ✅ Qualquer campo
  "server": "db-${ENVIRONMENT}.com"      // ✅ Interpolação
}
```

### Validação Automática

Se uma variável de ambiente não existir, você verá:

```
✗ Erro ao resolver credenciais de SQL Server - Produção:
Variável de ambiente 'SQL_SERVER_PROD_PASSWORD' não encontrada.
Configure-a antes de executar o monitor.
Exemplo: export SQL_SERVER_PROD_PASSWORD='sua_senha_aqui'
```

## ⚠️ Avisos de Segurança

### Detecção de Plaintext

O sistema detecta automaticamente senhas em plaintext e gera warnings:

```
⚠️  AVISO DE SEGURANÇA: Senha em plaintext detectada!
Use variáveis de ambiente: password: '${SQL_PASSWORD}'
```

### Arquivos Protegidos

Os seguintes arquivos **NÃO** devem ser commitados ao Git:

```gitignore
# Credenciais reais
.env
config/databases.json

# OK para commitar (templates)
.env.example
config/databases.json.example
```

## 🏢 Ambientes de Produção

Para ambientes de produção, considere usar:

### AWS Secrets Manager

```python
import boto3

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return response['SecretString']

# Use no código
password = get_secret('prod/database/password')
```

### Azure Key Vault

```python
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://myvault.vault.azure.net/", credential=credential)

# Use no código
password = client.get_secret("database-password").value
```

### HashiCorp Vault

```bash
# Configure Vault
export VAULT_ADDR='http://127.0.0.1:8200'
vault kv put secret/database password=Senha@Forte123!

# Leia no código
vault kv get -field=password secret/database
```

## 🔍 Checklist de Segurança

Antes de fazer deploy, verifique:

- [ ] ✅ Todas as senhas usam `${VAR_NAME}` em `databases.json`
- [ ] ✅ Arquivo `.env` está no `.gitignore`
- [ ] ✅ Arquivo `config/databases.json` está no `.gitignore`
- [ ] ✅ Variáveis de ambiente configuradas no servidor de produção
- [ ] ✅ Nenhum warning de plaintext é exibido ao iniciar
- [ ] ✅ Senhas fortes (mínimo 12 caracteres, maiúsculas, números, símbolos)
- [ ] ✅ Credenciais rotacionadas regularmente
- [ ] ✅ Acesso ao servidor limitado (firewall, VPN)

## 🛡️ Outras Práticas de Segurança

### 1. Sanitização de Queries

Todas as queries são automaticamente sanitizadas antes do envio à LLM:

```sql
-- ANTES (com dados sensíveis):
SELECT * FROM Usuarios WHERE CPF = '12345678900'

-- DEPOIS (sanitizada):
SELECT * FROM Usuarios WHERE CPF = @p1_VARCHAR
```

### 2. Logs Sem Dados Sensíveis

Os logs **NÃO** contêm:
- Valores de dados reais
- Senhas ou credenciais
- Informações pessoais (PII)

### 3. Permissões Mínimas

O usuário de monitoramento deve ter **apenas**:
- `VIEW SERVER STATE` (SQL Server)
- `pg_read_all_stats` (PostgreSQL)
- `MONITORING` (SAP HANA)

Consulte [PERMISSIONS.md](PERMISSIONS.md) para detalhes.

## 📚 Referências

- [OWASP - Password Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)
- [12-Factor App - Config](https://12factor.net/config)
- [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/)
- [Azure Key Vault](https://azure.microsoft.com/en-us/services/key-vault/)
- [HashiCorp Vault](https://www.vaultproject.io/)

---

**Em caso de comprometimento de credenciais:**
1. Rotacione imediatamente todas as senhas afetadas
2. Revise logs de acesso aos bancos de dados
3. Atualize variáveis de ambiente em todos os servidores
4. Considere implementar autenticação multifator (MFA)
