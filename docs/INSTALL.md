# Guia de Instalação - Multi-Database Monitor

Este guia cobre a instalação completa do Multi-Database Monitor do zero.

## Requisitos

### Sistema Operacional
- Linux (Ubuntu 20.04+, CentOS 8+, etc)
- macOS (10.15+)
- Windows 10/11 (com WSL2 recomendado)

### Software Necessário

#### Python
- **Python 3.9+** (recomendado: 3.11 ou 3.12)

```bash
# Verificar versão do Python
python3 --version
# ou
python --version
```

#### Drivers de Database

**SQL Server**:
- ODBC Driver 17 ou 18 para SQL Server

**PostgreSQL**:
- Bibliotecas cliente PostgreSQL (libpq)

**SAP HANA**:
- Cliente SAP HANA (hdbcli)

---

## Instalação Rápida

### 1. Clonar Repositório

```bash
# Clone o repositório (ou baixe o ZIP)
git clone https://github.com/seu-usuario/check_sql_server_performance.git
cd check_sql_server_performance
```

### 2. Criar Ambiente Virtual

```bash
# Criar venv
python3 -m venv .venv

# Ativar venv
# Linux/macOS:
source .venv/bin/activate
# Windows:
.venv\Scripts\activate
```

### 3. Instalar Dependências

```bash
# Atualizar pip
pip install --upgrade pip

# Instalar requirements
pip install -r requirements.txt
```

### 4. Instalar Drivers de Database

#### SQL Server (ODBC Driver)

**Ubuntu/Debian**:
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

**macOS**:
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql18
```

**Windows**:
- Baixe e instale: https://go.microsoft.com/fwlink/?linkid=2249004

#### PostgreSQL

**Ubuntu/Debian**:
```bash
sudo apt-get install -y libpq-dev
```

**macOS**:
```bash
brew install postgresql
```

**Windows**:
- Instalado automaticamente com psycopg2-binary

#### SAP HANA

**Todos os SOs**:
```bash
# Já incluído no requirements.txt
pip install hdbcli
```

### 5. Configurar Arquivos

```bash
# Copiar templates
cp .env.example .env
cp config/databases.json.example config/databases.json

# Editar configurações
nano .env
nano config/databases.json
```

### 6. Configurar Variáveis de Ambiente

Edite `.env`:

```env
# Google Gemini API Key (obrigatório)
GEMINI_API_KEY=sua_chave_api_aqui

# Senhas de Databases (recomendado)
SQL_SERVER_PROD_PASSWORD=sua_senha_sqlserver
POSTGRESQL_PROD_PASSWORD=sua_senha_postgresql
HANA_PROD_PASSWORD=sua_senha_hana
```

**Obter API Key do Gemini**:
1. Acesse: https://makersuite.google.com/app/apikey
2. Crie uma nova API Key (gratuita)
3. Copie e cole no `.env`

### 7. Configurar Databases

Edite `config/databases.json`:

```json
{
  "databases": [
    {
      "name": "SQL Server - Produção",
      "type": "SQLSERVER",
      "enabled": true,
      "credentials": {
        "server": "seu-servidor.com",
        "port": "1433",
        "database": "master",
        "username": "monitor_user",
        "password": "${SQL_SERVER_PROD_PASSWORD}",
        "driver": "ODBC Driver 18 for SQL Server"
      }
    }
  ]
}
```

### 8. Validar Configuração

```bash
# Validar arquivos de configuração
python validate_config.py
```

Se tudo estiver correto, você verá:

```
Todas as configurações estão válidas!
Você está pronto para executar o monitor
```

### 9. Executar o Monitor

```bash
# Executar
python main.py
```

---

## Instalação Detalhada por SO

### Ubuntu 20.04/22.04

```bash
# 1. Atualizar sistema
sudo apt-get update && sudo apt-get upgrade -y

# 2. Instalar Python 3.11
sudo apt-get install -y python3.11 python3.11-venv python3.11-dev

# 3. Instalar dependências do sistema
sudo apt-get install -y build-essential curl git

# 4. Instalar ODBC Driver (SQL Server)
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# 5. Instalar libpq (PostgreSQL)
sudo apt-get install -y libpq-dev

# 6. Clone e configure
git clone <repo-url>
cd check_sql_server_performance
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 7. Configure e valide
cp .env.example .env
cp config/databases.json.example config/databases.json
# (edite os arquivos)
python validate_config.py

# 8. Execute
python main.py
```

### macOS

```bash
# 1. Instalar Homebrew (se não tiver)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 2. Instalar Python 3.11
brew install python@3.11

# 3. Instalar drivers
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql18 postgresql

# 4. Clone e configure
git clone <repo-url>
cd check_sql_server_performance
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 5. Configure e valide
cp .env.example .env
cp config/databases.json.example config/databases.json
# (edite os arquivos)
python validate_config.py

# 6. Execute
python main.py
```

### Windows 10/11

**Opção 1: WSL2 (Recomendado)**

```powershell
# Instalar WSL2
wsl --install

# Reiniciar e abrir Ubuntu
# Seguir instruções de Ubuntu acima
```

**Opção 2: Windows Nativo**

```powershell
# 1. Instalar Python 3.11
# Baixe de: https://www.python.org/downloads/
# Marque "Add Python to PATH" durante instalação

# 2. Instalar ODBC Driver
# Baixe de: https://go.microsoft.com/fwlink/?linkid=2249004

# 3. Instalar Git
# Baixe de: https://git-scm.com/download/win

# 4. Clone e configure
git clone <repo-url>
cd check_sql_server_performance
python -m venv .venv
.venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

# 5. Configure
copy .env.example .env
copy config\databases.json.example config\databases.json
# (edite os arquivos)
python validate_config.py

# 6. Execute
python main.py
```

---

## Docker (Alternativa)

```bash
# Criar Dockerfile
cat > Dockerfile <<'EOF'
FROM python:3.11-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar ODBC Driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Configurar diretório de trabalho
WORKDIR /app

# Copiar requirements e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

CMD ["python", "main.py"]
EOF

# Build
docker build -t sql-monitor .

# Run
docker run -it --rm \
  --env-file .env \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/logs:/app/logs \
  sql-monitor
```

---

## Verificação Pós-Instalação

### 1. Testar Python e Dependências

```bash
python -c "import pyodbc; print('pyodbc OK')"
python -c "import psycopg2; print('psycopg2 OK')"
python -c "import hdbcli; print('hdbcli OK')"
python -c "import google.genai; print('google-genai OK')"
python -c "import pydantic; print('pydantic OK')"
```

### 2. Testar Drivers ODBC

```bash
# Listar drivers instalados
odbcinst -q -d

# Deve mostrar algo como:
# [ODBC Driver 18 for SQL Server]
```

### 3. Validar Configuração

```bash
python validate_config.py
```

### 4. Teste de Conexão

```bash
# Execute o monitor por 1 ciclo
python main.py

# Ctrl+C para parar
```

---

## Troubleshooting

### Erro: "No module named 'pyodbc'"

**Causa**: pyodbc não instalado ou venv não ativado
**Solução**:
```bash
source .venv/bin/activate  # Ativar venv
pip install pyodbc         # Reinstalar
```

### Erro: "Can't open lib 'ODBC Driver 18 for SQL Server'"

**Causa**: Driver ODBC não instalado
**Solução**: Instalar ODBC Driver (ver seção de instalação)

### Erro: "GEMINI_API_KEY não encontrada"

**Causa**: API key não configurada no .env
**Solução**:
```bash
echo "GEMINI_API_KEY=sua_chave_aqui" >> .env
```

### Erro: "Field required [type=missing]"

**Causa**: Campo obrigatório faltando em config.json
**Solução**: Execute `python validate_config.py` para ver qual campo

### Erro: "Porta deve estar entre 1-65535"

**Causa**: Porta inválida em databases.json
**Solução**: Use uma porta válida (ex: "1433", "5432", "30015")

---

## Próximos Passos

Após instalação bem-sucedida:

1. **Configurar Permissões de Database** - Ver [PERMISSIONS.md](PERMISSIONS.md)
2. **Entender Logging** - Ver [LOGGING.md](LOGGING.md)
3. **Configurar Segurança** - Ver [SECURITY.md](SECURITY.md)
4. **Customizar Configuração** - Ver [CONFIGURATION.md](CONFIGURATION.md)

---

## 🆘 Suporte

Se encontrar problemas:

1. **Verificar logs**: `logs/monitor.log`
2. **Validar configuração**: `python validate_config.py`
3. **Issues no GitHub**: [Reportar issue](https://github.com/seu-usuario/check_sql_server_performance/issues)

---

**Instalação concluída!**

Execute `python main.py` para iniciar o monitor.
