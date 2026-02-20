#!/usr/bin/env bash
# start.sh — Setup e inicializacao do SQL Monitor
set -euo pipefail

VENV_DIR=".venv"
PYTHON_MIN="3.11"

# ── Cores ──────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

ok()   { echo -e "${GREEN}✓${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET}  $*"; }
fail() { echo -e "${RED}✗${RESET} $*"; exit 1; }
info() { echo -e "  ${DIM}$*${RESET}"; }

echo -e "\n${BOLD}SQL Monitor — Setup${RESET}\n"

# ── Python ─────────────────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    fail "python3 não encontrado. Instale Python ${PYTHON_MIN}+."
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PYTHON_OK=$(python3 -c "import sys; print('ok' if sys.version_info >= (3, 11) else 'fail')")
if [[ "$PYTHON_OK" != "ok" ]]; then
    fail "Python ${PYTHON_VERSION} encontrado. Necessário: ${PYTHON_MIN}+."
fi
ok "Python ${PYTHON_VERSION}"

# ── Selecao de drivers ─────────────────────────────────────────────────────────
echo -e "\n${BOLD}Bancos de dados a monitorar:${RESET}"
echo -e "${DIM}Use ↑↓ para navegar, Espaço para marcar, Enter para confirmar${RESET}\n"

DB_LABELS=("SQL Server" "PostgreSQL" "SAP HANA")
DB_KEYS=("sqlserver" "postgresql" "hana")
DB_SELECTED=(false false false)
DB_CURRENT=0

_redraw_menu() {
    for i in "${!DB_LABELS[@]}"; do
        local mark color prefix
        [[ "${DB_SELECTED[$i]}" == true ]] && mark="${GREEN}[x]${RESET}" || mark="[ ]"
        if [[ $i -eq $DB_CURRENT ]]; then
            prefix="${BOLD}▶${RESET}"
        else
            prefix=" "
        fi
        tput el
        echo -e " ${prefix} ${mark} ${DB_LABELS[$i]}"
    done
    tput cuu ${#DB_LABELS[@]}
}

tput civis  # ocultar cursor
_redraw_menu

while true; do
    IFS= read -rsn1 key
    if [[ "$key" == $'\x1b' ]]; then
        IFS= read -rsn2 rest
        key="${key}${rest}"
    fi

    case "$key" in
        $'\x1b[A'|k)  # seta cima
            (( DB_CURRENT-- )) || true
            [[ $DB_CURRENT -lt 0 ]] && DB_CURRENT=$(( ${#DB_LABELS[@]} - 1 ))
            ;;
        $'\x1b[B'|j)  # seta baixo
            (( DB_CURRENT++ )) || true
            [[ $DB_CURRENT -ge ${#DB_LABELS[@]} ]] && DB_CURRENT=0
            ;;
        ' ')  # espaço — toggle
            if [[ "${DB_SELECTED[$DB_CURRENT]}" == true ]]; then
                DB_SELECTED[$DB_CURRENT]=false
            else
                DB_SELECTED[$DB_CURRENT]=true
            fi
            ;;
        '')  # Enter
            break
            ;;
    esac
    _redraw_menu
done

tput cud ${#DB_LABELS[@]}
tput cnorm  # mostrar cursor
echo

# Montar lista de selecionados
INSTALL_SQLSERVER=false
INSTALL_POSTGRESQL=false
INSTALL_HANA=false
SELECTED_LABELS=()

for i in "${!DB_KEYS[@]}"; do
    if [[ "${DB_SELECTED[$i]}" == true ]]; then
        SELECTED_LABELS+=("${DB_LABELS[$i]}")
        case "${DB_KEYS[$i]}" in
            sqlserver)  INSTALL_SQLSERVER=true ;;
            postgresql) INSTALL_POSTGRESQL=true ;;
            hana)       INSTALL_HANA=true ;;
        esac
    fi
done

if [[ ${#SELECTED_LABELS[@]} -eq 0 ]]; then
    warn "Nenhum banco selecionado. Selecione ao menos um e rode novamente."
    exit 0
fi

ok "Selecionado: ${SELECTED_LABELS[*]}"

# ── Virtualenv ─────────────────────────────────────────────────────────────────
echo
if [[ ! -d "$VENV_DIR" ]]; then
    echo "Criando ambiente virtual..."
    python3 -m venv "$VENV_DIR"
    ok "Ambiente virtual criado em ${VENV_DIR}/"
else
    ok "Ambiente virtual já existe"
fi

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

# ── Pacotes base ───────────────────────────────────────────────────────────────
echo -e "\nInstalando pacotes base..."
pip install -q --upgrade pip
pip install -q \
    "google-genai>=0.1.0" \
    "sqlparse>=0.4.4" \
    "python-dotenv>=1.0.0" \
    "pydantic>=2.0.0" \
    "pydantic-settings>=2.0.0" \
    "schedule>=1.2.0" \
    "requests>=2.31.0" \
    "typing-extensions>=4.8.0" \
    "duckdb>=0.9.0" \
    "fastapi>=0.104.0" \
    "uvicorn[standard]>=0.24.0" \
    "jinja2>=3.1.0" \
    "python-multipart>=0.0.6"
ok "Pacotes base instalados"

# ── Driver PostgreSQL ──────────────────────────────────────────────────────────
if [[ "$INSTALL_POSTGRESQL" == true ]]; then
    echo -e "\nInstalando driver PostgreSQL..."
    pip install -q "psycopg2-binary>=2.9.0"
    ok "psycopg2-binary instalado"
fi

# ── Driver SAP HANA ────────────────────────────────────────────────────────────
if [[ "$INSTALL_HANA" == true ]]; then
    echo -e "\nInstalando driver SAP HANA..."
    pip install -q "hdbcli>=2.19.0"
    ok "hdbcli instalado"
fi

# ── Driver SQL Server ──────────────────────────────────────────────────────────
if [[ "$INSTALL_SQLSERVER" == true ]]; then
    echo -e "\nInstalando driver SQL Server..."
    pip install -q "pyodbc>=5.0.0"
    ok "pyodbc instalado"

    # Verificar se o ODBC Driver 18 já está instalado no sistema
    if odbcinst -q -d 2>/dev/null | grep -qi "odbc driver 18"; then
        ok "ODBC Driver 18 for SQL Server já instalado"
    else
        echo -e "\n${BOLD}Instalando ODBC Driver 18 for SQL Server...${RESET}"

        if ! command -v sudo &>/dev/null; then
            warn "sudo não disponível — instale o ODBC Driver manualmente:"
            info "https://learn.microsoft.com/pt-br/sql/connect/odbc/download-odbc-driver-for-sql-server"
        elif command -v apt-get &>/dev/null; then
            # Debian / Ubuntu / WSL
            DISTRO=$(. /etc/os-release && echo "$ID")
            VERSION=$(. /etc/os-release && echo "$VERSION_ID")
            echo "  Adicionando repositório Microsoft (${DISTRO} ${VERSION})..."
            curl -sSL "https://packages.microsoft.com/keys/microsoft.asc" \
                | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg 2>/dev/null
            curl -sSL "https://packages.microsoft.com/config/${DISTRO}/${VERSION}/prod.list" \
                | sudo tee /etc/apt/sources.list.d/mssql-release.list > /dev/null
            sudo apt-get update -qq
            sudo ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18 unixodbc-dev
            ok "ODBC Driver 18 instalado"
        elif command -v yum &>/dev/null || command -v dnf &>/dev/null; then
            # RHEL / CentOS / Fedora
            PKG_MGR=$(command -v dnf || command -v yum)
            curl -sSL "https://packages.microsoft.com/config/rhel/8/prod.repo" \
                | sudo tee /etc/yum.repos.d/mssql-release.repo > /dev/null
            sudo ACCEPT_EULA=Y "$PKG_MGR" install -y msodbcsql18 unixODBC-devel
            ok "ODBC Driver 18 instalado"
        else
            warn "Distribuição não reconhecida — instale o ODBC Driver manualmente:"
            info "https://learn.microsoft.com/pt-br/sql/connect/odbc/download-odbc-driver-for-sql-server"
        fi
    fi
fi

# ── Arquivos de configuração ───────────────────────────────────────────────────
echo
NEEDS_CONFIG=false

if [[ ! -f ".env" ]]; then
    cp .env.example .env
    ok ".env criado a partir de .env.example"
    NEEDS_CONFIG=true
else
    ok ".env já existe"
fi

if [[ ! -f "config/databases.json" ]]; then
    cp config/databases.json.example config/databases.json
    ok "config/databases.json criado a partir do exemplo"
    NEEDS_CONFIG=true
else
    ok "config/databases.json já existe"
fi

# ── Verificações de configuração ───────────────────────────────────────────────
echo
READY=true

GEMINI_KEY=$(grep -E '^GEMINI_API_KEY=' .env | cut -d= -f2 || true)
if [[ -z "$GEMINI_KEY" || "$GEMINI_KEY" == "your_gemini_api_key_here" ]]; then
    warn "GEMINI_API_KEY não configurada em .env"
    info "Obtenha em: https://makersuite.google.com/app/apikey"
    READY=false
else
    ok "GEMINI_API_KEY configurada"
fi

ENABLED_DBS=$(python3 -c "
import json
try:
    with open('config/databases.json') as f:
        data = json.load(f)
    enabled = [d for d in data.get('databases', []) if d.get('enabled', True)]
    print(len(enabled))
except:
    print(0)
")

if [[ "$ENABLED_DBS" -eq 0 ]]; then
    warn "Nenhum banco habilitado em config/databases.json"
    info "Edite o arquivo e configure ao menos um banco de dados."
    READY=false
else
    ok "${ENABLED_DBS} banco(s) configurado(s) em databases.json"
fi

# ── Resultado ──────────────────────────────────────────────────────────────────
echo

if [[ "$NEEDS_CONFIG" == true ]]; then
    echo -e "${YELLOW}Arquivos de configuração criados. Edite antes de continuar:${RESET}"
    info ".env — adicione GEMINI_API_KEY e senhas dos bancos"
    info "config/databases.json — configure servidor, porta e credenciais"
    echo
fi

if [[ "$READY" == false ]]; then
    echo -e "${YELLOW}Configure os itens acima e rode ${BOLD}./start.sh${RESET}${YELLOW} novamente.${RESET}\n"
    exit 0
fi

# ── Iniciar ────────────────────────────────────────────────────────────────────
echo -e "${BOLD}Iniciando SQL Monitor...${RESET}\n"
python main.py
