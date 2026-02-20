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
RESET='\033[0m'

ok()   { echo -e "${GREEN}✓${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET}  $*"; }
fail() { echo -e "${RED}✗${RESET} $*"; exit 1; }
info() { echo -e "  $*"; }

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

# ── Virtualenv ─────────────────────────────────────────────────────────────────
if [[ ! -d "$VENV_DIR" ]]; then
    echo -e "\nCriando ambiente virtual..."
    python3 -m venv "$VENV_DIR"
    ok "Ambiente virtual criado em ${VENV_DIR}/"
else
    ok "Ambiente virtual já existe"
fi

# Ativar venv
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

# ── Dependências ───────────────────────────────────────────────────────────────
echo -e "\nInstalando dependências..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
ok "Dependências instaladas"

# ── Arquivos de configuração ───────────────────────────────────────────────────
echo

NEEDS_CONFIG=false

# .env
if [[ ! -f ".env" ]]; then
    cp .env.example .env
    ok ".env criado a partir de .env.example"
    NEEDS_CONFIG=true
else
    ok ".env já existe"
fi

# config/databases.json
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

# Checar GEMINI_API_KEY
GEMINI_KEY=$(grep -E '^GEMINI_API_KEY=' .env | cut -d= -f2 || true)
if [[ -z "$GEMINI_KEY" || "$GEMINI_KEY" == "your_gemini_api_key_here" ]]; then
    warn "GEMINI_API_KEY não configurada em .env"
    info "Obtenha em: https://makersuite.google.com/app/apikey"
    READY=false
else
    ok "GEMINI_API_KEY configurada"
fi

# Checar se databases.json tem ao menos uma entrada com enabled=true
ENABLED_DBS=$(python3 -c "
import json, sys
try:
    with open('config/databases.json') as f:
        data = json.load(f)
    enabled = [d for d in data.get('databases', []) if d.get('enabled', True)]
    print(len(enabled))
except Exception as e:
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
    [[ ! -f ".env" || "$GEMINI_KEY" == "your_gemini_api_key_here" ]] && info ".env — adicione GEMINI_API_KEY e senhas dos bancos"
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
