# pierre-budget-mcp

Servidor MCP (Model Context Protocol) local, complementar ao MCP oficial do
[Pierre Finance](https://pierre.finance), que adiciona **uma única ferramenta
derivada**: `summarize_spending_by_category`, um resumo de gastos por
categoria segundo a metodologia **50/30/20** (essenciais / desejos /
poupança-investimentos).

## Por que este projeto existe (e por que ele é tão pequeno)

O Pierre Finance já expõe um servidor MCP oficial em `https://pierre.finance/mcp`
com as ferramentas `getAccounts`, `getBalance`, `getBalanceByAccount`,
`getTransactions`, `getInstallments`, `manualUpdate`, entre outras — ver
[MCP Tools](https://docs.pierre.finance/api-reference/mcp/tools). Ou seja,
contas, saldos, transações filtradas, parcelas e sincronização manual **já
estão cobertos** e não precisam ser reimplementados.

A única lacuna encontrada foi a agregação por categoria segundo 50/30/20 —
não existe endpoint nem ferramenta equivalente na API do Pierre. Este
repositório cobre exatamente essa lacuna, e nada mais, chamando diretamente
`GET /tools/api/get-transactions` da API REST do Pierre.

**Use os dois servidores MCP em conjunto**: o oficial para contas, saldos,
transações e parcelas; este aqui só para o resumo 50/30/20.

## Pré-requisitos

- Node.js 18+
- Uma API Key do Pierre Finance com assinatura ativa

### Gerando a API Key

1. Acesse https://pierre.finance/api-key
2. Faça login (ou crie uma conta)
3. Clique em "Generate API Key"
4. Copie a chave (formato `sk-...`) e guarde em local seguro

**Nunca** coloque a chave direto no código ou em arquivos versionados no
git. Use variável de ambiente.

## Instalação e build

```bash
cd pierre-finance-mcp
npm install
npm run build
```

Isso gera `dist/index.js`, o entrypoint do servidor.

## Configuração da API Key

Copie `.env.example` para `.env` (já ignorado pelo `.gitignore`) só para uso
manual/local, ou exporte a variável diretamente no shell:

```bash
export PIERRE_API_KEY="sk-your-api-key-here"
```

O servidor lê `PIERRE_API_KEY` do ambiente do processo — o `.env` não é
carregado automaticamente pelo código (evita dependência extra); use-o como
referência ou com `direnv`/`dotenv-cli` se preferir.

## Rodando localmente (teste manual)

```bash
PIERRE_API_KEY="sk-your-api-key-here" npm start
```

O servidor conversa via stdio (protocolo MCP padrão) — não é uma aplicação
HTTP para acessar em navegador. Use o inspetor oficial para testar
interativamente:

```bash
npx @modelcontextprotocol/inspector node dist/index.js
```

## Registrando no Claude Code

### 1. MCP oficial do Pierre (contas, saldos, transações, parcelas, sync)

```bash
claude mcp add --transport http pierre-finance https://pierre.finance/mcp \
  --header "Authorization: Bearer ${PIERRE_API_KEY}"
```

Ou, em `.mcp.json` na raiz do projeto onde você usa o Claude Code:

```json
{
  "mcpServers": {
    "pierre-finance": {
      "type": "http",
      "url": "https://pierre.finance/mcp",
      "headers": {
        "Authorization": "Bearer ${PIERRE_API_KEY}"
      }
    }
  }
}
```

O Claude Code expande `${PIERRE_API_KEY}` a partir do ambiente do shell em
que ele foi iniciado — a chave nunca fica hardcoded no arquivo.

### 2. Este servidor (`summarize_spending_by_category`)

```bash
claude mcp add pierre-budget \
  --env PIERRE_API_KEY="${PIERRE_API_KEY}" \
  -- node /caminho/absoluto/para/pierre-finance-mcp/dist/index.js
```

Ou, equivalente em `.mcp.json`:

```json
{
  "mcpServers": {
    "pierre-budget": {
      "command": "node",
      "args": ["/caminho/absoluto/para/pierre-finance-mcp/dist/index.js"],
      "env": {
        "PIERRE_API_KEY": "${PIERRE_API_KEY}"
      }
    }
  }
}
```

Ajuste o caminho absoluto para onde você clonou/buildou este projeto.

## Registrando no Claude Desktop

Edite `claude_desktop_config.json` (Settings → Developer → Edit Config) e
combine os dois servidores:

```json
{
  "mcpServers": {
    "pierre-finance": {
      "url": "https://pierre.finance/mcp",
      "headers": {
        "Authorization": "Bearer sk-your-api-key-here"
      }
    },
    "pierre-budget": {
      "command": "node",
      "args": ["/caminho/absoluto/para/pierre-finance-mcp/dist/index.js"],
      "env": {
        "PIERRE_API_KEY": "sk-your-api-key-here"
      }
    }
  }
}
```

O Claude Desktop não expande variáveis de ambiente do shell dentro deste
arquivo, então a chave precisa estar no valor literal aqui. Esse arquivo de
configuração é local (fora de qualquer repositório git) — mantenha-o assim e
não o publique nem faça commit dele em nenhum projeto.

## Ferramenta exposta

### `summarize_spending_by_category`

Busca as transações do período (via `GET /tools/api/get-transactions` do
Pierre) e agrega:

- Total gasto por categoria (com % do total gasto)
- Total gasto por balde 50/30/20 (`essenciais`, `desejos`,
  `poupanca_investimentos`), com % do total gasto e, se `monthlyIncome` for
  informado, % da renda e comparação com a meta (50% / 30% / 20%)
- Categorias não mapeadas para nenhum balde (`nao_classificado`), listadas
  explicitamente em vez de forçadas para um balde errado

**Parâmetros (todos opcionais):**

| Parâmetro | Tipo | Descrição |
|---|---|---|
| `startDate` | `string` (YYYY-MM-DD) | Início do período. Padrão: 3 meses atrás |
| `endDate` | `string` (YYYY-MM-DD) | Fim do período. Padrão: hoje |
| `accountType` | `BANK \| CREDIT \| INVESTMENT \| LOAN` | Filtra por tipo de conta |
| `monthlyIncome` | `number` | Renda mensal, para calcular % sobre a renda |

**Exemplo de uso no Claude Code**, depois de registrar os dois servidores:

> "Quanto gastei em alimentação este mês?" → o Claude usa `getTransactions`
> (MCP oficial) com filtro de categoria.
>
> "Monta meu 50/30/20 com os dados reais do Pierre, minha renda é R$ 8000" →
> o Claude usa `summarize_spending_by_category` (este servidor) com
> `monthlyIncome: 8000`.

A lista de categorias reconhecidas para cada balde fica em
[`src/categorize.ts`](./src/categorize.ts) — ajuste o mapeamento se suas
categorias no Pierre usarem nomes diferentes dos previstos.

## Tratamento de erros e rate limiting

- **401/403**: mensagem clara indicando que a `PIERRE_API_KEY` está ausente,
  inválida, ou que a assinatura Pierre não está ativa.
- **429**: retry automático com backoff exponencial (até 4 tentativas),
  respeitando o header `Retry-After` quando presente.
- **Erros de rede**: retry com backoff exponencial; mensagem clara após
  esgotar as tentativas.
- Qualquer erro é retornado como resultado de ferramenta MCP com
  `isError: true` e uma mensagem em português — nunca derruba o processo.

## Privacidade

Este servidor não grava nenhum dado financeiro em disco. Cada chamada busca
as transações do período diretamente na API do Pierre e descarta os dados
ao final da resposta — não há cache persistente.

## Estrutura

```
pierre-finance-mcp/
├── src/
│   ├── index.ts        # Entry point MCP (registra a ferramenta)
│   ├── pierreClient.ts # Cliente REST: auth, retry/backoff, erros
│   └── categorize.ts   # Mapeamento de categorias → baldes 50/30/20
├── .env.example
└── package.json
```
