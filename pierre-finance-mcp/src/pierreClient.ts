const BASE_URL = "https://www.pierre.finance/tools/api";
const MAX_RETRIES = 4;
const BASE_BACKOFF_MS = 500;

export class PierreAuthError extends Error {
  constructor(status: number, body: string) {
    super(
      status === 401
        ? `Pierre Finance API retornou 401 (não autorizado). Verifique se PIERRE_API_KEY está definida e correta, e se a assinatura Pierre está ativa. Detalhes: ${body}`
        : `Pierre Finance API retornou 403 (acesso negado). Detalhes: ${body}`
    );
    this.name = "PierreAuthError";
  }
}

export class PierreApiError extends Error {
  constructor(status: number, body: string) {
    super(`Pierre Finance API retornou erro ${status}. Detalhes: ${body}`);
    this.name = "PierreApiError";
  }
}

export interface PierreTransaction {
  id: string;
  description: string;
  category: string | null;
  currency_code: string;
  amount: number;
  date: string;
  type: "DEBIT" | "CREDIT" | string;
  status: string;
  account_name: string;
  account_type: string;
  account_subtype: string;
}

interface GetTransactionsParams {
  startDate?: string;
  endDate?: string;
  accountType?: string;
  categories?: string;
}

function getApiKey(): string {
  const key = process.env.PIERRE_API_KEY;
  if (!key) {
    throw new Error(
      "Variável de ambiente PIERRE_API_KEY não está definida. Gere uma chave em https://pierre.finance/api-key e exporte-a antes de iniciar o servidor."
    );
  }
  return key;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function pierreFetch(path: string, params: Record<string, string | undefined>): Promise<unknown> {
  const apiKey = getApiKey();
  const url = new URL(`${BASE_URL}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") {
      url.searchParams.set(key, value);
    }
  }

  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    let response: Response;
    try {
      response = await fetch(url, {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          Accept: "application/json",
        },
      });
    } catch (err) {
      lastError = new Error(
        `Falha de rede ao chamar a API do Pierre Finance: ${err instanceof Error ? err.message : String(err)}`
      );
      if (attempt === MAX_RETRIES) throw lastError;
      await sleep(BASE_BACKOFF_MS * 2 ** attempt);
      continue;
    }

    if (response.status === 429) {
      if (attempt === MAX_RETRIES) {
        throw new Error(
          "Pierre Finance API retornou 429 (limite de requisições excedido) repetidamente. Tente novamente em alguns instantes."
        );
      }
      const retryAfterHeader = response.headers.get("Retry-After");
      const retryAfterMs = retryAfterHeader ? Number(retryAfterHeader) * 1000 : undefined;
      const backoff = retryAfterMs && !Number.isNaN(retryAfterMs) ? retryAfterMs : BASE_BACKOFF_MS * 2 ** attempt;
      await sleep(backoff);
      continue;
    }

    if (response.status === 401 || response.status === 403) {
      const body = await response.text();
      throw new PierreAuthError(response.status, body);
    }

    if (!response.ok) {
      const body = await response.text();
      throw new PierreApiError(response.status, body);
    }

    return response.json();
  }

  throw lastError ?? new Error("Falha desconhecida ao chamar a API do Pierre Finance.");
}

export async function getTransactions(params: GetTransactionsParams): Promise<PierreTransaction[]> {
  const json = (await pierreFetch("/get-transactions", {
    startDate: params.startDate,
    endDate: params.endDate,
    accountType: params.accountType,
    categories: params.categories,
    format: "raw",
  })) as { success: boolean; data: PierreTransaction[] };

  if (!json.success || !Array.isArray(json.data)) {
    throw new Error("Resposta inesperada da API do Pierre Finance ao buscar transações.");
  }

  return json.data;
}
