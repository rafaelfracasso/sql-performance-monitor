import type { PierreTransaction } from "./pierreClient.js";

export type Bucket = "essenciais" | "desejos" | "poupanca_investimentos" | "nao_classificado";

const BUCKET_TARGET_PCT: Record<Exclude<Bucket, "nao_classificado">, number> = {
  essenciais: 50,
  desejos: 30,
  poupanca_investimentos: 20,
};

// Mapeamento de categorias do Pierre (PT-BR) para os baldes da metodologia 50/30/20.
// Categorias não listadas caem em "nao_classificado" e são reportadas à parte
// para o usuário decidir manualmente, em vez de assumir um balde errado.
const CATEGORY_TO_BUCKET: Record<string, Bucket> = {
  moradia: "essenciais",
  aluguel: "essenciais",
  condominio: "essenciais",
  contas: "essenciais",
  "água": "essenciais",
  agua: "essenciais",
  luz: "essenciais",
  energia: "essenciais",
  internet: "essenciais",
  telefone: "essenciais",
  alimentação: "essenciais",
  alimentacao: "essenciais",
  supermercado: "essenciais",
  farmácia: "essenciais",
  farmacia: "essenciais",
  saúde: "essenciais",
  saude: "essenciais",
  transporte: "essenciais",
  combustível: "essenciais",
  combustivel: "essenciais",
  educação: "essenciais",
  educacao: "essenciais",
  seguro: "essenciais",

  lazer: "desejos",
  restaurantes: "desejos",
  "bares e restaurantes": "desejos",
  delivery: "desejos",
  compras: "desejos",
  vestuário: "desejos",
  vestuario: "desejos",
  eletrônicos: "desejos",
  eletronicos: "desejos",
  assinaturas: "desejos",
  streaming: "desejos",
  viagem: "desejos",
  viagens: "desejos",
  entretenimento: "desejos",
  beleza: "desejos",
  hobbies: "desejos",

  investimentos: "poupanca_investimentos",
  poupança: "poupanca_investimentos",
  poupanca: "poupanca_investimentos",
  previdência: "poupanca_investimentos",
  previdencia: "poupanca_investimentos",
  aplicações: "poupanca_investimentos",
  aplicacoes: "poupanca_investimentos",
};

function normalizeCategory(category: string | null): string {
  return (category ?? "sem categoria").trim();
}

function bucketFor(category: string): Bucket {
  const key = normalizeCategory(category).toLowerCase();
  return CATEGORY_TO_BUCKET[key] ?? "nao_classificado";
}

export interface CategoryTotal {
  category: string;
  total: number;
  percentOfSpend: number;
  transactionCount: number;
}

export interface BucketTotal {
  bucket: Bucket;
  total: number;
  percentOfSpend: number;
  targetPercent: number | null;
  percentOfIncome: number | null;
  categories: string[];
}

export interface SpendingSummary {
  period: { startDate?: string; endDate?: string };
  totalSpend: number;
  totalIncome: number | null;
  byCategory: CategoryTotal[];
  byBucket: BucketTotal[];
  unclassifiedCategories: string[];
  note: string;
}

export function summarizeSpending(
  transactions: PierreTransaction[],
  options: { startDate?: string; endDate?: string; monthlyIncome?: number }
): SpendingSummary {
  const expenses = transactions.filter((t) => t.amount < 0);
  const totalSpend = expenses.reduce((sum, t) => sum + Math.abs(t.amount), 0);

  const byCategoryMap = new Map<string, { total: number; count: number }>();
  for (const t of expenses) {
    const category = normalizeCategory(t.category);
    const entry = byCategoryMap.get(category) ?? { total: 0, count: 0 };
    entry.total += Math.abs(t.amount);
    entry.count += 1;
    byCategoryMap.set(category, entry);
  }

  const byCategory: CategoryTotal[] = [...byCategoryMap.entries()]
    .map(([category, { total, count }]) => ({
      category,
      total: round2(total),
      percentOfSpend: totalSpend > 0 ? round2((total / totalSpend) * 100) : 0,
      transactionCount: count,
    }))
    .sort((a, b) => b.total - a.total);

  const byBucketMap = new Map<Bucket, { total: number; categories: Set<string> }>();
  for (const c of byCategory) {
    const bucket = bucketFor(c.category);
    const entry = byBucketMap.get(bucket) ?? { total: 0, categories: new Set<string>() };
    entry.total += c.total;
    entry.categories.add(c.category);
    byBucketMap.set(bucket, entry);
  }

  const income = options.monthlyIncome && options.monthlyIncome > 0 ? options.monthlyIncome : null;

  const bucketOrder: Bucket[] = ["essenciais", "desejos", "poupanca_investimentos", "nao_classificado"];
  const byBucket: BucketTotal[] = bucketOrder
    .filter((b) => byBucketMap.has(b))
    .map((bucket) => {
      const entry = byBucketMap.get(bucket)!;
      return {
        bucket,
        total: round2(entry.total),
        percentOfSpend: totalSpend > 0 ? round2((entry.total / totalSpend) * 100) : 0,
        targetPercent: bucket === "nao_classificado" ? null : BUCKET_TARGET_PCT[bucket],
        percentOfIncome: income ? round2((entry.total / income) * 100) : null,
        categories: [...entry.categories].sort(),
      };
    });

  const unclassifiedCategories = byBucket.find((b) => b.bucket === "nao_classificado")?.categories ?? [];

  return {
    period: { startDate: options.startDate, endDate: options.endDate },
    totalSpend: round2(totalSpend),
    totalIncome: income,
    byCategory,
    byBucket,
    unclassifiedCategories,
    note:
      unclassifiedCategories.length > 0
        ? `Categorias não mapeadas para nenhum balde 50/30/20: ${unclassifiedCategories.join(", ")}. Classifique-as manualmente para um resultado mais preciso.`
        : "Todas as categorias foram classificadas em um dos baldes 50/30/20.",
  };
}

function round2(value: number): number {
  return Math.round(value * 100) / 100;
}
