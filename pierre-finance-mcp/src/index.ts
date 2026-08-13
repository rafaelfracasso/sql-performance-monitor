#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { getTransactions } from "./pierreClient.js";
import { summarizeSpending } from "./categorize.js";

const server = new McpServer({
  name: "pierre-budget-mcp",
  version: "0.1.0",
});

server.registerTool(
  "summarize_spending_by_category",
  {
    title: "Resumo de gastos por categoria (metodologia 50/30/20)",
    description:
      "Busca as transações do período no Pierre Finance e agrega os gastos por categoria e pelos baldes " +
      "da metodologia 50/30/20 (essenciais, desejos, poupança/investimentos), sem precisar categorizar " +
      "manualmente fatura por fatura. Ferramenta derivada — não existe endpoint equivalente na API do Pierre.",
    inputSchema: {
      startDate: z
        .string()
        .regex(/^\d{4}-\d{2}-\d{2}$/)
        .optional()
        .describe("Data inicial no formato YYYY-MM-DD. Padrão: 3 meses atrás."),
      endDate: z
        .string()
        .regex(/^\d{4}-\d{2}-\d{2}$/)
        .optional()
        .describe("Data final no formato YYYY-MM-DD. Padrão: hoje."),
      accountType: z
        .enum(["BANK", "CREDIT", "INVESTMENT", "LOAN"])
        .optional()
        .describe("Filtra transações por tipo de conta."),
      monthlyIncome: z
        .number()
        .positive()
        .optional()
        .describe("Renda mensal (opcional). Se informada, calcula o percentual de cada balde sobre a renda, além do percentual sobre o total gasto."),
    },
  },
  async ({ startDate, endDate, accountType, monthlyIncome }) => {
    try {
      const transactions = await getTransactions({ startDate, endDate, accountType });
      const summary = summarizeSpending(transactions, { startDate, endDate, monthlyIncome });
      return {
        content: [{ type: "text", text: JSON.stringify(summary, null, 2) }],
      };
    } catch (error) {
      return {
        isError: true,
        content: [
          {
            type: "text",
            text: error instanceof Error ? error.message : String(error),
          },
        ],
      };
    }
  }
);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((error) => {
  console.error("Falha ao iniciar o servidor MCP pierre-budget-mcp:", error);
  process.exit(1);
});
