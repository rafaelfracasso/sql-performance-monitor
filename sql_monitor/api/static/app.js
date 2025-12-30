/**
 * SQL Monitor Dashboard - JavaScript
 */

// Utilit\u00e1rios gerais
const Utils = {
    /**
     * Formata n\u00famero com separadores de milhar
     */
    formatNumber(num) {
        return new Intl.NumberFormat('pt-BR').format(num);
    },

    /**
     * Formata data/hora
     */
    formatDateTime(dateString) {
        const date = new Date(dateString);
        return new Intl.DateTimeFormat('pt-BR', {
            dateStyle: 'short',
            timeStyle: 'short'
        }).format(date);
    },

    /**
     * Mostra mensagem de toast
     */
    showToast(message, type = 'info') {
        // Implementa\u00e7\u00e3o simples - pode ser melhorada com biblioteca de toast
        alert(message);
    },

    /**
     * Debounce para otimizar eventos
     */
    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
};

// API Client
const API = {
    baseURL: '/api',

    /**
     * Faz requisi\u00e7\u00e3o GET
     */
    async get(endpoint) {
        try {
            const response = await fetch(`${this.baseURL}${endpoint}`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error('API GET error:', error);
            throw error;
        }
    },

    /**
     * Faz requisi\u00e7\u00e3o POST
     */
    async post(endpoint, data) {
        try {
            const response = await fetch(`${this.baseURL}${endpoint}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error('API POST error:', error);
            throw error;
        }
    },

    /**
     * Faz requisi\u00e7\u00e3o DELETE
     */
    async delete(endpoint) {
        try {
            const response = await fetch(`${this.baseURL}${endpoint}`, {
                method: 'DELETE'
            });
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error('API DELETE error:', error);
            throw error;
        }
    }
};

// Dashboard Components
const Dashboard = {
    /**
     * Inicializa dashboard
     */
    init() {
        this.setupAutoRefresh();
        this.setupSearchFilters();
    },

    /**
     * Configura auto-refresh
     */
    setupAutoRefresh() {
        // Refresh a cada 5 minutos
        const autoRefreshInterval = 5 * 60 * 1000;

        setInterval(() => {
            console.log('Auto-refreshing dashboard...');
            location.reload();
        }, autoRefreshInterval);
    },

    /**
     * Configura filtros de busca
     */
    setupSearchFilters() {
        const searchInputs = document.querySelectorAll('[data-search]');

        searchInputs.forEach(input => {
            input.addEventListener('input', Utils.debounce((e) => {
                const searchTerm = e.target.value.toLowerCase();
                const targetTable = document.querySelector(e.target.dataset.search);

                if (targetTable) {
                    this.filterTable(targetTable, searchTerm);
                }
            }, 300));
        });
    },

    /**
     * Filtra linhas da tabela
     */
    filterTable(table, searchTerm) {
        const rows = table.querySelectorAll('tbody tr');

        rows.forEach(row => {
            const text = row.textContent.toLowerCase();
            row.style.display = text.includes(searchTerm) ? '' : 'none';
        });
    }
};

// Chart Helpers
const ChartHelpers = {
    /**
     * Cores padr\u00e3o para gr\u00e1ficos
     */
    colors: {
        primary: 'rgba(37, 99, 235, 0.8)',
        success: 'rgba(16, 185, 129, 0.8)',
        warning: 'rgba(245, 158, 11, 0.8)',
        error: 'rgba(239, 68, 68, 0.8)',
        secondary: 'rgba(100, 116, 139, 0.8)'
    },

    /**
     * Op\u00e7\u00f5es padr\u00e3o para Chart.js
     */
    defaultOptions: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: 'top',
            }
        }
    },

    /**
     * Cria gr\u00e1fico de linha
     */
    createLineChart(ctx, data, options = {}) {
        return new Chart(ctx, {
            type: 'line',
            data: data,
            options: {
                ...this.defaultOptions,
                ...options
            }
        });
    },

    /**
     * Cria gr\u00e1fico de barras
     */
    createBarChart(ctx, data, options = {}) {
        return new Chart(ctx, {
            type: 'bar',
            data: data,
            options: {
                ...this.defaultOptions,
                ...options
            }
        });
    },

    /**
     * Cria gr\u00e1fico de pizza
     */
    createDoughnutChart(ctx, data, options = {}) {
        return new Chart(ctx, {
            type: 'doughnut',
            data: data,
            options: {
                ...this.defaultOptions,
                ...options
            }
        });
    }
};

// Plans Management
const PlansManager = {
    /**
     * Veta plano completo
     */
    async vetoPlan(planId, reason, vetedBy) {
        try {
            const response = await API.post(`/plans/${planId}/veto`, {
                reason: reason,
                vetoed_by: vetedBy
            });

            Utils.showToast('Plano vetado com sucesso!', 'success');
            location.reload();
            return response;
        } catch (error) {
            Utils.showToast('Erro ao vetar plano: ' + error.message, 'error');
            throw error;
        }
    },

    /**
     * Aprova plano
     */
    async approvePlan(planId, approvedBy, executeNow = false) {
        try {
            const response = await API.post(`/plans/${planId}/approve`, {
                approved_by: approvedBy,
                execute_now: executeNow
            });

            Utils.showToast('Plano aprovado com sucesso!', 'success');
            location.reload();
            return response;
        } catch (error) {
            Utils.showToast('Erro ao aprovar plano: ' + error.message, 'error');
            throw error;
        }
    },

    /**
     * Veta item espec\u00edfico
     */
    async vetoItem(planId, itemId, reason, vetedBy) {
        try {
            const response = await API.post(`/plans/${planId}/items/${itemId}/veto`, {
                reason: reason,
                vetoed_by: vetedBy
            });

            Utils.showToast('Item vetado com sucesso!', 'success');
            location.reload();
            return response;
        } catch (error) {
            Utils.showToast('Erro ao vetar item: ' + error.message, 'error');
            throw error;
        }
    },

    /**
     * Remove veto de item
     */
    async unvetoItem(planId, itemId) {
        try {
            const response = await API.delete(`/plans/${planId}/items/${itemId}/veto`);

            Utils.showToast('Veto removido com sucesso!', 'success');
            location.reload();
            return response;
        } catch (error) {
            Utils.showToast('Erro ao remover veto: ' + error.message, 'error');
            throw error;
        }
    },

    /**
     * Obt\u00e9m status do plano
     */
    async getPlanStatus(planId) {
        try {
            return await API.get(`/plans/${planId}/status`);
        } catch (error) {
            console.error('Erro ao obter status do plano:', error);
            throw error;
        }
    }
};

// Monitoring
const Monitoring = {
    /**
     * Monitora sa\u00fade do sistema
     */
    async checkHealth() {
        try {
            const health = await API.get('/health');
            console.log('System health:', health);
            return health;
        } catch (error) {
            console.error('Health check failed:', error);
            return { status: 'unhealthy' };
        }
    },

    /**
     * Inicia monitoramento de sa\u00fade
     */
    startHealthMonitoring(intervalMs = 60000) {
        setInterval(() => {
            this.checkHealth().then(health => {
                if (health.status !== 'healthy') {
                    Utils.showToast('Sistema reportou problemas de sa\u00fade', 'warning');
                }
            });
        }, intervalMs);
    }
};

// Export Functions
const ExportUtils = {
    /**
     * Exporta tabela para CSV
     */
    tableToCSV(tableId, filename = 'export.csv') {
        const table = document.getElementById(tableId);
        if (!table) {
            console.error('Tabela n\u00e3o encontrada:', tableId);
            return;
        }

        const rows = Array.from(table.querySelectorAll('tr'));
        const csv = rows.map(row => {
            const cells = Array.from(row.querySelectorAll('th, td'));
            return cells.map(cell => {
                const text = cell.textContent.trim();
                // Escapa aspas e adiciona aspas se cont\u00e9m v\u00edrgula
                return text.includes(',') ? `"${text.replace(/"/g, '""')}"` : text;
            }).join(',');
        }).join('\n');

        this.downloadFile(csv, filename, 'text/csv');
    },

    /**
     * Exporta dados JSON para arquivo
     */
    jsonToFile(data, filename = 'export.json') {
        const json = JSON.stringify(data, null, 2);
        this.downloadFile(json, filename, 'application/json');
    },

    /**
     * Download de arquivo
     */
    downloadFile(content, filename, contentType) {
        const blob = new Blob([content], { type: contentType });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        link.click();
        URL.revokeObjectURL(url);
    }
};

// Inicializa\u00e7\u00e3o quando DOM estiver pronto
document.addEventListener('DOMContentLoaded', () => {
    console.log('SQL Monitor Dashboard initialized');
    Dashboard.init();
    Monitoring.startHealthMonitoring();
});

// Expor fun\u00e7\u00f5es globalmente para uso nos templates
window.SQLMonitor = {
    Utils,
    API,
    Dashboard,
    ChartHelpers,
    PlansManager,
    Monitoring,
    ExportUtils
};
