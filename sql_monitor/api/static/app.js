/**
 * SQL Monitor Dashboard - Application Logic
 * Standardized JS for consistent UI/UX across all dashboards.
 */

// === UTILS ===
const Utils = {
    /**
     * Formata numero com separadores de milhar
     */
    formatNumber(num) {
        if (num === undefined || num === null) return 'N/A';
        return new Intl.NumberFormat('pt-BR').format(num);
    },

    /**
     * Formata data/hora
     */
    formatDateTime(dateString) {
        if (!dateString) return 'N/A';
        const date = new Date(dateString);
        return new Intl.DateTimeFormat('pt-BR', {
            dateStyle: 'short',
            timeStyle: 'short'
        }).format(date);
    },

    /**
     * Debounce para otimizar eventos
     */
    debounce(func, wait) {
        let timeout;
        return function(...args) {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), wait);
        };
    },

    /**
     * Mostra mensagem de toast
     */
    showToast(message, type = 'info') {
        let container = document.getElementById('toast-container');
        if (!container) {
            container = document.createElement('div');
            container.id = 'toast-container';
            container.className = 'toast-container';
            document.body.appendChild(container);
        }

        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.innerHTML = `
            <div class="toast-icon">${this._toastIcon(type)}</div>
            <div class="toast-message">${message}</div>
            <button class="toast-close" onclick="this.parentElement.remove()">&times;</button>
            <div class="toast-progress"><div class="toast-progress-bar"></div></div>
        `;

        container.appendChild(toast);
        requestAnimationFrame(() => toast.classList.add('toast-visible'));

        setTimeout(() => {
            toast.classList.remove('toast-visible');
            toast.classList.add('toast-hiding');
            setTimeout(() => toast.remove(), 300);
        }, 5000);
    },

    _toastIcon(type) {
        const icons = {
            success: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
            error: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
            warning: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
            info: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>'
        };
        return icons[type] || icons.info;
    }
};

// === API CLIENT ===
const API = {
    baseURL: '/api',

    async request(endpoint, options = {}) {
        try {
            const response = await fetch(`${this.baseURL}${endpoint}`, options);
            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.detail || errorData.error || `HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error(`API Error (${endpoint}):`, error);
            throw error;
        }
    },

    get(endpoint) { return this.request(endpoint); },
    
    post(endpoint, data) {
        return this.request(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
    },

    delete(endpoint) {
        return this.request(endpoint, { method: 'DELETE' });
    }
};

// === THEME MANAGER ===
const ThemeManager = {
    toggleBtn: null,

    init() {
        this.toggleBtn = document.getElementById('theme-toggle');
        if (this.toggleBtn) {
            this.toggleBtn.addEventListener('click', () => this.toggleTheme());
        }
        this.applyTheme(localStorage.getItem('theme') || 'light');
    },

    toggleTheme() {
        const current = document.documentElement.getAttribute('data-theme');
        const next = current === 'dark' ? 'light' : 'dark';
        this.applyTheme(next);
    },

    applyTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
        this.updateButtonState();
        
        if (window.Chart) {
            Chart.defaults.color = theme === 'dark' ? '#cbd5e1' : '#64748b';
            Chart.defaults.borderColor = theme === 'dark' ? '#334155' : '#e2e8f0';
        }
    },

    updateButtonState() {
        const currentTheme = document.documentElement.getAttribute('data-theme');
        if (this.toggleBtn) {
            this.toggleBtn.setAttribute('aria-label', 
                currentTheme === 'dark' ? 'Mudar para tema claro' : 'Mudar para tema escuro'
            );
        }
    }
};

// === CHART FACTORY ===
const ChartFactory = {
    colors: {
        primary: 'rgb(37, 99, 235)',      // blue-600
        success: 'rgb(16, 185, 129)',     // emerald-500
        warning: 'rgb(245, 158, 11)',     // amber-500
        error: 'rgb(239, 68, 68)',        // red-500
        secondary: 'rgb(100, 116, 139)',  // slate-500
        purple: 'rgb(147, 51, 234)',      // purple-600
        pink: 'rgb(219, 39, 119)',        // pink-600
        orange: 'rgb(234, 88, 12)'        // orange-600
    },

    bgColors: {
        primary: 'rgba(37, 99, 235, 0.1)',
        success: 'rgba(16, 185, 129, 0.1)',
        warning: 'rgba(245, 158, 11, 0.1)',
        error: 'rgba(239, 68, 68, 0.1)',
        secondary: 'rgba(100, 116, 139, 0.1)'
    },

    defaults: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: 'top',
                labels: { usePointStyle: true, padding: 15 }
            },
            tooltip: {
                mode: 'index',
                intersect: false,
                padding: 10,
                cornerRadius: 8
            }
        },
        interaction: {
            mode: 'nearest',
            axis: 'x',
            intersect: false
        }
    },

    createLine(canvasId, config) {
        const ctx = document.getElementById(canvasId)?.getContext('2d');
        if (!ctx) return null;
        return new Chart(ctx, {
            type: 'line',
            data: config.data,
            options: this._mergeOptions(config.options)
        });
    },

    createBar(canvasId, config) {
        const ctx = document.getElementById(canvasId)?.getContext('2d');
        if (!ctx) return null;
        return new Chart(ctx, {
            type: 'bar',
            data: config.data,
            options: this._mergeOptions(config.options)
        });
    },

    createDoughnut(canvasId, config) {
        const ctx = document.getElementById(canvasId)?.getContext('2d');
        if (!ctx) return null;
        return new Chart(ctx, {
            type: 'doughnut',
            data: config.data,
            options: this._mergeOptions(config.options)
        });
    },

    _mergeOptions(customOptions) {
        return {
            ...this.defaults,
            ...customOptions,
            plugins: { ...this.defaults.plugins, ...(customOptions?.plugins || {}) },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { borderDash: [2, 4] },
                    ...(customOptions?.scales?.y || {})
                },
                x: {
                    grid: { display: false },
                    ...(customOptions?.scales?.x || {})
                },
                ...(customOptions?.scales || {})
            }
        };
    }
};

// === ALERTS MODULE ===
const Alerts = {
    init() {
        document.addEventListener('click', (e) => {
            const alertItem = e.target.closest('.clickable-alert');
            if (alertItem && alertItem.dataset.id) {
                this.openDetails(alertItem.dataset.id);
            }
        });
    },

    openDetails(alertId) {
        if (alertId) {
            window.location.href = `/dashboard/alerts/${alertId}`;
        }
    }
};

// === DASHBOARD (Legacy restored) ===
const Dashboard = {
    init() {
        this.setupAutoRefresh();
        this.setupSearchFilters();
    },

    setupAutoRefresh() {
        const autoRefreshInterval = 5 * 60 * 1000;
        setInterval(() => {
            console.log('Auto-refreshing dashboard...');
            location.reload();
        }, autoRefreshInterval);
    },

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

    filterTable(table, searchTerm) {
        const rows = table.querySelectorAll('tbody tr');
        rows.forEach(row => {
            const text = row.textContent.toLowerCase();
            row.style.display = text.includes(searchTerm) ? '' : 'none';
        });
    }
};

// === PLANS MANAGER (Legacy restored) ===
const PlansManager = {
    async vetoPlan(planId, reason, vetedBy) {
        try {
            const response = await API.post(`/plans/${planId}/veto`, { reason, vetoed_by: vetedBy });
            Utils.showToast('Plano vetado com sucesso!', 'success');
            location.reload();
            return response;
        } catch (error) {
            Utils.showToast('Erro ao vetar plano: ' + error.message, 'error');
            throw error;
        }
    },

    async approvePlan(planId, approvedBy, executeNow = false) {
        try {
            const response = await API.post(`/plans/${planId}/approve`, { approved_by: approvedBy, execute_now: executeNow });
            Utils.showToast('Plano aprovado com sucesso!', 'success');
            location.reload();
            return response;
        } catch (error) {
            Utils.showToast('Erro ao aprovar plano: ' + error.message, 'error');
            throw error;
        }
    },

    async vetoItem(planId, itemId, reason, vetedBy) {
        try {
            const response = await API.post(`/plans/${planId}/items/${itemId}/veto`, { reason, vetoed_by: vetedBy });
            Utils.showToast('Item vetado com sucesso!', 'success');
            location.reload();
            return response;
        } catch (error) {
            Utils.showToast('Erro ao vetar item: ' + error.message, 'error');
            throw error;
        }
    },

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

    async getPlanStatus(planId) {
        try {
            return await API.get(`/plans/${planId}/status`);
        } catch (error) {
            console.error('Erro ao obter status do plano:', error);
            throw error;
        }
    }
};

// === MONITORING (Legacy restored) ===
const Monitoring = {
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

    startHealthMonitoring(intervalMs = 60000) {
        setInterval(() => {
            this.checkHealth().then(health => {
                if (health.status !== 'healthy') {
                    Utils.showToast('Sistema reportou problemas de saúde', 'warning');
                }
            });
        }, intervalMs);
    }
};

// === QUERY MODAL ===
const QueryModal = {
    init() {
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.close();
        });
    },

    async open(queryHash) {
        const modal = document.getElementById('queryModal');
        const modalQueryText = document.getElementById('modalQueryText');
        const modalQueryHash = document.getElementById('modalQueryHash');

        if (!modal) return;

        modalQueryText.innerHTML = '<code>Carregando...</code>';
        modalQueryHash.textContent = `Hash: ${queryHash}`;
        modal.style.display = 'flex';
        modal.dataset.queryText = '';

        try {
            const data = await API.get(`/queries/${queryHash}/full-text`);
            const textToDisplay = data.formatted_query || data.query_text;
            modalQueryText.textContent = textToDisplay;
            modalQueryText.className = 'language-sql';
            if (window.hljs) hljs.highlightElement(modalQueryText);
            modal.dataset.queryText = data.query_text;
        } catch (error) {
            modalQueryText.innerHTML = `<code style="color: var(--error-color);">Erro: ${error.message}</code>`;
        }
    },

    close() {
        const modal = document.getElementById('queryModal');
        if (modal) modal.style.display = 'none';
    },

    async copyToClipboard() {
        const modal = document.getElementById('queryModal');
        const queryText = modal?.dataset.queryText || modal?.querySelector('code')?.textContent;
        if (!queryText) return;
        try {
            await navigator.clipboard.writeText(queryText);
            const btn = document.querySelector('.btn-copy');
            const originalHTML = btn.innerHTML;
            btn.innerHTML = '✓ Copiado!';
            btn.style.backgroundColor = 'var(--success-color)';
            setTimeout(() => {
                btn.innerHTML = originalHTML;
                btn.style.backgroundColor = '';
            }, 2000);
        } catch (err) {
            console.error('Copy failed:', err);
            Utils.showToast('Erro ao copiar', 'error');
        }
    }
};

// === MARKDOWN RENDERER (Legacy restored) ===
const MarkdownRenderer = {
    init() {
        if (typeof marked !== 'undefined') {
            marked.setOptions({
                highlight: function(code, lang) {
                    if (lang && hljs.getLanguage(lang)) {
                        try { return hljs.highlight(code, { language: lang }).value; } 
                        catch (e) { console.error('Highlight error:', e); }
                    }
                    try { return hljs.highlightAuto(code).value; } catch (e) { return code; }
                },
                breaks: true,
                gfm: true
            });
        }
        this.renderAll();
    },

    render(element) {
        if (!element || typeof marked === 'undefined') return;
        const rawContent = element.textContent || element.innerText;
        if (!rawContent.trim()) return;

        try {
            const htmlContent = marked.parse(rawContent);
            element.innerHTML = htmlContent;
            element.classList.add('markdown-rendered');
            element.querySelectorAll('pre code').forEach((block) => {
                if (!block.classList.contains('hljs')) hljs.highlightElement(block);
            });
        } catch (e) {
            console.error('Markdown render error:', e);
        }
    },

    renderAll() {
        document.querySelectorAll('.markdown-content').forEach(el => this.render(el));
    }
};

// === PLANS UI (New) ===
const PlansUI = {
    // Legacy/Removed features kept as stubs if needed, or removed.
    // approvePlan(planId) { ... }
    // vetoPlan(planId) { ... }

    async deletePlan(planId) {
        if (!confirm(`Tem certeza que deseja deletar o relatorio ${planId}?`)) return;

        const btn = event?.target?.closest?.('button');
        const originalText = btn?.innerHTML;
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = 'Removendo...';
        }

        try {
            await API.delete(`/plans/${planId}`);
            Utils.showToast('Relatorio removido com sucesso!', 'success');
            const card = btn?.closest('.plan-card');
            if (card) {
                card.style.transition = 'opacity 0.3s, transform 0.3s';
                card.style.opacity = '0';
                card.style.transform = 'scale(0.95)';
                setTimeout(() => { card.remove(); }, 300);
            } else {
                setTimeout(() => location.reload(), 800);
            }
        } catch (error) {
            Utils.showToast('Erro ao deletar relatorio: ' + error.message, 'error');
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = originalText;
            }
        }
    },

    async generatePlan() {
        if (!confirm('Deseja gerar uma nova analise agora? Isso pode levar alguns segundos.')) return;

        const btn = document.querySelector('[data-action="generate-plan"]') || document.querySelector('button[onclick="generatePlan()"]');
        const originalText = btn?.innerHTML;
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-sm"></span> Gerando...';
        }

        try {
            Utils.showToast('Gerando analise, aguarde...', 'info');
            const response = await API.post('/plans/generate', {});
            Utils.showToast('Analise gerada com sucesso!', 'success');
            setTimeout(() => window.location.href = `/plans/${response.plan_id}`, 1000);
        } catch (error) {
            console.error('Erro ao gerar plano:', error);
            Utils.showToast('Erro ao gerar analise: ' + error.message, 'error');
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = originalText;
            }
        }
    }
};

// === QUERIES UI (New) ===
const QueriesUI = {
    toggleAdvancedFilters() {
        const panel = document.getElementById('advancedFilters');
        if (panel) panel.classList.toggle('show');
    },

};

// Extend Dashboard with new methods
Object.assign(Dashboard, {
    loadCacheEfficiency(hours = 24) {
        API.get(`/dashboard/cache-efficiency?hours=${hours}`)
            .then(data => {
                const canvas = document.getElementById('cacheEfficiencyChart');
                if (!canvas) return;
                
                ChartFactory.createDoughnut('cacheEfficiencyChart', {
                    data: {
                        labels: ['Cache Hits', 'Novas Análises'],
                        datasets: [{
                            data: [data.cache_hits, data.new_analyses],
                            backgroundColor: [
                                ChartFactory.colors.success,
                                ChartFactory.colors.primary
                            ]
                        }]
                    },
                    options: {
                        plugins: {
                            title: {
                                display: true,
                                text: 'Cache Hit Rate: ' + data.cache_hit_rate_percent.toFixed(1) + '%'
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Erro ao carregar cache efficiency:', error));
    },

    loadCyclesHistory(days = 7) {
        API.get(`/dashboard/trends?days=${days}&granularity=day`)
            .then(data => {
                const canvas = document.getElementById('cyclesHistoryChart');
                if (!canvas) return;
                
                const ctx = canvas.getContext('2d');
                new Chart(ctx, {
                    type: 'line',
                    data: data,
                    options: {
                        responsive: true,
                        interaction: { mode: 'index', intersect: false },
                        plugins: {
                            legend: { position: 'top' },
                            title: { display: true, text: `Evolução de Performance nos Últimos ${days} Dias` }
                        },
                        scales: {
                            y: {
                                type: 'linear',
                                display: true,
                                position: 'left',
                                title: { display: true, text: 'Tempo (ms)' }
                            },
                            y1: {
                                type: 'linear',
                                display: true,
                                position: 'right',
                                title: { display: true, text: 'Quantidade' },
                                grid: { drawOnChartArea: false }
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Erro ao carregar histórico de ciclos:', error));
    },

    exportData(tableId, filename) {
        const table = document.getElementById(tableId);
        if (!table) return;
        
        let csv = [];
        const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent);
        csv.push(headers.join(','));

        const rows = Array.from(table.querySelectorAll('tbody tr'));
        rows.forEach(row => {
            const cells = Array.from(row.querySelectorAll('td')).map(td => td.textContent.trim());
            csv.push(cells.join(','));
        });

        const csvContent = csv.join('\n');
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = filename;
        link.click();
    },
    
    renderExecutionChart(executions) {
        const canvas = document.getElementById('executionChart');
        if (!canvas || !executions || executions.length === 0) return;

        const ctx = canvas.getContext('2d');
        new Chart(ctx, {
            type: 'line',
            data: {
                labels: executions.map(e => e.timestamp),
                datasets: [
                    {
                        label: 'CPU Time (ms)',
                        data: executions.map(e => e.cpu_time),
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        tension: 0.3,
                        fill: true
                    },
                    {
                        label: 'Duration (ms)',
                        data: executions.map(e => e.duration),
                        borderColor: 'rgb(54, 162, 235)',
                        backgroundColor: 'rgba(54, 162, 235, 0.1)',
                        tension: 0.3,
                        fill: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' },
                    title: { display: true, text: 'Evolução das Métricas ao Longo do Tempo' }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: { display: true, text: 'Tempo (ms)' }
                    }
                }
            }
        });
    }
});

// === GLOBAL EXPOSURE (Immediate) ===
window.SQLMonitor = {
    Utils,
    API,
    ChartFactory,
    Alerts,
    QueryModal,
    Dashboard,
    PlansManager,
    Monitoring,
    ThemeManager,
    PlansUI,
    QueriesUI
};

// Legacy support
window.openQueryModal = (hash) => QueryModal.open(hash);
window.closeQueryModal = () => QueryModal.close();
window.copyQueryToClipboard = () => QueryModal.copyToClipboard();
window.generatePlan = PlansUI.generatePlan;
window.deletePlan = PlansUI.deletePlan;
window.viewAlertDetails = Alerts.openDetails;
window.viewHotspotAlerts = (instance, database, table) => {
    const params = new URLSearchParams(window.location.search);
    params.set('instance', instance);
    params.set('database', database);
    if (table && table !== 'None' && table !== 'null') {
        params.set('table', table);
    } else {
        params.delete('table');
    }
    window.location.href = `/dashboard/alerts?${params.toString()}`;
};
window.viewQueryDetails = (queryHash) => window.location.href = `/dashboard/queries/${queryHash}`;
window.toggleAdvancedFilters = QueriesUI.toggleAdvancedFilters;
window.exportData = (tableId, filename) => Dashboard.exportData(tableId, filename); // Generic export wrapper

// === INITIALIZATION ===
document.addEventListener('DOMContentLoaded', () => {
    console.log('SQL Monitor Dashboard initialized');
    ThemeManager.init();
    Dashboard.init();
    Monitoring.startHealthMonitoring();
    Alerts.init();
    QueryModal.init();
    MarkdownRenderer.init();
});