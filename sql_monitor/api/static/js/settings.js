// Settings Module - Namespace encapsulado
const SettingsManager = {
    currentDbType: '',
    promptTypes: ['base_template', 'task_instructions', 'features', 'index_syntax'],
    promptTypeMap: {
        'base_template': 'base',
        'task_instructions': 'task',
        'features': 'features',
        'index_syntax': 'index'
    },
    promptFormConfig: [
        { shortType: 'base', fullType: 'base_template', label: 'Base Template', rows: 15,
          description: 'Template base usado para montar o prompt. Variaveis: {db_name}, {query}, {ddl}, {indexes}, {metrics}' },
        { shortType: 'task', fullType: 'task_instructions', label: 'Task Instructions', rows: 15,
          description: 'Instrucoes especificas da tarefa de analise' },
        { shortType: 'features', fullType: 'features', label: 'Features Especificas', rows: 10,
          description: 'Features e caracteristicas especificas do banco' },
        { shortType: 'index', fullType: 'index_syntax', label: 'Index Syntax', rows: 8,
          description: 'Sintaxe de criacao de indices para o banco' }
    ],

    // Helper: show toast
    toast(message, type) {
        if (window.SQLMonitor && SQLMonitor.Utils) {
            SQLMonitor.Utils.showToast(message, type);
        }
    },

    // Helper: set button loading state
    setBtnLoading(btn, loading, originalText) {
        if (!btn) return;
        if (loading) {
            btn._originalText = btn.innerHTML;
            btn.innerHTML = 'Salvando...';
            btn.classList.add('btn-loading');
            btn.disabled = true;
        } else {
            btn.innerHTML = btn._originalText || originalText || 'Salvar';
            btn.classList.remove('btn-loading');
            btn.disabled = false;
        }
    },

    // Helper: escape HTML
    escapeHtml(str) {
        if (str === null || str === undefined) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    },

    // Tab switching
    initTabs() {
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const tabName = btn.dataset.tab;
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                document.getElementById(tabName).classList.add('active');
            });
        });
    },

    // Accordion toggle
    toggleAccordion(id) {
        const content = document.getElementById(id);
        const icon = content.previousElementSibling.querySelector('.accordion-icon');
        const isOpen = content.classList.contains('open');
        if (isOpen) {
            content.classList.remove('open');
            icon.style.transform = 'rotate(0deg)';
        } else {
            content.classList.add('open');
            icon.style.transform = 'rotate(180deg)';
        }
    },

    // ====== LOADERS ======

    async loadThresholds() {
        try {
            const response = await fetch('/api/settings/thresholds');
            const data = await response.json();
            data.forEach(item => {
                const form = document.getElementById(`form-thresholds-${item.db_type}`);
                if (form) {
                    Object.keys(item).forEach(key => {
                        const input = form.querySelector(`[name="${key}"]`);
                        if (input && key !== 'db_type') input.value = item[key];
                    });
                }
            });
        } catch (error) {
            console.error('Erro ao carregar thresholds:', error);
        }
    },

    async loadCollectionSettings() {
        try {
            const response = await fetch('/api/settings/collection');
            const data = await response.json();
            const tbody = document.getElementById('collection-settings-table');
            tbody.innerHTML = data.map(item => `
                <tr data-dbtype="${item.db_type}">
                    <td><strong>${item.db_type.toUpperCase()}</strong></td>
                    <td>${item.min_duration_seconds}s</td>
                    <td>${item.collect_active_queries ? '&#10003;' : '&#10007;'}</td>
                    <td>${item.collect_expensive_queries ? '&#10003;' : '&#10007;'}</td>
                    <td>${item.collect_table_scans ? '&#10003;' : '&#10007;'}</td>
                    <td>${item.max_queries_per_cycle}</td>
                    <td><button class="btn btn-sm" onclick="SettingsManager.editCollection('${item.db_type}')">Editar</button></td>
                </tr>
            `).join('');
        } catch (error) {
            console.error('Erro ao carregar collection settings:', error);
        }
    },

    async loadCacheConfig() {
        try {
            const response = await fetch('/api/settings/cache');
            const data = await response.json();
            const form = document.getElementById('form-cache-config');
            form.querySelector('[name="enabled"]').checked = data.enabled;
            form.querySelector('[name="ttl_hours"]').value = data.ttl_hours;
            form.querySelector('[name="max_entries"]').value = data.max_entries;
            form.querySelector('[name="cache_ddl"]').checked = data.cache_ddl;
            form.querySelector('[name="cache_indexes"]').checked = data.cache_indexes;
        } catch (error) {
            console.error('Erro ao carregar cache config:', error);
        }
    },

    async loadAuditLog() {
        try {
            const response = await fetch('/api/settings/audit');
            const data = await response.json();
            const tbody = document.getElementById('audit-log-table');
            tbody.innerHTML = data.map(item => `
                <tr>
                    <td>${new Date(item.changed_at).toLocaleString('pt-BR')}</td>
                    <td>${item.changed_by}</td>
                    <td>${item.config_table}</td>
                    <td>${item.config_key}</td>
                    <td><small>${item.new_value ? item.new_value.substring(0, 50) + '...' : 'N/A'}</small></td>
                </tr>
            `).join('');
        } catch (error) {
            console.error('Erro ao carregar audit log:', error);
        }
    },

    async loadLLMConfig() {
        try {
            const response = await fetch('/api/settings/llm');
            const data = await response.json();
            const form = document.getElementById('form-llm-config');
            if (form && data) {
                Object.keys(data).forEach(key => {
                    const input = form.querySelector(`[name="${key}"]`);
                    if (input) {
                        input.value = (key === 'retry_delays' && Array.isArray(data[key]))
                            ? JSON.stringify(data[key]) : data[key];
                    }
                });
            }
        } catch (error) {
            console.error('Erro ao carregar LLM config:', error);
        }
    },

    async loadMonitorConfig() {
        try {
            const response = await fetch('/api/settings/monitor');
            const data = await response.json();
            const form = document.getElementById('form-monitor-config');
            if (form && data) form.querySelector('[name="interval_seconds"]').value = data.interval_seconds;
        } catch (error) {
            console.error('Erro ao carregar Monitor config:', error);
        }
    },

    async loadTeamsConfig() {
        try {
            const response = await fetch('/api/settings/teams');
            const data = await response.json();
            const form = document.getElementById('form-teams-config');
            if (form && data) {
                form.querySelector('[name="enabled"]').checked = data.enabled;
                form.querySelector('[name="webhook_url"]').value = data.webhook_url || '';
                form.querySelector('[name="notify_on_cache_hit"]').checked = data.notify_on_cache_hit;
                form.querySelector('[name="priority_filter"]').value = JSON.stringify(data.priority_filter || []);
                form.querySelector('[name="timeout"]').value = data.timeout;
            }
        } catch (error) {
            console.error('Erro ao carregar Teams config:', error);
        }
    },

    async loadTimeoutsConfig() {
        try {
            const response = await fetch('/api/settings/timeouts');
            const data = await response.json();
            const form = document.getElementById('form-timeouts-config');
            if (form && data) {
                Object.keys(data).forEach(key => {
                    const input = form.querySelector(`[name="${key}"]`);
                    if (input) input.value = data[key];
                });
            }
        } catch (error) {
            console.error('Erro ao carregar Timeouts config:', error);
        }
    },

    async loadSecurityConfig() {
        try {
            const response = await fetch('/api/settings/security');
            const data = await response.json();
            const form = document.getElementById('form-security-config');
            if (form && data) {
                form.querySelector('[name="sanitize_queries"]').checked = data.sanitize_queries;
                form.querySelector('[name="placeholder_prefix"]').value = data.placeholder_prefix || '@p';
                form.querySelector('[name="show_example_values"]').checked = data.show_example_values;
            }
        } catch (error) {
            console.error('Erro ao carregar Security config:', error);
        }
    },

    async loadQueryCacheConfig() {
        try {
            const response = await fetch('/api/settings/query_cache');
            const data = await response.json();
            const form = document.getElementById('form-query-cache-config');
            if (form && data) {
                form.querySelector('[name="enabled"]').checked = data.enabled;
                form.querySelector('[name="ttl_hours"]').value = data.ttl_hours;
                form.querySelector('[name="cache_file"]').value = data.cache_file || 'logs/query_cache.json';
                form.querySelector('[name="auto_save_interval"]').value = data.auto_save_interval;
            }
        } catch (error) {
            console.error('Erro ao carregar Query Cache config:', error);
        }
    },

    async loadLoggingConfig() {
        try {
            const response = await fetch('/api/settings/logging');
            const data = await response.json();
            const form = document.getElementById('form-logging-config');
            if (form && data) {
                form.querySelector('[name="level"]').value = data.level || 'INFO';
                form.querySelector('[name="format"]').value = data.format || 'colored';
                form.querySelector('[name="log_file"]').value = data.log_file || 'logs/monitor.log';
                form.querySelector('[name="enable_console"]').checked = data.enable_console !== false;
            }
        } catch (error) {
            console.error('Erro ao carregar Logging config:', error);
        }
    },

    async loadStorageConfig() {
        try {
            const response = await fetch('/api/settings/metrics_store');
            const data = await response.json();
            const form = document.getElementById('form-storage-config');
            if (form && data) {
                form.querySelector('[name="db_path"]').value = data.db_path || 'logs/metrics.duckdb';
                form.querySelector('[name="enable_compression"]').checked = data.enable_compression !== false;
                form.querySelector('[name="retention_days"]').value = data.retention_days || 30;
            }
        } catch (error) {
            console.error('Erro ao carregar Storage config:', error);
        }
    },

    async loadWeeklyOptimizerConfig() {
        try {
            const response = await fetch('/api/settings/weekly_optimizer');
            const data = await response.json();
            const form = document.getElementById('form-weekly-optimizer-config');
            if (form && data) {
                form.querySelector('[name="enabled"]').checked = data.enabled || false;
                if (data.schedule) {
                    form.querySelector('[name="analysis_day"]').value = data.schedule.analysis_day || 'thursday';
                    form.querySelector('[name="analysis_time"]').value = data.schedule.analysis_time || '18:00';
                    form.querySelector('[name="execution_day"]').value = data.schedule.execution_day || 'sunday';
                    form.querySelector('[name="execution_time"]').value = data.schedule.execution_time || '02:00';
                    form.querySelector('[name="report_day"]').value = data.schedule.report_day || 'monday';
                    form.querySelector('[name="report_time"]').value = data.schedule.report_time || '08:00';
                }
                if (data.veto_window) {
                    form.querySelector('[name="veto_window_hours"]').value = data.veto_window.hours || 72;
                    form.querySelector('[name="check_before_execution"]').checked = data.veto_window.check_before_execution !== false;
                }
                if (data.risk_thresholds) {
                    form.querySelector('[name="table_size_gb_medium"]').value = data.risk_thresholds.table_size_gb_medium || 100;
                    form.querySelector('[name="table_size_gb_high"]').value = data.risk_thresholds.table_size_gb_high || 500;
                    form.querySelector('[name="table_size_gb_critical"]').value = data.risk_thresholds.table_size_gb_critical || 1000;
                    form.querySelector('[name="index_fragmentation_percent"]').value = data.risk_thresholds.index_fragmentation_percent || 50;
                    form.querySelector('[name="max_execution_time_minutes"]').value = data.risk_thresholds.max_execution_time_minutes || 240;
                }
                if (data.auto_rollback) {
                    form.querySelector('[name="auto_rollback_enabled"]').checked = data.auto_rollback.enabled !== false;
                    form.querySelector('[name="degradation_threshold_percent"]').value = data.auto_rollback.degradation_threshold_percent || 20;
                    form.querySelector('[name="wait_after_execution_minutes"]').value = data.auto_rollback.wait_after_execution_minutes || 10;
                }
                if (data.api) {
                    form.querySelector('[name="api_enabled"]').checked = data.api.enabled !== false;
                    form.querySelector('[name="api_host"]').value = data.api.host || '0.0.0.0';
                    form.querySelector('[name="api_port"]').value = data.api.port || 8080;
                    form.querySelector('[name="cors_enabled"]').checked = data.api.cors_enabled !== false;
                }
                if (data.analysis) {
                    form.querySelector('[name="analysis_days"]').value = data.analysis.days || 7;
                    form.querySelector('[name="min_occurrences"]').value = data.analysis.min_occurrences || 10;
                    form.querySelector('[name="min_avg_duration_ms"]').value = data.analysis.min_avg_duration_ms || 1000;
                }
            }
        } catch (error) {
            console.error('Erro ao carregar Weekly Optimizer config:', error);
        }
    },

    // ====== SAVERS ======

    initThresholdForms() {
        ['hana', 'sqlserver', 'postgresql'].forEach(dbType => {
            const form = document.getElementById(`form-thresholds-${dbType}`);
            if (!form) return;
            form.addEventListener('submit', async (e) => {
                e.preventDefault();
                const btn = form.querySelector('button[type="submit"]');
                this.setBtnLoading(btn, true);
                const formData = new FormData(e.target);
                const data = Object.fromEntries(formData);
                Object.keys(data).forEach(key => { data[key] = parseFloat(data[key]); });

                try {
                    const response = await fetch(`/api/settings/thresholds/${dbType}`, {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(data)
                    });
                    if (response.ok) {
                        this.toast(`Thresholds do ${dbType.toUpperCase()} salvos com sucesso!`, 'success');
                        this.loadThresholds();
                    } else {
                        this.toast('Erro ao salvar thresholds', 'error');
                    }
                } catch (error) {
                    console.error('Erro:', error);
                    this.toast('Erro ao salvar thresholds', 'error');
                } finally {
                    this.setBtnLoading(btn, false);
                }
            });
        });
    },

    initCacheForm() {
        document.getElementById('form-cache-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const formData = new FormData(e.target);
            const data = {
                enabled: formData.get('enabled') === 'on',
                ttl_hours: parseInt(formData.get('ttl_hours')),
                max_entries: parseInt(formData.get('max_entries')),
                cache_ddl: formData.get('cache_ddl') === 'on',
                cache_indexes: formData.get('cache_indexes') === 'on'
            };
            try {
                const response = await fetch('/api/settings/cache', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Configuracao de cache salva com sucesso!', 'success');
                    this.loadCacheConfig();
                } else {
                    this.toast('Erro ao salvar configuracao', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar configuracao', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    initLLMForm() {
        document.getElementById('form-llm-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const formData = new FormData(e.target);
            const data = Object.fromEntries(formData);
            try { data.retry_delays = JSON.parse(data.retry_delays); } catch { data.retry_delays = [3, 8, 15]; }
            ['temperature', 'max_tokens', 'max_retries', 'max_requests_per_day',
             'max_requests_per_minute', 'max_requests_per_cycle', 'min_delay_between_requests'].forEach(key => {
                if (data[key]) data[key] = parseFloat(data[key]);
            });
            try {
                const response = await fetch('/api/settings/llm', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Configuracao de LLM salva com sucesso!', 'success');
                    this.loadLLMConfig();
                } else {
                    this.toast('Erro ao salvar configuracao de LLM', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar configuracao de LLM', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    initMonitorForm() {
        document.getElementById('form-monitor-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const data = { interval_seconds: parseInt(new FormData(e.target).get('interval_seconds')) };
            try {
                const response = await fetch('/api/settings/monitor', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Configuracao de Monitor salva! Mudancas serao aplicadas no proximo ciclo.', 'success');
                    this.loadMonitorConfig();
                } else {
                    this.toast('Erro ao salvar configuracao de Monitor', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar configuracao de Monitor', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    initTeamsForm() {
        document.getElementById('form-teams-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const formData = new FormData(e.target);
            let priority_filter = [];
            try { priority_filter = JSON.parse(formData.get('priority_filter')); } catch {}
            const data = {
                enabled: formData.get('enabled') === 'on',
                webhook_url: formData.get('webhook_url'),
                notify_on_cache_hit: formData.get('notify_on_cache_hit') === 'on',
                priority_filter, timeout: parseInt(formData.get('timeout'))
            };
            try {
                const response = await fetch('/api/settings/teams', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Configuracao de Teams salva com sucesso!', 'success');
                    this.loadTeamsConfig();
                } else {
                    this.toast('Erro ao salvar configuracao de Teams', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar configuracao de Teams', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    initTimeoutsForm() {
        document.getElementById('form-timeouts-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const formData = new FormData(e.target);
            const data = {};
            ['database_connect', 'database_query', 'llm_analysis', 'thread_shutdown', 'circuit_breaker_recovery'].forEach(key => {
                data[key] = parseInt(formData.get(key));
            });
            try {
                const response = await fetch('/api/settings/timeouts', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Timeouts salvos com sucesso!', 'success');
                    this.loadTimeoutsConfig();
                } else {
                    this.toast('Erro ao salvar timeouts', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar timeouts', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    initSecurityForm() {
        document.getElementById('form-security-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const formData = new FormData(e.target);
            const data = {
                sanitize_queries: formData.get('sanitize_queries') === 'on',
                placeholder_prefix: formData.get('placeholder_prefix'),
                show_example_values: formData.get('show_example_values') === 'on'
            };
            try {
                const response = await fetch('/api/settings/security', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Configuracao de Seguranca salva!', 'success');
                    this.loadSecurityConfig();
                } else {
                    this.toast('Erro ao salvar configuracao de Seguranca', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar configuracao de Seguranca', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    initQueryCacheForm() {
        document.getElementById('form-query-cache-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const formData = new FormData(e.target);
            const data = {
                enabled: formData.get('enabled') === 'on',
                ttl_hours: parseInt(formData.get('ttl_hours')),
                cache_file: formData.get('cache_file'),
                auto_save_interval: parseInt(formData.get('auto_save_interval'))
            };
            try {
                const response = await fetch('/api/settings/query_cache', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Configuracao de Query Cache salva!', 'success');
                    this.loadQueryCacheConfig();
                } else {
                    this.toast('Erro ao salvar configuracao de Query Cache', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar configuracao de Query Cache', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    initLoggingForm() {
        document.getElementById('form-logging-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const formData = new FormData(e.target);
            const data = {
                level: formData.get('level'), format: formData.get('format'),
                log_file: formData.get('log_file'), enable_console: formData.get('enable_console') === 'on'
            };
            try {
                const response = await fetch('/api/settings/logging', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Configuracao de Logging salva! Mudancas serao aplicadas no proximo restart.', 'success');
                    this.loadLoggingConfig();
                } else {
                    this.toast('Erro ao salvar configuracao de Logging', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar configuracao de Logging', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    initStorageForm() {
        document.getElementById('form-storage-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const formData = new FormData(e.target);
            const data = {
                db_path: formData.get('db_path'),
                enable_compression: formData.get('enable_compression') === 'on',
                retention_days: parseInt(formData.get('retention_days'))
            };
            try {
                const response = await fetch('/api/settings/metrics_store', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Configuracao de Armazenamento salva! Mudancas no caminho requerem restart.', 'success');
                    this.loadStorageConfig();
                } else {
                    this.toast('Erro ao salvar configuracao de Armazenamento', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar configuracao de Armazenamento', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    initWeeklyOptimizerForm() {
        document.getElementById('form-weekly-optimizer-config')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            this.setBtnLoading(btn, true);
            const formData = new FormData(e.target);
            const data = {
                enabled: formData.get('enabled') === 'on',
                schedule: {
                    analysis_day: formData.get('analysis_day'), analysis_time: formData.get('analysis_time'),
                    execution_day: formData.get('execution_day'), execution_time: formData.get('execution_time'),
                    report_day: formData.get('report_day'), report_time: formData.get('report_time')
                },
                veto_window: {
                    hours: parseInt(formData.get('veto_window_hours')),
                    check_before_execution: formData.get('check_before_execution') === 'on'
                },
                risk_thresholds: {
                    table_size_gb_medium: parseInt(formData.get('table_size_gb_medium')),
                    table_size_gb_high: parseInt(formData.get('table_size_gb_high')),
                    table_size_gb_critical: parseInt(formData.get('table_size_gb_critical')),
                    index_fragmentation_percent: parseInt(formData.get('index_fragmentation_percent')),
                    max_execution_time_minutes: parseInt(formData.get('max_execution_time_minutes'))
                },
                auto_rollback: {
                    enabled: formData.get('auto_rollback_enabled') === 'on',
                    degradation_threshold_percent: parseInt(formData.get('degradation_threshold_percent')),
                    wait_after_execution_minutes: parseInt(formData.get('wait_after_execution_minutes'))
                },
                api: {
                    enabled: formData.get('api_enabled') === 'on',
                    host: formData.get('api_host'), port: parseInt(formData.get('api_port')),
                    cors_enabled: formData.get('cors_enabled') === 'on'
                },
                analysis: {
                    days: parseInt(formData.get('analysis_days')),
                    min_occurrences: parseInt(formData.get('min_occurrences')),
                    min_avg_duration_ms: parseInt(formData.get('min_avg_duration_ms'))
                }
            };
            try {
                const response = await fetch('/api/settings/weekly_optimizer', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                if (response.ok) {
                    this.toast('Configuracao de Weekly Optimizer salva com sucesso!', 'success');
                    this.loadWeeklyOptimizerConfig();
                } else {
                    this.toast('Erro ao salvar configuracao de Weekly Optimizer', 'error');
                }
            } catch (error) {
                console.error('Erro:', error);
                this.toast('Erro ao salvar configuracao de Weekly Optimizer', 'error');
            } finally {
                this.setBtnLoading(btn, false);
            }
        });
    },

    // ====== ACTIONS ======

    async resetDefaults(dbType) {
        if (!confirm(`Resetar configuracoes do ${dbType.toUpperCase()} para os padroes?`)) return;
        try {
            const response = await fetch(`/api/settings/reset/${dbType}`, { method: 'POST' });
            if (response.ok) {
                this.toast('Configuracoes resetadas!', 'success');
                this.loadThresholds();
            }
        } catch (error) {
            console.error('Erro:', error);
            this.toast('Erro ao resetar configuracoes', 'error');
        }
    },

    async clearCache() {
        if (!confirm('Limpar todo o cache de metadados?')) return;
        try {
            const response = await fetch('/api/settings/cache/clear', { method: 'POST' });
            if (response.ok) {
                this.toast('Cache limpo com sucesso!', 'success');
            }
        } catch (error) {
            console.error('Erro:', error);
            this.toast('Erro ao limpar cache', 'error');
        }
    },

    testTeamsWebhook() {
        this.toast('Teste de webhook nao implementado ainda. Configure via Power Automate.', 'warning');
    },

    // ====== COLLECTION EDIT ======

    async editCollection(dbType) {
        try {
            const response = await fetch('/api/settings/collection');
            const allSettings = await response.json();
            const settings = allSettings.find(s => s.db_type === dbType);
            if (!settings) {
                this.toast(`Configuracoes nao encontradas para ${dbType}`, 'error');
                return;
            }
            const formHtml = `
                <tr id="edit-row-${dbType}" class="edit-row">
                    <td colspan="7">
                        <form id="form-edit-collection-${dbType}" style="padding: 1rem;">
                            <h4 style="margin-top:0">Editar Coleta: ${dbType.toUpperCase()}</h4>
                            <div class="form-grid">
                                <div class="form-group">
                                    <label>Duracao Minima (s)</label>
                                    <input type="number" step="0.001" class="form-control"
                                           name="min_duration_seconds" value="${settings.min_duration_seconds}">
                                </div>
                                <div class="form-group">
                                    <label>Queries Ativas</label>
                                    <select class="form-control" name="collect_active_queries">
                                        <option value="true" ${settings.collect_active_queries ? 'selected' : ''}>Sim</option>
                                        <option value="false" ${!settings.collect_active_queries ? 'selected' : ''}>Nao</option>
                                    </select>
                                </div>
                                <div class="form-group">
                                    <label>Queries Caras</label>
                                    <select class="form-control" name="collect_expensive_queries">
                                        <option value="true" ${settings.collect_expensive_queries ? 'selected' : ''}>Sim</option>
                                        <option value="false" ${!settings.collect_expensive_queries ? 'selected' : ''}>Nao</option>
                                    </select>
                                </div>
                                <div class="form-group">
                                    <label>Table Scans</label>
                                    <select class="form-control" name="collect_table_scans">
                                        <option value="true" ${settings.collect_table_scans ? 'selected' : ''}>Sim</option>
                                        <option value="false" ${!settings.collect_table_scans ? 'selected' : ''}>Nao</option>
                                    </select>
                                </div>
                            </div>
                            <div class="form-actions" style="margin-top:1rem">
                                <button type="submit" class="btn btn-primary">Salvar</button>
                                <button type="button" class="btn btn-secondary" onclick="SettingsManager.cancelEditCollection('${dbType}')">Cancelar</button>
                            </div>
                        </form>
                    </td>
                </tr>
            `;
            const row = document.querySelector(`tr[data-dbtype="${dbType}"]`);
            if (row) {
                const oldForm = document.getElementById(`edit-row-${dbType}`);
                if (oldForm) oldForm.remove();
                row.insertAdjacentHTML('afterend', formHtml);
                document.getElementById(`form-edit-collection-${dbType}`)
                    .addEventListener('submit', async (e) => {
                        e.preventDefault();
                        await this.saveCollectionSettings(dbType, new FormData(e.target));
                    });
            }
        } catch (error) {
            console.error('Erro ao editar collection:', error);
            this.toast('Erro ao carregar formulario', 'error');
        }
    },

    cancelEditCollection(dbType) {
        const editRow = document.getElementById(`edit-row-${dbType}`);
        if (editRow) editRow.remove();
    },

    async saveCollectionSettings(dbType, formData) {
        const data = {
            min_duration_seconds: parseFloat(formData.get('min_duration_seconds')),
            collect_active_queries: formData.get('collect_active_queries') === 'true',
            collect_expensive_queries: formData.get('collect_expensive_queries') === 'true',
            collect_table_scans: formData.get('collect_table_scans') === 'true'
        };
        try {
            const response = await fetch(`/api/settings/collection/${dbType}`, {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data)
            });
            if (response.ok) {
                this.toast(`Configuracoes de ${dbType} salvas com sucesso!`, 'success');
                this.cancelEditCollection(dbType);
                this.loadCollectionSettings();
            } else {
                const error = await response.json();
                this.toast(error.detail || 'Erro ao salvar', 'error');
            }
        } catch (error) {
            console.error('Erro ao salvar:', error);
            this.toast('Erro de comunicacao com servidor', 'error');
        }
    },

    // ====== PROMPTS ======

    renderPromptForms() {
        const container = document.getElementById('promptsAccordion');
        if (!container) return;
        container.innerHTML = this.promptFormConfig.map(cfg => `
            <div class="accordion-item">
                <div class="accordion-header" onclick="SettingsManager.toggleAccordion('prompt-${cfg.shortType}')">
                    <h4>${this.escapeHtml(cfg.label)} <span class="badge">Versao: <span id="version-${cfg.shortType}">-</span></span></h4>
                    <span class="accordion-icon">&#9660;</span>
                </div>
                <div id="prompt-${cfg.shortType}" class="accordion-content open">
                    <form id="form-prompt-${cfg.shortType}" class="settings-form">
                        <div class="form-group">
                            <label>Nome</label>
                            <input type="text" class="form-control" name="name" required>
                        </div>
                        <div class="form-group">
                            <label>Conteudo</label>
                            <textarea class="form-control" name="content" rows="${cfg.rows}" style="font-family: monospace;"></textarea>
                            <small class="text-muted">${this.escapeHtml(cfg.description)}</small>
                        </div>
                        <div class="form-group">
                            <label>Motivo da Mudanca</label>
                            <input type="text" class="form-control" name="change_reason" placeholder="Opcional">
                        </div>
                        <div class="form-actions">
                            <button type="submit" class="btn btn-primary">Salvar</button>
                            <button type="button" class="btn btn-secondary" onclick="SettingsManager.showPromptHistory('${cfg.fullType}')">Ver Historico</button>
                        </div>
                    </form>
                </div>
            </div>
        `).join('');
        this.attachPromptFormHandlers();
    },

    async loadPromptsForDbType() {
        const dbType = document.getElementById('prompt-db-type').value;
        if (!dbType) {
            document.getElementById('prompts-editor').style.display = 'none';
            return;
        }
        this.currentDbType = dbType;
        document.getElementById('prompts-editor').style.display = 'block';
        try {
            const response = await fetch(`/api/prompts?db_type=${dbType}`);
            const prompts = await response.json();
            for (const promptType of this.promptTypes) {
                const prompt = prompts.find(p => p.prompt_type === promptType);
                const formId = `form-prompt-${this.promptTypeMap[promptType]}`;
                const form = document.getElementById(formId);
                if (form && prompt) {
                    form.querySelector('[name="name"]').value = prompt.name || '';
                    form.querySelector('[name="content"]').value = prompt.content || '';
                    document.getElementById(`version-${this.promptTypeMap[promptType]}`).textContent = prompt.version || '-';
                }
            }
        } catch (error) {
            console.error('Erro ao carregar prompts:', error);
            this.toast('Erro ao carregar prompts', 'error');
        }
    },

    attachPromptFormHandlers() {
        this.promptFormConfig.forEach(cfg => {
            document.getElementById(`form-prompt-${cfg.shortType}`)?.addEventListener('submit', async (e) => {
                e.preventDefault();
                if (!this.currentDbType) {
                    this.toast('Selecione um tipo de banco primeiro', 'warning');
                    return;
                }
                const btn = e.target.querySelector('button[type="submit"]');
                this.setBtnLoading(btn, true);
                const formData = new FormData(e.target);
                const data = {
                    name: formData.get('name'), content: formData.get('content'),
                    change_reason: formData.get('change_reason') || 'Atualizacao via UI',
                    updated_by: 'web_user'
                };
                try {
                    const response = await fetch(`/api/prompts/${this.currentDbType}/${cfg.fullType}`, {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(data)
                    });
                    if (response.ok) {
                        this.toast(`Prompt ${cfg.shortType} salvo com sucesso!`, 'success');
                        this.loadPromptsForDbType();
                    } else {
                        this.toast('Erro ao salvar prompt', 'error');
                    }
                } catch (error) {
                    console.error('Erro:', error);
                    this.toast('Erro ao salvar prompt', 'error');
                } finally {
                    this.setBtnLoading(btn, false);
                }
            });
        });
    },

    async showPromptHistory(promptType) {
        if (!this.currentDbType) {
            this.toast('Selecione um tipo de banco primeiro', 'warning');
            return;
        }
        try {
            const response = await fetch(`/api/prompts/${this.currentDbType}/${promptType}/history`);
            const data = await response.json();
            const tbody = document.getElementById('history-table-body');
            tbody.innerHTML = data.history.map(h => `
                <tr>
                    <td>${this.escapeHtml(h.version)}</td>
                    <td>${this.escapeHtml(new Date(h.updated_at).toLocaleString('pt-BR'))}</td>
                    <td>${this.escapeHtml(h.updated_by)}</td>
                    <td>${this.escapeHtml(this._getChangeReason(data.changes, h.version))}</td>
                    <td>${this.escapeHtml(h.content_length)} chars</td>
                    <td>${h.is_active
                        ? '<span class="badge badge-success">Ativo</span>'
                        : '<span class="badge badge-secondary">Inativo</span>'}</td>
                    <td>${!h.is_active
                        ? `<button class="btn btn-sm" onclick="SettingsManager.rollbackPrompt('${this.escapeHtml(promptType)}', ${Number(h.version)})">Restaurar</button>`
                        : ''}</td>
                </tr>
            `).join('');
            document.getElementById('history-modal').style.display = 'flex';
        } catch (error) {
            console.error('Erro ao carregar historico:', error);
            this.toast('Erro ao carregar historico', 'error');
        }
    },

    _getChangeReason(changes, version) {
        const change = changes.find(c => c.version === version);
        return change?.change_reason || '-';
    },

    closeHistoryModal() {
        document.getElementById('history-modal').style.display = 'none';
    },

    async rollbackPrompt(promptType, version) {
        if (!this.currentDbType) return;
        if (!confirm(`Restaurar prompt para versao ${version}?`)) return;
        try {
            const response = await fetch(`/api/prompts/${this.currentDbType}/${promptType}/rollback/${version}`, {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ restored_by: 'web_user', change_reason: `Rollback para versao ${version} via UI` })
            });
            if (response.ok) {
                this.toast('Prompt restaurado com sucesso!', 'success');
                this.closeHistoryModal();
                this.loadPromptsForDbType();
            } else {
                this.toast('Erro ao restaurar prompt', 'error');
            }
        } catch (error) {
            console.error('Erro:', error);
            this.toast('Erro ao restaurar prompt', 'error');
        }
    },

    // ====== INIT ======

    init() {
        this.initTabs();
        this.initThresholdForms();
        this.initCacheForm();
        this.initLLMForm();
        this.initMonitorForm();
        this.initTeamsForm();
        this.initTimeoutsForm();
        this.initSecurityForm();
        this.initQueryCacheForm();
        this.initLoggingForm();
        this.initStorageForm();
        this.initWeeklyOptimizerForm();

        // Load all data
        this.loadThresholds();
        this.loadCollectionSettings();
        this.loadCacheConfig();
        this.loadLLMConfig();
        this.loadMonitorConfig();
        this.loadTeamsConfig();
        this.loadTimeoutsConfig();
        this.loadSecurityConfig();
        this.loadQueryCacheConfig();
        this.loadLoggingConfig();
        this.loadStorageConfig();
        this.loadWeeklyOptimizerConfig();
        this.loadAuditLog();
        this.renderPromptForms();

        // Modal close on click outside
        document.addEventListener('click', (e) => {
            if (e.target === document.getElementById('history-modal')) {
                this.closeHistoryModal();
            }
        });
    }
};

// Legacy global functions for onclick handlers in HTML
window.toggleAccordion = (id) => SettingsManager.toggleAccordion(id);
window.resetDefaults = (dbType) => SettingsManager.resetDefaults(dbType);
window.clearCache = () => SettingsManager.clearCache();
window.testTeamsWebhook = () => SettingsManager.testTeamsWebhook();
window.editCollection = (dbType) => SettingsManager.editCollection(dbType);
window.loadPromptsForDbType = () => SettingsManager.loadPromptsForDbType();
window.showPromptHistory = (type) => SettingsManager.showPromptHistory(type);
window.closeHistoryModal = () => SettingsManager.closeHistoryModal();
window.rollbackPrompt = (type, ver) => SettingsManager.rollbackPrompt(type, ver);

document.addEventListener('DOMContentLoaded', () => SettingsManager.init());
