# Scripts - SQL Performance Monitor

Este diretório contém scripts auxiliares para manutenção e configuração do sistema.

## 📁 Estrutura

```
scripts/
├── README.md (este arquivo)
└── validate_config.py      # Validador de arquivos de configuração
```

## 📜 Scripts Disponíveis

### validate_config.py

**Propósito**: Validar arquivos de configuração do sistema antes de executar.

**Uso**:
```bash
python scripts/validate_config.py
```

**O que faz**:
- Valida `config.json` (configurações gerais)
- Valida `config/databases.json` (configurações de banco)
- Verifica estrutura JSON
- Valida campos obrigatórios
- Verifica tipos de dados
- Exibe erros e avisos

**Saída de exemplo**:
```
✓ config.json válido
✓ config/databases.json válido
✓ 3 bancos configurados
⚠️ SQL_PASSWORD não definida (usando plaintext)
```

## 🔧 Adicionar Novos Scripts

Ao adicionar um novo script:

1. **Criar o arquivo** em `scripts/`:
   ```bash
   touch scripts/novo_script.py
   chmod +x scripts/novo_script.py
   ```

2. **Adicionar shebang**:
   ```python
   #!/usr/bin/env python3
   """
   Descrição do script.
   """
   ```

3. **Documentar** neste README:
   - Nome do script
   - Propósito
   - Como usar
   - Parâmetros (se houver)

4. **Adicionar ao .gitignore** se gerar arquivos temporários

## 🎯 Boas Práticas

### Estrutura de um Script

```python
#!/usr/bin/env python3
"""
Nome do Script - Descrição breve.

Uso:
    python scripts/nome_script.py [argumentos]
"""

import sys
import os
from pathlib import Path

# Adicionar raiz do projeto ao PATH
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def main():
    """Função principal."""
    # Implementação
    pass

if __name__ == '__main__':
    main()
```

### Checklist

- [ ] Shebang presente
- [ ] Docstring descritiva
- [ ] Tratamento de erros
- [ ] Mensagens claras para o usuário
- [ ] Documentado no README
- [ ] Testado antes de commitar

## 🚀 Exemplos de Uso

### Validar configuração antes de deploy
```bash
python scripts/validate_config.py && python main.py
```

### Em CI/CD
```yaml
# .github/workflows/deploy.yml
- name: Validate configuration
  run: python scripts/validate_config.py
```

## 📚 Documentação

Para mais detalhes sobre configuração:
- `docs/CONFIGURATION.md` - Guia de configuração
- `docs/SECURITY.md` - Segurança e credenciais
- `README.md` - Documentação principal

---

**Última atualização**: 2026-01-07
