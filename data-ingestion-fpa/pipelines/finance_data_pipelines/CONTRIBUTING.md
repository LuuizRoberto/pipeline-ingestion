# Guia de contribuição

## Fluxo de trabalho

Todo desenvolvimento parte da branch `dev`. Nunca faça push direto em `main`.

```
1. git checkout dev
2. git pull origin dev
3. git checkout -b feat/nome-da-sua-mudanca
4. ... desenvolva e teste localmente ...
5. git push origin feat/nome-da-sua-mudanca
6. Abra um Pull Request: sua branch → main
```

## Convenção de commits

Use prefixos para facilitar o histórico:

| Prefixo | Quando usar |
|---|---|
| `feat:` | Nova funcionalidade ou pipeline |
| `fix:` | Correção de bug |
| `refactor:` | Mudança interna sem alterar comportamento |
| `docs:` | Atualização de documentação |
| `test:` | Adição ou correção de testes |
| `chore:` | Dependências, configs, CI |

Exemplos:
```
feat: add extractor for nova_fonte API
fix: handle empty guide list in lancamento_contabil
docs: update README with Databricks setup steps
```

## Adicionando uma nova fonte de dados

1. Crie a pasta `sources/<nome_da_fonte>/`
2. Adicione `config/config.py` com o dicionário de configuração
3. Adicione `extractor/extractor.py` herdando `BaseExtractor`
4. Implemente obrigatoriamente: `get_api_data()` e `normalize_data()`
5. Crie os jobs em `jobs/<nome_da_fonte>/run_<tabela>.py`
6. Adicione testes em `tests/unit/test_<nome_da_fonte>.py`
7. Atualize o `README.md` com a nova fonte

## Rodando localmente

```bash
# Setup
python -m venv .venv
source .venv/bin/activate      # Linux/Mac
.venv\Scripts\activate         # Windows

pip install -e ".[dev]"

# Testes
make test

# Lint
make lint
```

## Checklist antes de abrir PR

- [ ] Testes passando (`make test`)
- [ ] Sem erros de lint (`make lint`)
- [ ] README atualizado se necessário
- [ ] Nenhuma credencial ou `.env` commitado
