# LogiTech - Pipeline ETL Simples

## Como Executar em 3 Passos

### 1. Instalar Pandas
```bash
pip install pandas
```

### 2. Coloque os 4 arquivos no mesmo diretório
```
seu_projeto/
  ├── envios_brutos.csv
  ├── clientes_crm.json
  └── pipeline_etl.py
```

### 3. Executar o Pipeline
```bash
python pipeline_etl.py
```



## O que o Script Faz

1. **Lê** os dados brutos (CSV + JSON)
2. **Limpa** anomalias (datas, acentos, status, CPF)
3. **Remove** duplicatas e valores nulos
4. **Cria** tabelas de dimensão (Cliente, Tempo, Status)
5. **Salva** tudo em SQLite
6. **Executa** 2 queries de validação
7. **Mostra** os resultados no console



## Tabelas Criadas

| Tabela | Descrição |
|--------|-----------|
| **Dim_Cliente** | Dados de clientes |
| **Dim_Tempo** | Calendário |
| **Dim_Status** | Status de entrega |
| **Fato_Envios** | Transações de envios |

---

## Resultado Final

- Banco de dados: `logitech_dw.db`
- 2 Queries executadas e mostradas na tela
- Dados prontos para análise



## Para Ver os Dados no Banco

Opção 1: Download do DB Browser (gratuito)
https://sqlitebrowser.org/

Opção 2: Usar Python
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('logitech_dw.db')
df = pd.read_sql_query("SELECT * FROM Dim_Cliente", conn)
print(df)
```

