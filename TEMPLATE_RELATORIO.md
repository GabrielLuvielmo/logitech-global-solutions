# Trabalho de ETL - LogiTech Global Solutions



## 1. INTRODUÇÃO

A LogiTech Global Solutions é uma empresa de logística que enfrentava problemas com inconsistências em seus dados de fretes, causadas por 3 sistemas operacionais isolados com formatos diferentes.

**Objetivo:** Criar um pipeline ETL que unifique esses dados em um Data Warehouse corporativo.



## 2. PROBLEMAS IDENTIFICADOS

### Fonte 1: Sistema Legacy-ERP (CSV)
- Datas em dois formatos: `DD/MM/AAAA` e `AAAA-MM-DD`
- Valores de frete com campos vazios
- Registros duplicados
- Status em diferentes caixas (maiúsculas, minúsculas, mistas)

### Fonte 2: Sistema CRM-Cloud (JSON)
- Caracteres acentuados corrompidos
- CPFs com e sem formatação
- Nomes de campos inconsistentes
- Categorias de conta em diferentes formatos

**Impacto:** Impossível fazer análises confiáveis, retrabalho manual, decisões prejudicadas.



## 3. SOLUÇÃO PROPOSTA

### Arquitetura
```
CSV (Envios) ──┐
               ├─→ Pipeline ETL ──→ Limpeza ──→ Data Warehouse ──→ SQL Queries
JSON (Clientes)┘
```

### Star Schema (4 Tabelas)

**Dim_Cliente**
- cliente_id (PK)
- nome_completo
- documento_cpf_cnpj
- regiao_estado
- categoria_conta

**Dim_Tempo**
- ID_Data (PK)
- Data
- Ano
- Mes

**Dim_Status**
- ID_Status (PK)
- Status

**Fato_Envios**
- ID_Transacao (PK)
- ID_Cliente (FK)
- Data_Envio
- Status_Entrega
- Valor_Frete_USD
- Valor_Frete_BRL

---

## 4. PIPELINE ETL

O script Python executa 6 fases:

1. **Ingestão:** Lê CSV e JSON
2. **Limpeza de Envios:** Remove duplicatas, padroniza datas e status
3. **Limpeza de Clientes:** Remove acentos, padroniza CPF, mapeia colunas
4. **Criar Dimensões:** Cria tabelas de tempo e status
5. **Carregar:** Salva tudo em SQLite
6. **Validar:** Executa 2 queries analíticas



## 5. TRATAMENTO DE ANOMALIAS

| Anomalia | Tratamento |
|----------|-----------|
| Datas mistas | Função que tenta múltiplos formatos → YYYY-MM-DD |
| Valores nulos | Preenchimento com média |
| Duplicatas | Remoção com keep='first' |
| Acentos | Decomposição NFD + limpeza |
| Status inconsistente | Mapeamento case-insensitive |
| CPF formatado | Remove pontos e hífens |
| Colunas não padrão | Mapeamento dinâmico de nomes |



## 6. CÓDIGO PRINCIPAL

O script `pipeline_etl.py` contém ~100 linhas de código Python com:

Limpeza de dados  
Remoção de duplicatas  
Padronização de datas  
Criação de dimensões  
Carregamento em SQLite  
2 Queries de validação  


## 7. QUERIES ANALÍTICAS

### Query 1: Total Faturado por Estado

```sql
SELECT 
    c.regiao_estado AS Estado,
    COUNT(f.ID_Transacao) AS Qtd_Envios,
    ROUND(SUM(f.Valor_Frete_USD * 5.15), 2) AS Total_BRL
FROM Fato_Envios f
LEFT JOIN Dim_Cliente c ON f.ID_Cliente = c.cliente_id
GROUP BY c.regiao_estado
ORDER BY Total_BRL DESC
```

**Resultado esperado:** Mostra faturamento por estado em reais

### Query 2: Status por Mês

```sql
SELECT 
    t.Ano,
    t.Mes,
    f.Status_Entrega AS Status,
    COUNT(f.ID_Transacao) AS Quantidade
FROM Fato_Envios f
LEFT JOIN Dim_Tempo t ON f.Data_Envio = t.Data
GROUP BY t.Ano, t.Mes, f.Status_Entrega
ORDER BY t.Ano, t.Mes, f.Status_Entrega
```

**Resultado esperado:** Mostra distribuição de status (Entregue, Em Trânsito, Cancelado) por mês



## 8. EXECUÇÃO E RESULTADOS

### Passos Realizados:
1. ✓ Instalado Pandas
2. ✓ Colocou os 4 arquivos no mesmo diretório
3. ✓ Executou: `python pipeline_etl.py`
4. ✓ Banco `logitech_dw.db` criado
5. ✓ 2 Queries executadas com sucesso

### Prints Capturados:
- [Print 1] Saída do console mostrando "PIPELINE CONCLUÍDO COM SUCESSO!"
- [Print 2] Query 1 com resultados (faturamento por estado)
- [Print 3] Query 2 com resultados (status por mês)
- [Print 4] Estrutura das tabelas no DB Browser (opcional)



## 9. VERIFICAÇÃO DOS REQUISITOS

| Requisito | Status | Evidência |
|-----------|--------|-----------|
| **1. Arquitetura & Modelagem** | ✅ | Star Schema com 4 tabelas |
| **2. Programação ETL** | ✅ | Script Python funcional |
| **3. Automação & Tratamento de Erros** | ✅ | Try/except, logging |
| **4. Validação com Queries** | ✅ | 2 Queries executadas |



## 10. CONCLUSÃO

O pipeline ETL foi implementado com sucesso, unificando os dados de 3 sistemas em um Data Warehouse único. O projeto demonstra:

✓ Extração de múltiplas fontes heterogêneas  
✓ Transformação robusta de dados  
✓ Armazenamento em modelagem dimensional  
✓ Queries analíticas para suporte à decisão  

A LogiTech agora possui um repositório centralizado de dados, permitindo análises estratégicas sobre fretes por região e performance de entrega ao longo do tempo.



## ANEXOS

### Tabelas Criadas:
- Dim_Cliente: [número de registros]
- Dim_Tempo: [número de registros]
- Dim_Status: 3 registros (Entregue, Em Trânsito, Cancelado)
- Fato_Envios: [número de registros após limpeza]

### Taxa de Conversão:
- USD → BRL = 5.15

### Tecnologias Usadas:
- Python 3.8+
- Pandas
- SQLite3
- SQL



*Trabalho apresentado em Setembro de 2024*
