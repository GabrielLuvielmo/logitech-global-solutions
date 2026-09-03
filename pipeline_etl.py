#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE ETL - LogiTech Global Solutions
Autor: Engenharia de Dados
Data: 2024
Descrição: ETL com tratamento defensivo de anomalias, normalização e carga em Star Schema
"""

import pandas as pd
import json
import sqlite3
import unicodedata
import re
from datetime import datetime
import sys
import traceback

print("\n" + "="*70)
print("PIPELINE ETL v2.0 - LogiTech Global Solutions")
print("Engenharia de Dados e MLOps - Data Warehouse Construction")
print("="*70)

# ============ CONFIGURAÇÕES GLOBAIS ============
DB_PATH = 'logitech_dw.db'
LOG_ERRORS = []
STATS = {'duplicatas_removidas': 0, 'nulos_preenchidos': 0, 'registros_invalidos': 0}

# ============ FUNÇÕES DE LIMPEZA (REQUISITO 2) ============

def limpar_acentos(texto):
    """Remove acentos e caracteres especiais (sanitização de strings)"""
    if pd.isna(texto):
        return texto
    try:
        nfd = unicodedata.normalize('NFD', str(texto).strip())
        return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    except Exception as e:
        LOG_ERRORS.append(f"Erro ao limpar acentos: {e}")
        return str(texto)

def padronizar_data(data):
    """Converte datas para formato único YYYY-MM-DD"""
    if pd.isna(data) or data == '':
        return None
    
    formatos = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y']
    
    for fmt in formatos:
        try:
            data_obj = datetime.strptime(str(data).strip(), fmt)
            return data_obj.strftime('%Y-%m-%d')
        except:
            continue
    
    LOG_ERRORS.append(f"Data inválida e não conversível: {data}")
    return None

def limpar_cpf(cpf):
    """Sanitiza CPF/CNPJ removendo formatação"""
    if pd.isna(cpf) or cpf == '':
        return None
    
    try:
        cpf_limpo = re.sub(r'[.\-\s/]', '', str(cpf).strip())
        
        # Validar se é número puro e tem tamanho correto
        if cpf_limpo.isdigit() and len(cpf_limpo) in [11, 14]:
            return cpf_limpo
        else:
            STATS['registros_invalidos'] += 1
            return None
    except Exception as e:
        LOG_ERRORS.append(f"Erro ao limpar CPF: {cpf} - {e}")
        return None

def padronizar_status(status):
    """Normaliza status em caixa padronizada"""
    if pd.isna(status) or status == '':
        return None
    
    try:
        s = limpar_acentos(str(status)).lower().replace('_', ' ').strip()
        
        if 'entregue' in s:
            return 'Entregue'
        elif 'transito' in s or 'trânsito' in s:
            return 'Em Transito'
        elif 'cancelado' in s:
            return 'Cancelado'
        else:
            return 'Desconhecido'
    except Exception as e:
        LOG_ERRORS.append(f"Erro ao padronizar status: {status} - {e}")
        return 'Desconhecido'

# ============ FASE 1: INGESTÃO (REQUISITO 2) ============
print("\n[FASE 1] INGESTÃO DE DADOS")
print("-" * 70)

df_envios = None
df_clientes = None

try:
    # Ler CSV
    df_envios = pd.read_csv('envios_brutos.csv', dtype={'ID_Transacao': str})
    print(f"✓ CSV carregado: {len(df_envios)} registros")
    print(f"  Colunas: {', '.join(df_envios.columns)}")
except FileNotFoundError:
    print("✗ ERRO: envios_brutos.csv não encontrado!")
    sys.exit(1)
except Exception as e:
    print(f"✗ ERRO ao ler CSV: {e}")
    LOG_ERRORS.append(f"Erro na leitura do CSV: {e}")
    sys.exit(1)

try:
    # Ler JSON
    with open('clientes_crm.json', 'r', encoding='utf-8') as f:
        clientes_json = json.load(f)
    df_clientes = pd.DataFrame(clientes_json)
    print(f"✓ JSON carregado: {len(df_clientes)} registros")
    print(f"  Colunas: {', '.join(df_clientes.columns)}")
except FileNotFoundError:
    print("✗ ERRO: clientes_crm.json não encontrado!")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"✗ ERRO ao decodificar JSON: {e}")
    sys.exit(1)
except Exception as e:
    print(f"✗ ERRO ao ler JSON: {e}")
    LOG_ERRORS.append(f"Erro na leitura do JSON: {e}")
    sys.exit(1)

# ============ FASE 2: LIMPAR ENVIOS (REQUISITO 2) ============
print("\n[FASE 2] LIMPEZA E NORMALIZAÇÃO DE ENVIOS")
print("-" * 70)

try:
    # Remover duplicatas por ID_Transacao
    duplicatas_antes = len(df_envios)
    df_envios = df_envios.drop_duplicates(subset=['ID_Transacao'], keep='first')
    STATS['duplicatas_removidas'] = duplicatas_antes - len(df_envios)
    print(f"✓ Duplicatas removidas: {STATS['duplicatas_removidas']}")
    
    # Padronizar datas
    print("  Padronizando datas (formato: YYYY-MM-DD)...")
    df_envios['Data_Envio'] = df_envios['Data_Envio'].apply(padronizar_data)
    datas_nulas = df_envios['Data_Envio'].isna().sum()
    if datas_nulas > 0:
        print(f"    ⚠ {datas_nulas} datas não conversíveis foram marcadas como NULL")
        df_envios = df_envios[df_envios['Data_Envio'].notna()]  # Remover registros com datas inválidas
    
    # Padronizar status
    print("  Padronizando status de entrega...")
    df_envios['Status_Entrega'] = df_envios['Status_Entrega'].apply(padronizar_status)
    
    # Preencher valores nulos de frete com a média
    nulos_frete = df_envios['Valor_Frete_USD'].isna().sum()
    if nulos_frete > 0:
        media_frete = df_envios['Valor_Frete_USD'].mean()
        df_envios['Valor_Frete_USD'] = df_envios['Valor_Frete_USD'].fillna(media_frete)
        STATS['nulos_preenchidos'] += nulos_frete
        print(f"  ⚠ {nulos_frete} valores nulos de frete preenchidos com média: ${media_frete:.2f}")
    
    # Garantir tipos de dados corretos
    df_envios['Valor_Frete_USD'] = pd.to_numeric(df_envios['Valor_Frete_USD'], errors='coerce')
    
    print(f"✓ Envios limpos: {len(df_envios)} registros válidos")
    
except Exception as e:
    print(f"✗ ERRO na limpeza de envios: {e}")
    traceback.print_exc()
    LOG_ERRORS.append(f"Erro na limpeza de envios: {e}")
    sys.exit(1)

# ============ FASE 3: LIMPAR CLIENTES (REQUISITO 2) ============
print("\n[FASE 3] LIMPEZA E NORMALIZAÇÃO DE CLIENTES")
print("-" * 70)

try:
    # Padronizar nomes de colunas
    df_clientes.columns = df_clientes.columns.str.lower().str.strip()
    
    # Garantir colunas obrigatórias
    colunas_obrigatorias = {
        'cliente_id': 'N/A',
        'nome_completo': 'N/A',
        'documento_cpf_cnpj': '00000000000',
        'regiao_estado': 'SP',
        'categoria_conta': 'Standard'
    }
    
    for col, valor_padrao in colunas_obrigatorias.items():
        if col not in df_clientes.columns:
            df_clientes[col] = valor_padrao
            print(f"  ⚠ Coluna ausente '{col}' criada com valor padrão")
    
    # Limpar nome completo
    print("  Limpando nomes (removendo acentos)...")
    df_clientes['nome_completo'] = df_clientes['nome_completo'].apply(limpar_acentos)
    
    # Limpar e validar CPF/CNPJ
    print("  Limpando CPF/CNPJ...")
    cpfs_validos_antes = df_clientes['documento_cpf_cnpj'].notna().sum()
    df_clientes['documento_cpf_cnpj'] = df_clientes['documento_cpf_cnpj'].apply(limpar_cpf)
    cpfs_validos_depois = df_clientes['documento_cpf_cnpj'].notna().sum()
    print(f"    CPFs válidos: {cpfs_validos_antes} → {cpfs_validos_depois}")
    
    # Padronizar estado (uppercase)
    df_clientes['regiao_estado'] = df_clientes['regiao_estado'].str.upper().str.strip()
    
    # Remover duplicatas de clientes
    dup_antes = len(df_clientes)
    df_clientes = df_clientes.drop_duplicates(subset=['cliente_id'], keep='first')
    dup_removidas = dup_antes - len(df_clientes)
    if dup_removidas > 0:
        print(f"  ⚠ {dup_removidas} duplicatas de cliente removidas")
    
    # Selecionar apenas colunas necessárias
    df_clientes = df_clientes[['cliente_id', 'nome_completo', 'documento_cpf_cnpj', 'regiao_estado', 'categoria_conta']]
    
    print(f"✓ Clientes limpos: {len(df_clientes)} registros válidos")
    
except Exception as e:
    print(f"✗ ERRO na limpeza de clientes: {e}")
    traceback.print_exc()
    LOG_ERRORS.append(f"Erro na limpeza de clientes: {e}")
    sys.exit(1)

# ============ FASE 4: CRIAR DIMENSÕES (STAR SCHEMA - Requisito 1) ============
print("\n[FASE 4] CRIAÇÃO DO STAR SCHEMA (MODELAGEM DIMENSIONAL)")
print("-" * 70)

try:
    # Tabela Dimensão Tempo
    datas_validas = pd.to_datetime(df_envios['Data_Envio']).unique()
    datas_validas = sorted([d for d in datas_validas if pd.notna(d)])
    
    df_tempo = pd.DataFrame({
        'ID_Data': range(1, len(datas_validas) + 1),
        'Data': datas_validas,
        'Ano': [d.year for d in datas_validas],
        'Mes': [d.month for d in datas_validas],
        'Trimestre': [f"Q{(d.month-1)//3 + 1}" for d in datas_validas],
        'Nome_Mes': [d.strftime('%B') for d in datas_validas]
    })
    print(f"✓ Dim_Tempo criada: {len(df_tempo)} datas únicas")
    print(f"  Período: {df_tempo['Data'].min()} até {df_tempo['Data'].max()}")
    
    # Tabela Dimensão Status
    df_status = pd.DataFrame({
        'ID_Status': [1, 2, 3, 4],
        'Status': ['Entregue', 'Em Transito', 'Cancelado', 'Desconhecido']
    })
    print(f"✓ Dim_Status criada: {len(df_status)} status únicos")
    
    # Tabela Dimensão Cliente
    print(f"✓ Dim_Cliente pronta: {len(df_clientes)} clientes únicos")
    
    # Mapear IDs de dimensão na tabela fato
    print("  Mapeando chaves estrangeiras...")
    
    # Criar mapa de datas para IDs
    mapa_data = dict(zip(df_tempo['Data'].astype(str), df_tempo['ID_Data']))
    df_envios['ID_Data'] = df_envios['Data_Envio'].map(mapa_data)
    
    # Criar mapa de status para IDs
    mapa_status = dict(zip(df_status['Status'], df_status['ID_Status']))
    df_envios['ID_Status'] = df_envios['Status_Entrega'].map(mapa_status)
    
    # Renomear coluna ID_Cliente para manter consistência
    df_envios = df_envios.rename(columns={'ID_Cliente': 'FK_Cliente'})
    
    # Tabela Fato Envios (estrutura final)
    df_fato = df_envios[['ID_Transacao', 'FK_Cliente', 'ID_Data', 'ID_Status', 
                          'Valor_Frete_USD', 'Data_Envio', 'Status_Entrega']].copy()
    print(f"✓ Fato_Envios pronta: {len(df_fato)} transações")
    
except Exception as e:
    print(f"✗ ERRO na criação das dimensões: {e}")
    traceback.print_exc()
    LOG_ERRORS.append(f"Erro na criação das dimensões: {e}")
    sys.exit(1)

# ============ FASE 5: CARREGAR NO BANCO DE DADOS (Requisito 2) ============
print("\n[FASE 5] CARGA NO DATA WAREHOUSE (SQLite)")
print("-" * 70)

try:
    # Remover banco antigo se existir
    import os
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"  ⚠ Banco anterior removido ({DB_PATH})")
    
    # Conectar e criar banco
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Criar tabelas com tipos de dados apropriados
    print("  Criando schema do Data Warehouse...")
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Cliente (
            cliente_id TEXT PRIMARY KEY,
            nome_completo TEXT NOT NULL,
            documento_cpf_cnpj TEXT,
            regiao_estado TEXT NOT NULL,
            categoria_conta TEXT NOT NULL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Tempo (
            ID_Data INTEGER PRIMARY KEY,
            Data DATE NOT NULL UNIQUE,
            Ano INTEGER NOT NULL,
            Mes INTEGER NOT NULL,
            Trimestre TEXT,
            Nome_Mes TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Status (
            ID_Status INTEGER PRIMARY KEY,
            Status TEXT NOT NULL UNIQUE
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Fato_Envios (
            ID_Transacao TEXT PRIMARY KEY,
            FK_Cliente TEXT NOT NULL,
            ID_Data INTEGER NOT NULL,
            ID_Status INTEGER NOT NULL,
            Valor_Frete_USD REAL NOT NULL,
            Data_Envio DATE,
            Status_Entrega TEXT,
            FOREIGN KEY (FK_Cliente) REFERENCES Dim_Cliente(cliente_id),
            FOREIGN KEY (ID_Data) REFERENCES Dim_Tempo(ID_Data),
            FOREIGN KEY (ID_Status) REFERENCES Dim_Status(ID_Status)
        )
    ''')
    
    print("  Carregando Dim_Cliente...")
    df_clientes.to_sql('Dim_Cliente', conn, if_exists='replace', index=False)
    
    print("  Carregando Dim_Tempo...")
    df_tempo.to_sql('Dim_Tempo', conn, if_exists='replace', index=False)
    
    print("  Carregando Dim_Status...")
    df_status.to_sql('Dim_Status', conn, if_exists='replace', index=False)
    
    print("  Carregando Fato_Envios...")
    df_fato.to_sql('Fato_Envios', conn, if_exists='replace', index=False)
    
    conn.commit()
    print(f"✓ Banco de dados criado: {DB_PATH}")
    
except sqlite3.Error as e:
    print(f"✗ ERRO no banco de dados SQLite: {e}")
    LOG_ERRORS.append(f"Erro SQLite: {e}")
    if conn:
        conn.rollback()
    sys.exit(1)
except Exception as e:
    print(f"✗ ERRO na carga: {e}")
    traceback.print_exc()
    LOG_ERRORS.append(f"Erro na carga: {e}")
    if conn:
        conn.rollback()
    sys.exit(1)

# ============ FASE 6: VALIDAÇÃO E QUERIES ANALÍTICAS (Requisito 4) ============
print("\n[FASE 6] VALIDAÇÃO - CONSULTAS ANALÍTICAS")
print("-" * 70)

try:
    # Query 1: Total Faturado em BRL por Região/Estado
    print("\n📊 CONSULTA 1: FATURAMENTO POR REGIÃO/ESTADO (em BRL)")
    print("SQL: SELECT regiao_estado, COUNT(*) AS Qtd_Envios, SUM(Valor_Frete_USD * 5.15) AS Total_BRL")
    print("-" * 70)
    
    query1 = """
    SELECT 
        c.regiao_estado AS Estado,
        COUNT(f.ID_Transacao) AS Quantidade_Envios,
        ROUND(SUM(f.Valor_Frete_USD), 2) AS Total_USD,
        ROUND(SUM(f.Valor_Frete_USD * 5.15), 2) AS Total_BRL
    FROM Fato_Envios f
    LEFT JOIN Dim_Cliente c ON f.FK_Cliente = c.cliente_id
    GROUP BY c.regiao_estado
    ORDER BY Total_BRL DESC
    """
    
    resultado1 = pd.read_sql_query(query1, conn)
    print(resultado1.to_string(index=False))
    print(f"Total geral: R$ {resultado1['Total_BRL'].sum():.2f}")
    
    # Query 2: Percentual de Status por Mês
    print("\n\n📊 CONSULTA 2: PERCENTUAL DE STATUS DE ENTREGA POR MÊS")
    print("SQL: SELECT Ano, Mes, Status, COUNT(*) AS Qtd, PERCENTUAL")
    print("-" * 70)
    
    query2 = """
    SELECT 
        t.Ano,
        t.Mes,
        t.Nome_Mes,
        s.Status,
        COUNT(f.ID_Transacao) AS Quantidade,
        ROUND(100.0 * COUNT(f.ID_Transacao) / SUM(COUNT(f.ID_Transacao)) OVER (PARTITION BY t.Ano, t.Mes), 2) AS Percentual
    FROM Fato_Envios f
    LEFT JOIN Dim_Tempo t ON f.ID_Data = t.ID_Data
    LEFT JOIN Dim_Status s ON f.ID_Status = s.ID_Status
    GROUP BY t.Ano, t.Mes, s.Status
    ORDER BY t.Ano, t.Mes, Percentual DESC
    """
    
    resultado2 = pd.read_sql_query(query2, conn)
    print(resultado2.to_string(index=False))
    
    # Estatísticas finais
    print("\n\n📊 ESTATÍSTICAS DE QUALIDADE DOS DADOS")
    print("-" * 70)
    
    query_stats = """
    SELECT 
        (SELECT COUNT(*) FROM Fato_Envios) AS Total_Transacoes,
        (SELECT COUNT(DISTINCT FK_Cliente) FROM Fato_Envios) AS Clientes_Ativos,
        (SELECT COUNT(*) FROM Dim_Tempo) AS Periodos,
        (SELECT COUNT(DISTINCT ID_Status) FROM Fato_Envios) AS Status_Utilizados,
        ROUND((SELECT AVG(Valor_Frete_USD) FROM Fato_Envios), 2) AS Media_Frete_USD,
        ROUND((SELECT MAX(Valor_Frete_USD) FROM Fato_Envios), 2) AS Max_Frete_USD,
        ROUND((SELECT MIN(Valor_Frete_USD) FROM Fato_Envios), 2) AS Min_Frete_USD
    """
    
    stats_df = pd.read_sql_query(query_stats, conn)
    print(stats_df.to_string(index=False))
    
    conn.close()
    
except Exception as e:
    print(f"✗ ERRO na validação: {e}")
    traceback.print_exc()
    LOG_ERRORS.append(f"Erro na validação: {e}")
    sys.exit(1)

# ============ RELATÓRIO FINAL ============
print("\n" + "="*70)
print("✓ PIPELINE ETL CONCLUÍDO COM SUCESSO!")
print("="*70)

print("\n📊 RESUMO DA EXECUÇÃO:")
print(f"  • Duplicatas removidas: {STATS['duplicatas_removidas']}")
print(f"  • Valores nulos preenchidos: {STATS['nulos_preenchidos']}")
print(f"  • Registros inválidos: {STATS['registros_invalidos']}")
print(f"  • Arquivo de saída: {DB_PATH}")

if LOG_ERRORS:
    print(f"\n⚠️  ALERTAS/ERROS TRATADOS: {len(LOG_ERRORS)}")
    for erro in LOG_ERRORS[:5]:  # Mostrar primeiros 5
        print(f"  - {erro}")
    if len(LOG_ERRORS) > 5:
        print(f"  ... e mais {len(LOG_ERRORS) - 5} alertas")

print("\n" + "="*70)