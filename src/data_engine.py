"""Motor de Dados BondTrack - ETL, Merge SND+Anbima, Limpeza, Curvas ANBIMA"""
import pandas as pd
import numpy as np
import sqlite3
import os
import unicodedata
import streamlit as st
from datetime import datetime

# Caminhos dos bancos de dados
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "debentures_anbima.db")
CURVAS_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "curvas_anbima.db")

def smart_clean(df):
    """Higienização e padronização de dados"""
    if df.empty: 
        return pd.DataFrame()

    # 1. Normalização de nomes de colunas
    temp_map = {}
    for col in df.columns:
        col_str = str(col)
        if col_str == "_merge":
            temp_map[col] = "_merge"
            continue
            
        nfkd = unicodedata.normalize('NFKD', col_str)
        clean = "".join([c for c in nfkd if not unicodedata.combining(c)])
        clean = clean.lower().strip().replace(" ", "_").replace(".", "").replace("/", "_").replace("-", "_")
        temp_map[col] = clean
    df = df.rename(columns=temp_map)
    
    # 2. Mapeamento de Colunas (Keywords)
    keywords = {
        "taxa": ["taxa_indicativa", "taxa_emissao", "taxa_compra", "taxa"],
        "duration": ["duration", "duracao", "du"],
        "pu": ["pu", "preco", "unitario"],
        "indexador": ["indexador", "indice", "idx"],
        "emissor": ["emissor", "nome_emissor", "razao_social", "empresa", "nome"],
        "codigo": ["codigo", "ativo", "ticker"],
        "incentivada": ["deb_incent", "incentivada", "lei_12431", "isenta", "ir"],
        "vencimento": ["vencimento", "data_vencimento", "dt_vencimento"],
        "emissao": ["emissao", "data_emissao", "dt_emissao"]
    }
    
    final_map = {}
    for padrao, lista in keywords.items():
        # Pula se já existe uma coluna com o nome padrão
        if padrao in df.columns:
            continue
        for col in df.columns:
            if any(p in col for p in lista) and padrao not in final_map.values():
                final_map[col] = padrao
                break
    df = df.rename(columns=final_map)

    # 3. Tratamento Numérico
    df['taxa'] = pd.to_numeric(df.get('taxa'), errors='coerce').fillna(0)
    df['pu'] = pd.to_numeric(df.get('pu'), errors='coerce')
    df['duration'] = pd.to_numeric(df.get('duration'), errors='coerce').fillna(0)
    
    # Converter duration de dias úteis para anos se necessário
    if not df.empty and df['duration'].mean() > 50:
        df['duration'] = df['duration'] / 252

    # 4. HIGIENIZAÇÃO DE INDEXADORES (Padronização)
    if 'indexador' not in df.columns:
        df['indexador'] = 'N/D'
    df['indexador'] = df['indexador'].fillna('N/D').astype(str).str.upper().str.strip()
    
    # Dicionário de Correção
    correcoes = {
        r'\bD\.I\.\b': 'CDI',
        r'\bDI\b': 'CDI',
        r'\bIGPM\b': 'IGP-M',
        r'\bIGP\s*M\b': 'IGP-M',
        r'\bIPC-A\b': 'IPCA',
        r'\bIPCA\+\b': 'IPCA',
        r'\bTR\s.*': 'TR',
        r'\bPRE\b': 'PRÉ',
        r'\bPREFIXADO\b': 'PRÉ'
    }
    df['indexador'] = df['indexador'].replace(correcoes, regex=True)

    # Limpeza de Emissor
    if 'emissor' not in df.columns:
        df['emissor'] = 'N/D'
    df['emissor'] = df['emissor'].fillna('N/D').astype(str).str.split("-").str[0].str.strip()
    
    # 5. Categorização para Gráficos
    def classificar(row):
        idx = row["indexador"]
        taxa = row["taxa"]
        
        # Lógica de Incentivada
        incent_val = str(row.get("incentivada", "")).upper()
        is_incentivada = any(x in incent_val for x in ['S', 'SIM', 'YES', 'TRUE', '1'])

        if "IPCA" in idx:
            return "IPCA Incentivado" if is_incentivada else "IPCA Não Incentivado"
            
        if "CDI" in idx:
            # Se taxa > 30, assumimos que é percentual (Ex: 110% do CDI)
            return "% CDI" if taxa > 30 else "CDI +"
        
        if "PRÉ" in idx or "PREFIXADO" in idx:
            return "Prefixado"
            
        return "Outros"

    df["categoria_grafico"] = df.apply(classificar, axis=1)
    df["pu_size"] = pd.to_numeric(df.get("pu", 1000), errors='coerce').fillna(1000).clip(lower=100)

    # Cluster Duration
    def cluster_dur(d):
        if d <= 0: return "Sem Prazo"
        if d <= 1: return "0-1 ano"
        if d <= 3: return "1-3 anos"
        if d <= 5: return "3-5 anos"
        if d <= 10: return "5-10 anos"
        return "10+ anos"
    df["cluster_duration"] = df["duration"].apply(cluster_dur)

    return df

@st.cache_data(ttl=60)
def get_available_dates():
    """Retorna datas disponíveis no banco (ordenadas da mais recente para a mais antiga)"""
    if not os.path.exists(DB_PATH): 
        return []
    
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("SELECT DISTINCT data_referencia FROM mercado_secundario", conn)
        datas = sorted(
            pd.to_datetime(df['data_referencia'], dayfirst=True, errors='coerce')
            .dropna()
            .dt.strftime('%d/%m/%Y')
            .unique(), 
            reverse=True
        )
    except:
        datas = []
    finally:
        conn.close()
    return datas

@st.cache_data(ttl=300)
def load_data(data_selecionada=None):
    """Carrega e mescla dados SND + ANBIMA
    
    Args:
        data_selecionada: Data específica (formato DD/MM/YYYY) ou None para dados mais recentes
    
    Returns:
        tuple: (DataFrame limpo e processado, mensagem de erro ou None)
    """
    if not os.path.exists(DB_PATH): 
        return None, "Banco de dados não encontrado"
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # 1. Carregar ANBIMA (Preços de Mercado Secundário)
        query_anbima = "SELECT * FROM mercado_secundario"
        if data_selecionada:
            query_anbima += f" WHERE data_referencia = '{data_selecionada}'"
        else:
            # Pega data mais recente automaticamente
            query_anbima += " WHERE data_referencia = (SELECT MAX(data_referencia) FROM mercado_secundario)"
        
        df_anbima = pd.read_sql(query_anbima, conn)
        
        # 2. Carregar SND (Cadastro Completo)
        df_snd = pd.read_sql("SELECT * FROM cadastro_snd", conn)
        
    except Exception as e:
        conn.close()
        return None, f"Erro ao carregar dados: {str(e)}"
    finally:
        conn.close()
    
    # --- PREPARAÇÃO DAS CHAVES (Limpeza pré-merge) ---
    if not df_snd.empty:
        df_snd.columns = [c.strip() for c in df_snd.columns]
        if 'Codigo do Ativo' in df_snd.columns:
            if 'codigo' in df_snd.columns: 
                df_snd = df_snd.drop(columns=['codigo'])
            df_snd = df_snd.rename(columns={
                'Codigo do Ativo': 'codigo', 
                'Empresa': 'nome_snd', 
                'indice': 'indexador_snd'
            })
        
        if 'codigo' in df_snd.columns:
            df_snd['codigo'] = df_snd['codigo'].astype(str).str.upper().str.strip()

    if not df_anbima.empty:
        df_anbima.columns = [c.strip() for c in df_anbima.columns]
        if 'codigo' in df_anbima.columns:
            df_anbima['codigo'] = df_anbima['codigo'].astype(str).str.upper().str.strip()

    # --- MERGE (CRUZAMENTO) ---
    # indicator=True cria a coluna '_merge' para rastreamento
    df_final = pd.merge(
        df_anbima, 
        df_snd, 
        on='codigo', 
        how='outer', 
        indicator=True, 
        suffixes=('', '_snd')
    )
    
    # Consolidação de campos (Se não tem na Anbima, pega do SND)
    if 'nome_snd' in df_final.columns:
        if 'nome' not in df_final.columns: 
            df_final['nome'] = None
        df_final['nome'] = df_final['nome'].fillna(df_final['nome_snd'])
    
    if 'indexador_snd' in df_final.columns:
        if 'indexador' not in df_final.columns: 
            df_final['indexador'] = df_final['indexador_snd']
        else:
            df_final['indexador'] = df_final['indexador'].fillna(df_final['indexador_snd'])
    
    # Garantir que emissor existe (usar nome_snd se necessário)
    if 'emissor' not in df_final.columns and 'nome_snd' in df_final.columns:
        df_final['emissor'] = df_final['nome_snd']
    elif 'emissor' not in df_final.columns and 'nome' in df_final.columns:
        df_final['emissor'] = df_final['nome']

    # --- LIMPEZA GERAL E PADRONIZAÇÃO ---
    df_limpo = smart_clean(df_final)
    
    # --- CRIAÇÃO DA COLUNA FONTE ---
    def definir_fonte(row):
        m = row.get('_merge')
        if m == 'both': 
            return 'SND + Anbima'
        elif m == 'left_only': 
            return 'Anbima'
        else: 
            return 'SND'

    df_limpo['FONTE'] = df_limpo.apply(definir_fonte, axis=1)
    
    # Remove colunas técnicas
    df_limpo = df_limpo.drop(columns=['_merge', 'nome_snd', 'indexador_snd'], errors='ignore')
    
    # Filtro final - Remove indexadores inválidos
    df_limpo = df_limpo[~df_limpo['indexador'].isin(['N/D', '', 'NAN', '-', '0'])]
    
    # Adicionar coluna de data_referencia se não existir
    if 'data_referencia' not in df_limpo.columns and data_selecionada:
        df_limpo['data_referencia'] = data_selecionada

    return df_limpo, None

def apply_filters(df, filtros):
    """Aplica filtros ao DataFrame
    
    Args:
        df: DataFrame a ser filtrado
        filtros: Dicionário com os filtros {emissor, indexador, fonte, cluster, categoria}
    
    Returns:
        DataFrame filtrado
    """
    df_f = df.copy()
    
    if filtros.get("emissor"): 
        df_f = df_f[df_f["emissor"].isin(filtros["emissor"])]
    
    if filtros.get("indexador"): 
        df_f = df_f[df_f["indexador"].isin(filtros["indexador"])]
    
    if filtros.get("fonte") and filtros.get("fonte") != "Todos": 
        df_f = df_f[df_f["FONTE"] == filtros["fonte"]]
    
    if filtros.get("cluster"): 
        df_f = df_f[df_f["cluster_duration"].isin(filtros["cluster"])]
    
    if filtros.get("categoria"): 
        df_f = df_f[df_f["categoria_grafico"].isin(filtros["categoria"])]
    
    # Filtros de taxa
    if filtros.get("taxa_min") is not None:
        df_f = df_f[df_f["taxa"] >= filtros["taxa_min"]]
    
    if filtros.get("taxa_max") is not None:
        df_f = df_f[df_f["taxa"] <= filtros["taxa_max"]]
    
    # Filtros de duration
    if filtros.get("duration_min") is not None:
        df_f = df_f[df_f["duration"] >= filtros["duration_min"]]
    
    if filtros.get("duration_max") is not None:
        df_f = df_f[df_f["duration"] <= filtros["duration_max"]]
    
    return df_f

def get_data_quality_report(df):
    """Gera relatório de qualidade dos dados
    
    Returns:
        dict com métricas de qualidade
    """
    report = {
        "total_registros": len(df),
        "campos_completos": {},
        "duplicatas": 0,
        "inconsistencias": []
    }
    
    # Análise de completude por campo
    campos_criticos = ['codigo', 'emissor', 'indexador', 'taxa', 'duration']
    for campo in campos_criticos:
        if campo in df.columns:
            total = len(df)
            validos = df[campo].notna().sum() if campo != 'taxa' else (df[campo] > 0).sum()
            report["campos_completos"][campo] = {
                "validos": int(validos),
                "invalidos": int(total - validos),
                "percentual": round((validos / total * 100), 2) if total > 0 else 0
            }
    
    # Detecção de duplicatas (mesmo código, mesma data)
    if 'codigo' in df.columns and 'data_referencia' in df.columns:
        duplicatas = df.duplicated(subset=['codigo', 'data_referencia'], keep=False)
        report["duplicatas"] = int(duplicatas.sum())
    
    # Inconsistências
    if 'taxa' in df.columns:
        taxas_negativas = (df['taxa'] < 0).sum()
        if taxas_negativas > 0:
            report["inconsistencias"].append(f"{taxas_negativas} taxas negativas")
    
    if 'duration' in df.columns:
        durations_negativas = (df['duration'] < 0).sum()
        if durations_negativas > 0:
            report["inconsistencias"].append(f"{durations_negativas} durations negativas")
    
    # Score geral (0-100)
    completude_media = sum([v["percentual"] for v in report["campos_completos"].values()]) / len(report["campos_completos"]) if report["campos_completos"] else 0
    penalidade_duplicatas = min((report["duplicatas"] / report["total_registros"] * 100), 20) if report["total_registros"] > 0 else 0
    penalidade_inconsistencias = min(len(report["inconsistencias"]) * 5, 20)
    
    report["score_qualidade"] = max(0, completude_media - penalidade_duplicatas - penalidade_inconsistencias)
    
    return report


# ============================================================================
# FUNÇÕES DA CURVA DE JUROS ANBIMA
# ============================================================================

@st.cache_data(ttl=60)
def get_curvas_anbima_dates():
    """Retorna datas disponíveis na tabela de curvas ANBIMA"""
    if not os.path.exists(CURVAS_DB_PATH):
        return []
    
    try:
        conn = sqlite3.connect(CURVAS_DB_PATH)
        df = pd.read_sql("SELECT DISTINCT data_referencia FROM curvas_anbima", conn)
        conn.close()
        
        if df.empty:
            return []
        
        # Converter para datetime e ordenar
        df['data_dt'] = pd.to_datetime(df['data_referencia'], format="%d/%m/%Y", errors='coerce')
        return df.sort_values('data_dt', ascending=False)['data_referencia'].tolist()
    except Exception as e:
        return []


@st.cache_data(ttl=300)
def load_curva_anbima(data_referencia=None):
    """
    Carrega a curva de juros ANBIMA para uma data específica
    
    Args:
        data_referencia: Data no formato DD/MM/YYYY ou None para a mais recente
    
    Returns:
        DataFrame com colunas: dias_corridos, taxa_ipca, taxa_pre, inflacao_implicita
    """
    if not os.path.exists(CURVAS_DB_PATH):
        return pd.DataFrame()
    
    try:
        conn = sqlite3.connect(CURVAS_DB_PATH)
        
        if data_referencia:
            query = f"SELECT * FROM curvas_anbima WHERE data_referencia = '{data_referencia}'"
        else:
            # Pega a data mais recente
            query = """SELECT * FROM curvas_anbima 
                      WHERE data_referencia = (SELECT MAX(data_referencia) FROM curvas_anbima)"""
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    except Exception as e:
        return pd.DataFrame()


def interpolar_taxa_curva(curva_df, dias_uteis, tipo_curva='taxa_ipca'):
    """
    Interpola a taxa da curva para um prazo específico em dias úteis
    
    Args:
        curva_df: DataFrame da curva ANBIMA
        dias_uteis: Número de dias úteis do prazo
        tipo_curva: 'taxa_ipca', 'taxa_pre' ou 'inflacao_implicita'
    
    Returns:
        Taxa interpolada (em % a.a.)
    """
    if curva_df.empty or tipo_curva not in curva_df.columns:
        return None
    
    # Garantir que temos a coluna de dias
    if 'dias_corridos' not in curva_df.columns:
        return None
    
    curva = curva_df.sort_values('dias_corridos')
    
    # Se o prazo está dentro do range da curva
    max_dias = curva['dias_corridos'].max()
    min_dias = curva['dias_corridos'].min()
    
    if dias_uteis > max_dias:
        # Retorna a taxa do maior prazo disponível
        return curva[curva['dias_corridos'] == max_dias][tipo_curva].values[0]
    
    if dias_uteis < min_dias:
        # Retorna a taxa do menor prazo disponível
        return curva[curva['dias_corridos'] == min_dias][tipo_curva].values[0]
    
    # Interpolação linear
    idx = np.abs(curva['dias_corridos'] - dias_uteis).argmin()
    
    # Se encontrou exatamente o prazo
    if curva.iloc[idx]['dias_corridos'] == dias_uteis:
        return curva.iloc[idx][tipo_curva]
    
    # Interpolação entre dois pontos mais próximos
    if curva.iloc[idx]['dias_corridos'] < dias_uteis:
        idx_menor = idx
        idx_maior = min(idx + 1, len(curva) - 1)
    else:
        idx_maior = idx
        idx_menor = max(idx - 1, 0)
    
    dias_menor = curva.iloc[idx_menor]['dias_corridos']
    dias_maior = curva.iloc[idx_maior]['dias_corridos']
    taxa_menor = curva.iloc[idx_menor][tipo_curva]
    taxa_maior = curva.iloc[idx_maior][tipo_curva]
    
    if dias_maior == dias_menor:
        return taxa_menor
    
    # Interpolação linear
    taxa_interpolada = taxa_menor + (taxa_maior - taxa_menor) * (dias_uteis - dias_menor) / (dias_maior - dias_menor)
    
    return taxa_interpolada


def calcular_spread_vs_curva(taxa_titulo, duration_anos, indexador, curva_df):
    """
    Calcula o spread de um título em relação à curva ANBIMA
    
    Args:
        taxa_titulo: Taxa do título (em % a.a.)
        duration_anos: Duration em anos
        indexador: Indexador do título ('IPCA', 'CDI', 'PRÉ', etc.)
        curva_df: DataFrame da curva ANBIMA
    
    Returns:
        dict com spread em bps e taxa benchmark
    """
    if curva_df.empty or taxa_titulo is None or duration_anos is None:
        return {"spread_bps": None, "taxa_benchmark": None, "tipo_curva": None}
    
    # Converter duration para dias úteis (252 dias úteis por ano)
    dias_uteis = int(duration_anos * 252)
    
    # Determinar qual curva usar baseado no indexador
    indexador_upper = str(indexador).upper()
    
    if 'IPCA' in indexador_upper:
        tipo_curva = 'taxa_ipca'
    elif 'CDI' in indexador_upper or 'DI' in indexador_upper:
        tipo_curva = 'taxa_pre'  # Para CDI+, comparar com curva pré
    elif 'PRÉ' in indexador_upper or 'PRE' in indexador_upper:
        tipo_curva = 'taxa_pre'
    else:
        tipo_curva = 'taxa_pre'  # Default
    
    # Interpolar a taxa da curva
    taxa_benchmark = interpolar_taxa_curva(curva_df, dias_uteis, tipo_curva)
    
    if taxa_benchmark is None:
        return {"spread_bps": None, "taxa_benchmark": None, "tipo_curva": tipo_curva}
    
    # Calcular spread em basis points
    spread_bps = (taxa_titulo - taxa_benchmark) * 100
    
    return {
        "spread_bps": round(spread_bps, 2),
        "taxa_benchmark": round(taxa_benchmark, 4),
        "tipo_curva": tipo_curva
    }


def adicionar_spreads_ao_df(df, curva_df=None):
    """
    Adiciona colunas de spread ao DataFrame de debêntures
    
    Args:
        df: DataFrame de debêntures
        curva_df: DataFrame da curva ANBIMA (se None, carrega automaticamente)
    
    Returns:
        DataFrame com colunas adicionais: spread_bps, taxa_benchmark, tipo_curva
    """
    if df.empty:
        return df
    
    # Carregar curva se não foi fornecida
    if curva_df is None or curva_df.empty:
        curva_df = load_curva_anbima()
    
    if curva_df.empty:
        df['spread_bps'] = None
        df['taxa_benchmark'] = None
        df['tipo_curva'] = None
        return df
    
    # Calcular spread para cada título
    spreads = []
    for _, row in df.iterrows():
        result = calcular_spread_vs_curva(
            taxa_titulo=row.get('taxa'),
            duration_anos=row.get('duration'),
            indexador=row.get('indexador'),
            curva_df=curva_df
        )
        spreads.append(result)
    
    spreads_df = pd.DataFrame(spreads)
    
    # Adicionar colunas ao DataFrame original
    df = df.reset_index(drop=True)
    df['spread_bps'] = spreads_df['spread_bps']
    df['taxa_benchmark'] = spreads_df['taxa_benchmark']
    df['tipo_curva'] = spreads_df['tipo_curva']
    
    return df


# ============================================================================
# FUNÇÕES DE VOLUME NEGOCIADO (SND)
# ============================================================================

@st.cache_data(ttl=60)
def get_volume_dates():
    """Retorna datas disponíveis na tabela de negociação SND"""
    if not os.path.exists(DB_PATH):
        return []
    
    try:
        conn = sqlite3.connect(DB_PATH)
        # Verifica se a tabela existe
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='negociacao_snd'")
        if not cursor.fetchone():
            conn.close()
            return []
        
        df = pd.read_sql("SELECT DISTINCT data_base FROM negociacao_snd ORDER BY data_base DESC", conn)
        conn.close()
        return df['data_base'].tolist() if not df.empty else []
    except:
        return []


@st.cache_data(ttl=300)
def load_volume_data(data_base=None):
    """
    Carrega dados de volume negociado do SND
    
    Args:
        data_base: Data específica (formato YYYY-MM-DD) ou None para a mais recente
    
    Returns:
        DataFrame com dados de volume ou None
    """
    if not os.path.exists(DB_PATH):
        return None
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Verifica se a tabela existe
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='negociacao_snd'")
        if not cursor.fetchone():
            conn.close()
            return None
        
        if data_base:
            query = f"SELECT * FROM negociacao_snd WHERE data_base = '{data_base}'"
        else:
            query = """SELECT * FROM negociacao_snd 
                      WHERE data_base = (SELECT MAX(data_base) FROM negociacao_snd)"""
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Normaliza código para merge
        if not df.empty and 'codigo' in df.columns:
            df['codigo'] = df['codigo'].str.strip().str.upper()
        
        return df
    except Exception as e:
        return None


def get_volume_summary():
    """
    Retorna resumo de volume total do dia mais recente
    
    Returns:
        dict com métricas de volume ou None
    """
    df = load_volume_data()
    
    if df is None or df.empty:
        return None
    
    return {
        "data_base": df['data_base'].iloc[0] if 'data_base' in df.columns else None,
        "volume_total": df['volume_total'].sum() if 'volume_total' in df.columns else 0,
        "qtd_ativos": df['codigo'].nunique() if 'codigo' in df.columns else 0,
        "total_negocios": df['numero_negocios'].sum() if 'numero_negocios' in df.columns else 0,
        "quantidade_total": df['quantidade'].sum() if 'quantidade' in df.columns else 0
    }


def get_top_volume(n=10):
    """
    Retorna os N ativos com maior volume negociado
    
    Args:
        n: Número de ativos a retornar
    
    Returns:
        DataFrame com top ativos por volume
    """
    df = load_volume_data()
    
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Ordena por volume e pega top N
    df_sorted = df.nlargest(n, 'volume_total')
    
    cols = ['codigo', 'emissor', 'volume_total', 'quantidade', 'numero_negocios', 'pu_medio', 'data_base']
    available_cols = [c for c in cols if c in df_sorted.columns]
    
    return df_sorted[available_cols]


def adicionar_volume_ao_df(df):
    """
    Adiciona dados de volume ao DataFrame principal de debêntures
    
    Args:
        df: DataFrame de debêntures
    
    Returns:
        DataFrame com colunas adicionais de volume
    """
    if df.empty:
        df['volume_total'] = None
        df['quantidade_negociada'] = None
        df['numero_negocios'] = None
        return df
    
    # Carregar dados de volume
    df_volume = load_volume_data()
    
    if df_volume is None or df_volume.empty:
        df['volume_total'] = None
        df['quantidade_negociada'] = None
        df['numero_negocios'] = None
        return df
    
    # Preparar para merge
    df_vol_merge = df_volume[['codigo', 'volume_total', 'quantidade', 'numero_negocios']].copy()
    df_vol_merge = df_vol_merge.rename(columns={'quantidade': 'quantidade_negociada'})
    
    # Merge por código
    df = df.merge(df_vol_merge, on='codigo', how='left')
    
    return df
