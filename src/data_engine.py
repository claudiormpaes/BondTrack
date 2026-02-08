"""Motor de Dados BondTrack - ETL, Merge SND+Anbima, Limpeza, Curvas ANBIMA"""
import pandas as pd
import numpy as np
import sqlite3
import os
import unicodedata
import streamlit as st
from datetime import datetime

# Caminhos dos bancos de dados
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_DEBENTURES = os.path.join(DATA_DIR, "debentures_anbima.db")
DB_CURVAS = os.path.join(DATA_DIR, "curvas_anbima.db")

# Mantemos DB_PATH para compatibilidade com funções antigas se houver
DB_PATH = DB_DEBENTURES 

def smart_clean(df):
    """
    Higienização e padronização de dados.
    CRÍTICO: Não altere a lógica de mapeamento pois o App depende desses nomes.
    """
    if df.empty: 
        return pd.DataFrame()

    # 1. Normalização de nomes de colunas
    temp_map = {}
    for col in df.columns:
        col_str = str(col)
        if col_str in ["_merge", "FONTE"]: # Preserva colunas de sistema
            temp_map[col] = col_str
            continue
            
        nfkd = unicodedata.normalize('NFKD', col_str)
        clean = "".join([c for c in nfkd if not unicodedata.combining(c)])
        clean = clean.lower().strip().replace(" ", "_").replace(".", "").replace("/", "_").replace("-", "_")
        temp_map[col] = clean
    df = df.rename(columns=temp_map)
    
    # 2. Mapeamento de Colunas (Keywords)
    keywords = {
        "taxa": ["taxa_indicativa", "taxa_emissao", "taxa_compra", "taxa", "taxa_media"],
        "duration": ["duration", "duracao", "du"],
        "pu": ["pu_medio", "pu", "preco", "unitario", "pu_teorico"], # Prioriza PU Médio do SND se existir
        "indexador": ["indexador", "indice", "idx"],
        "emissor": ["emissor", "nome_emissor", "razao_social", "empresa", "nome"],
        "codigo": ["codigo", "ativo", "ticker"],
        "incentivada": ["deb_incent", "incentivada", "lei_12431", "isenta", "ir"],
        "volume": ["volume_total", "volume", "vol"],
        "negocios": ["numero_negocios", "negocios"]
    }
    
    final_map = {}
    for padrao, lista in keywords.items():
        if padrao in df.columns: continue
        for col in df.columns:
            if any(p in col for p in lista) and padrao not in final_map.values():
                final_map[col] = padrao
                break
    df = df.rename(columns=final_map)

    # 3. Tratamento Numérico Seguro
    for col in ['taxa', 'duration', 'volume', 'negocios']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    if 'pu' in df.columns:
        df['pu'] = pd.to_numeric(df['pu'], errors='coerce') # Mantém NaN se vazio
    
    # Converter duration de dias úteis para anos se necessário (Heurística > 50)
    if not df.empty and 'duration' in df.columns and df['duration'].mean() > 50:
        df['duration'] = df['duration'] / 252

    # 4. HIGIENIZAÇÃO DE INDEXADORES
    if 'indexador' not in df.columns: df['indexador'] = 'N/D'
    df['indexador'] = df['indexador'].fillna('N/D').astype(str).str.upper().str.strip()
    
    correcoes = {
        r'\bD\.I\.\b': 'CDI', r'\bDI\b': 'CDI',
        r'\bIGPM\b': 'IGP-M', r'\bIGP\s*M\b': 'IGP-M',
        r'\bIPC-A\b': 'IPCA', r'\bIPCA\+\b': 'IPCA',
        r'\bPRE\b': 'PRÉ', r'\bPREFIXADO\b': 'PRÉ'
    }
    df['indexador'] = df['indexador'].replace(correcoes, regex=True)

    # Limpeza de Emissor
    if 'emissor' not in df.columns: df['emissor'] = 'N/D'
    df['emissor'] = df['emissor'].fillna('N/D').astype(str).str.split("-").str[0].str.strip()
    
    # 5. Categorização para Gráficos
    def classificar(row):
        idx = row.get("indexador", "N/D")
        taxa = row.get("taxa", 0)
        incent_val = str(row.get("incentivada", "")).upper()
        is_incentivada = any(x in incent_val for x in ['S', 'SIM', 'YES', 'TRUE', '1'])

        if "IPCA" in idx: return "IPCA Incentivado" if is_incentivada else "IPCA Não Incentivado"
        if "CDI" in idx: return "% CDI" if taxa > 30 else "CDI +"
        if "PRÉ" in idx: return "Prefixado"
        return "Outros"

    df["categoria_grafico"] = df.apply(classificar, axis=1)
    
    # Cluster Duration
    def cluster_dur(d):
        if d <= 0: return "Sem Prazo"
        if d <= 1: return "0-1 ano"
        if d <= 3: return "1-3 anos"
        if d <= 5: return "3-5 anos"
        if d <= 10: return "5-10 anos"
        return "10+ anos"
    
    if 'duration' in df.columns:
        df["cluster_duration"] = df["duration"].apply(cluster_dur)
    else:
        df["cluster_duration"] = "N/D"

    return df

@st.cache_data(ttl=60)
def get_available_dates():
    """
    Retorna datas disponíveis combinando TODAS as tabelas.
    Se tem data no SND ou na Anbima ou na Curva, ela aparece.
    """
    datas = set()
    
    # 1. Datas de Negociação (SND)
    if os.path.exists(DB_DEBENTURES):
        try:
            conn = sqlite3.connect(DB_DEBENTURES)
            # Tenta SND
            try:
                df = pd.read_sql("SELECT DISTINCT data_base FROM negociacao_snd", conn)
                datas.update(df['data_base'].dropna().tolist())
            except: pass
            # Tenta Anbima
            try:
                df = pd.read_sql("SELECT DISTINCT data_referencia FROM mercado_secundario", conn)
                datas.update(df['data_referencia'].dropna().tolist())
            except: pass
            conn.close()
        except: pass

    # 2. Datas de Curvas
    if os.path.exists(DB_CURVAS):
        try:
            conn = sqlite3.connect(DB_CURVAS)
            df = pd.read_sql("SELECT DISTINCT data_referencia FROM curvas_anbima", conn)
            datas.update(df['data_referencia'].dropna().tolist())
            conn.close()
        except: pass
            
    # Padroniza formato para DD/MM/YYYY para o dropdown
    lista_formatada = []
    for d in datas:
        try:
            # Tenta converter se for YYYY-MM-DD
            dt = datetime.strptime(d, "%Y-%m-%d")
            lista_formatada.append(dt.strftime("%d/%m/%Y"))
        except:
            try:
                # Tenta converter se já for DD/MM/YYYY
                dt = datetime.strptime(d, "%d/%m/%Y")
                lista_formatada.append(dt.strftime("%d/%m/%Y"))
            except: pass
            
    return sorted(list(set(lista_formatada)), reverse=True)

@st.cache_data(ttl=300)
def load_data(data_selecionada=None):
    """
    Carrega dados de forma resiliente.
    Se faltar Anbima, carrega SND. Se faltar SND, carrega Anbima.
    """
    if not os.path.exists(DB_DEBENTURES): 
        return None, "Banco de dados não encontrado"
    
    # Converte data selecionada (DD/MM/YYYY) para formatos do banco
    date_iso = None # YYYY-MM-DD
    date_br = data_selecionada # DD/MM/YYYY
    
    if data_selecionada:
        try:
            dt_obj = datetime.strptime(data_selecionada, "%d/%m/%Y")
            date_iso = dt_obj.strftime("%Y-%m-%d")
        except:
            date_iso = data_selecionada # Fallback
            
    conn = sqlite3.connect(DB_DEBENTURES)
    
    df_snd = pd.DataFrame()
    df_anbima = pd.DataFrame()
    df_cadastro = pd.DataFrame()
    
    try:
        # 1. Carrega SND (Negociação) - Usa ISO YYYY-MM-DD
        try:
            q = f"SELECT * FROM negociacao_snd WHERE data_base = '{date_iso}'"
            df_snd = pd.read_sql(q, conn)
        except: pass
        
        # 2. Carrega Anbima (Mercado Secundário) - Usa BR DD/MM/YYYY (geralmente)
        try:
            # Tenta com os dois formatos por garantia
            q = f"SELECT * FROM mercado_secundario WHERE data_referencia = '{date_br}' OR data_referencia = '{date_iso}'"
            df_anbima = pd.read_sql(q, conn)
        except: pass
        
        # 3. Carrega Cadastro
        try:
            df_cadastro = pd.read_sql("SELECT * FROM cadastro_snd", conn)
        except: pass
        
    except Exception as e:
        conn.close()
        return None, f"Erro SQL: {e}"
    finally:
        conn.close()
        
    # Se não tem nada de negociação/preço em nenhum dos dois, retorna vazio
    if df_snd.empty and df_anbima.empty:
        return pd.DataFrame(), None # Data existe na lista (curva), mas sem ativos
        
    # --- PREPARAÇÃO PARA MERGE ---
    # Normaliza código para chave de junção
    if not df_snd.empty and 'codigo' in df_snd.columns:
        df_snd['codigo'] = df_snd['codigo'].str.strip().str.upper()
        
    if not df_anbima.empty and 'codigo' in df_anbima.columns:
        df_anbima['codigo'] = df_anbima['codigo'].str.strip().str.upper()
        
    # Normaliza cadastro
    if not df_cadastro.empty:
        # Procura coluna de código
        col_cod = next((c for c in df_cadastro.columns if c.lower() in ['codigo', 'codigo_ativo', 'ativo']), None)
        if col_cod:
            df_cadastro = df_cadastro.rename(columns={col_cod: 'codigo'})
            df_cadastro['codigo'] = df_cadastro['codigo'].astype(str).str.strip().str.upper()

    # --- MERGE ROBUSTO ---
    # Começa com todos os códigos únicos encontrados
    codigos = set()
    if not df_snd.empty: codigos.update(df_snd['codigo'].tolist())
    if not df_anbima.empty: codigos.update(df_anbima['codigo'].tolist())
    
    df_final = pd.DataFrame({'codigo': list(codigos)})
    
    # Junta SND (Volume, PU Real)
    if not df_snd.empty:
        df_final = pd.merge(df_final, df_snd, on='codigo', how='left', suffixes=('', '_snd'))
        
    # Junta Anbima (Taxa, Duration, PU Teórico)
    if not df_anbima.empty:
        df_final = pd.merge(df_final, df_anbima, on='codigo', how='left', suffixes=('', '_anbima'))
        
    # Junta Cadastro (Emissor, Indexador)
    if not df_cadastro.empty:
        df_final = pd.merge(df_final, df_cadastro, on='codigo', how='left', suffixes=('', '_cad'))

    # Define Fonte
    def get_fonte(row):
        has_snd = pd.notna(row.get('volume_total')) or pd.notna(row.get('numero_negocios'))
        has_anbima = pd.notna(row.get('taxa_indicativa')) or pd.notna(row.get('taxa_compra')) # Exemplo
        
        if has_snd and has_anbima: return "SND + Anbima"
        if has_snd: return "SND (Negócios)"
        if has_anbima: return "Anbima (Teórico)"
        return "Cadastro"
        
    df_final['FONTE'] = df_final.apply(get_fonte, axis=1)
    
    # Aplica Limpeza Inteligente (Transforma colunas bagunçadas em padrão)
    df_limpo = smart_clean(df_final)
    
    # Garante data_referencia
    if 'data_referencia' not in df_limpo.columns:
        df_limpo['data_referencia'] = date_br

    return df_limpo, None

def apply_filters(df, filtros):
    """Aplica filtros ao DataFrame"""
    df_f = df.copy()
    if filtros.get("emissor"): df_f = df_f[df_f["emissor"].isin(filtros["emissor"])]
    if filtros.get("indexador"): df_f = df_f[df_f["indexador"].isin(filtros["indexador"])]
    if filtros.get("cluster"): df_f = df_f[df_f["cluster_duration"].isin(filtros["cluster"])]
    if filtros.get("categoria"): df_f = df_f[df_f["categoria_grafico"].isin(filtros["categoria"])]
    return df_f

def get_data_quality_report(df):
    """Relatório simples de qualidade"""
    return {
        "total": len(df),
        "com_preco": len(df[df['pu'] > 0]) if 'pu' in df.columns else 0,
        "com_volume": len(df[df['volume'] > 0]) if 'volume' in df.columns else 0
    }

# ============================================================================
# FUNÇÕES DA CURVA DE JUROS (MANTIDAS IGUAIS)
# ============================================================================

@st.cache_data(ttl=60)
def get_curvas_anbima_dates():
    if not os.path.exists(CURVAS_DB_PATH): return []
    try:
        conn = sqlite3.connect(CURVAS_DB_PATH)
        df = pd.read_sql("SELECT DISTINCT data_referencia FROM curvas_anbima", conn)
        conn.close()
        # Ordenação robusta
        try:
            df['dt'] = pd.to_datetime(df['data_referencia'], dayfirst=True)
            return df.sort_values('dt', ascending=False)['data_referencia'].tolist()
        except:
            return df['data_referencia'].tolist()
    except: return []

@st.cache_data(ttl=300)
def load_curva_anbima(data_referencia=None):
    if not os.path.exists(CURVAS_DB_PATH): return pd.DataFrame()
    try:
        conn = sqlite3.connect(CURVAS_DB_PATH)
        if data_referencia:
            # Tenta direto DD/MM/YYYY
            df = pd.read_sql(f"SELECT * FROM curvas_anbima WHERE data_referencia = '{data_referencia}'", conn)
            # Se falhar, tenta YYYY-MM-DD
            if df.empty:
                try:
                    iso = datetime.strptime(data_referencia, "%d/%m/%Y").strftime("%Y-%m-%d")
                    df = pd.read_sql(f"SELECT * FROM curvas_anbima WHERE data_referencia = '{iso}'", conn)
                except: pass
        else:
            df = pd.read_sql("SELECT * FROM curvas_anbima", conn)
            # Pega max (precisa converter data)
            # Simplificando: retorna tudo e o app filtra ou pega ultimo
            # Melhor: SQL max
            # Como formato é texto, MAX pode falhar se DD/MM/YYYY. Python handle:
            pass 
        
        conn.close()
        return df
    except: return pd.DataFrame()

def interpolar_taxa_curva(curva_df, dias_uteis, tipo_curva='taxa_ipca'):
    if curva_df.empty or tipo_curva not in curva_df.columns: return None
    if 'dias_corridos' not in curva_df.columns: return None
    
    curva = curva_df.sort_values('dias_corridos')
    try:
        return np.interp(dias_uteis, curva['dias_corridos'], curva[tipo_curva])
    except: return None

def adicionar_spreads_ao_df(df, curva_df=None):
    if df.empty: return df
    if curva_df is None or curva_df.empty: 
        curva_df = load_curva_anbima()
    if curva_df.empty: return df
    
    # Lógica de cálculo (resumida para manter concisão mas funcional)
    # Assume que duration está em ANOS
    if 'duration' not in df.columns: return df
    
    def calc(row):
        dur = row.get('duration', 0)
        idx = str(row.get('indexador', '')).upper()
        taxa = row.get('taxa', 0)
        
        if dur <= 0 or taxa <= 0: return None
        dias = dur * 252
        
        col_curva = 'taxa_pre'
        if 'IPCA' in idx: col_curva = 'taxa_ipca'
        
        bench = interpolar_taxa_curva(curva_df, dias, col_curva)
        if bench: return (taxa - bench) * 100
        return None
        
    df['spread_bps'] = df.apply(calc, axis=1)
    return df

# ============================================================================
# FUNÇÕES DE VOLUME (APONTANDO PARA NEGOCIACAO_SND)
# ============================================================================

def get_volume_summary():
    # Usa load_data que já traz volume se existir
    # Ou lê direto do banco para performance
    if not os.path.exists(DB_DEBENTURES): return None
    try:
        conn = sqlite3.connect(DB_DEBENTURES)
        # Pega ultimo dia
        last = pd.read_sql("SELECT MAX(data_base) as d FROM negociacao_snd", conn).iloc[0]['d']
        df = pd.read_sql(f"SELECT * FROM negociacao_snd WHERE data_base = '{last}'", conn)
        conn.close()
        return {
            "volume_total": df['volume_total'].sum(),
            "qtd_ativos": df['codigo'].nunique(),
            "total_negocios": df['numero_negocios'].sum()
        }
    except: return None

def get_top_volume(n=10):
    if not os.path.exists(DB_DEBENTURES): return pd.DataFrame()
    try:
        conn = sqlite3.connect(DB_DEBENTURES)
        last = pd.read_sql("SELECT MAX(data_base) as d FROM negociacao_snd", conn).iloc[0]['d']
        df = pd.read_sql(f"SELECT * FROM negociacao_snd WHERE data_base = '{last}' ORDER BY volume_total DESC LIMIT {n}", conn)
        conn.close()
        return df
    except: return pd.DataFrame()
