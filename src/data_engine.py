"""
Motor de Dados BondTrack - ETL, Merge SND+Anbima, Limpeza, Curvas ANBIMA
Arquivo central de inteligência de dados.
"""
import pandas as pd
import numpy as np
import sqlite3
import os
import unicodedata
import streamlit as st
from datetime import datetime

# --- CONFIGURAÇÃO DE CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_DEBENTURES = os.path.join(DATA_DIR, "debentures_anbima.db")
DB_CURVAS = os.path.join(DATA_DIR, "curvas_anbima.db")

# Mantemos DB_PATH para compatibilidade
DB_PATH = DB_DEBENTURES 

def smart_clean(df):
    """
    Higienização e padronização de dados.
    """
    if df.empty: 
        return pd.DataFrame()

    # 1. Normalização de nomes de colunas
    temp_map = {}
    for col in df.columns:
        col_str = str(col)
        if col_str in ["_merge", "FONTE"]: 
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
        "pu": ["pu_medio", "pu", "preco", "unitario", "pu_teorico"],
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

    # 3. Tratamento Numérico
    for col in ['taxa', 'duration', 'volume', 'negocios']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    if 'pu' in df.columns:
        df['pu'] = pd.to_numeric(df['pu'], errors='coerce')
    
    # Converter duration de dias úteis para anos se necessário
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
    
    # 5. Categorização
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
    """Retorna datas disponíveis combinando todas as tabelas"""
    datas = set()
    
    if os.path.exists(DB_DEBENTURES):
        try:
            conn = sqlite3.connect(DB_DEBENTURES)
            try:
                df = pd.read_sql("SELECT DISTINCT data_base FROM negociacao_snd", conn)
                datas.update(df['data_base'].dropna().tolist())
            except: pass
            try:
                df = pd.read_sql("SELECT DISTINCT data_referencia FROM mercado_secundario", conn)
                datas.update(df['data_referencia'].dropna().tolist())
            except: pass
            conn.close()
        except: pass

    if os.path.exists(DB_CURVAS):
        try:
            conn = sqlite3.connect(DB_CURVAS)
            df = pd.read_sql("SELECT DISTINCT data_referencia FROM curvas_anbima", conn)
            datas.update(df['data_referencia'].dropna().tolist())
            conn.close()
        except: pass
            
    lista_formatada = []
    for d in datas:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            lista_formatada.append(dt.strftime("%d/%m/%Y"))
        except:
            try:
                dt = datetime.strptime(d, "%d/%m/%Y")
                lista_formatada.append(dt.strftime("%d/%m/%Y"))
            except: pass
            
    return sorted(list(set(lista_formatada)), reverse=True)

@st.cache_data(ttl=300)
def load_curva_anbima(target_date=None):
    """Carrega a curva de juros"""
    if not os.path.exists(DB_CURVAS):
        return pd.DataFrame()
        
    conn = sqlite3.connect(DB_CURVAS)
    try:
        if target_date:
            query = f"SELECT * FROM curvas_anbima WHERE data_referencia = '{target_date}'"
            df = pd.read_sql(query, conn)
            
            if df.empty:
                try:
                    dt_obj = datetime.strptime(target_date, "%d/%m/%Y")
                    dt_iso = dt_obj.strftime("%Y-%m-%d")
                    df = pd.read_sql(f"SELECT * FROM curvas_anbima WHERE data_referencia = '{dt_iso}'", conn)
                except: pass
        else:
            df = pd.read_sql("SELECT * FROM curvas_anbima", conn)
            # Tenta pegar a mais recente
            if not df.empty and 'data_referencia' in df.columns:
                try:
                    df['dt_temp'] = pd.to_datetime(df['data_referencia'], dayfirst=True)
                    last_date = df.sort_values('dt_temp', ascending=False)['data_referencia'].iloc[0]
                    df = df[df['data_referencia'] == last_date].drop(columns=['dt_temp'])
                except: pass
        return df
    except: return pd.DataFrame()
    finally: conn.close()

@st.cache_data(ttl=60)
def load_data(selected_date_str):
    """Carrega dados principais unindo Preço + Cadastro"""
    if not os.path.exists(DB_DEBENTURES):
        return None, "Banco de dados não encontrado."

    try:
        dt_obj = datetime.strptime(selected_date_str, "%d/%m/%Y")
        date_iso = dt_obj.strftime("%Y-%m-%d")
    except:
        date_iso = selected_date_str

    conn = sqlite3.connect(DB_DEBENTURES)
    df_precos = pd.DataFrame()
    df_cadastro = pd.DataFrame()

    try:
        # Tenta carregar preço (SND ou Anbima)
        try:
            df_precos = pd.read_sql(f"SELECT * FROM negociacao_snd WHERE data_base = '{date_iso}'", conn)
        except: pass
        
        if df_precos.empty:
             try:
                 df_precos = pd.read_sql(f"SELECT * FROM mercado_secundario WHERE data_referencia = '{selected_date_str}'", conn)
             except: pass
        
        try:
            df_cadastro = pd.read_sql("SELECT * FROM cadastro_snd", conn)
        except: pass

        conn.close()

        if df_precos.empty and df_cadastro.empty:
            return pd.DataFrame(), None

        if df_precos.empty and not df_cadastro.empty:
             df_final = df_cadastro
        else:
            # Merge
            if 'codigo' in df_precos.columns:
                df_precos['codigo_join'] = df_precos['codigo'].astype(str).str.strip().str.upper()
            
            col_cad_codigo = None
            if not df_cadastro.empty:
                possiveis = ['Codigo_Ativo', 'codigo', 'Código', 'Ativo']
                for c in possiveis:
                    for col_real in df_cadastro.columns:
                        if col_real.lower() == c.lower():
                            col_cad_codigo = col_real
                            break
                    if col_cad_codigo: break
            
            if not df_cadastro.empty and col_cad_codigo:
                df_cadastro['codigo_join'] = df_cadastro[col_cad_codigo].astype(str).str.strip().str.upper()
                df_final = pd.merge(df_precos, df_cadastro, on='codigo_join', how='left', suffixes=('', '_cad'))
            else:
                df_final = df_precos

        df_final['FONTE'] = 'SND + Anbima'
        df_final = smart_clean(df_final)
        
        if 'data_referencia' not in df_final.columns:
            df_final['data_referencia'] = selected_date_str
            
        return df_final, None

    except Exception as e:
        return None, str(e)

def apply_filters(df, filtros):
    """
    Aplica filtros ao DataFrame
    """
    df_f = df.copy()
    
    if filtros.get("emissor"): 
        df_f = df_f[df_f["emissor"].isin(filtros["emissor"])]
        
    if filtros.get("indexador"): 
        df_f = df_f[df_f["indexador"].isin(filtros["indexador"])]
        
    if filtros.get("cluster"): 
        df_f = df_f[df_f["cluster_duration"].isin(filtros["cluster"])]
        
    if filtros.get("categoria"): 
        df_f = df_f[df_f["categoria_grafico"].isin(filtros["categoria"])]
        
    if filtros.get("fonte") and filtros["fonte"] != "Todos":
        df_f = df_f[df_f["FONTE"] == filtros["fonte"]]
        
    # Filtros Numéricos
    if "taxa_min" in filtros: df_f = df_f[df_f["taxa"] >= filtros["taxa_min"]]
    if "taxa_max" in filtros: df_f = df_f[df_f["taxa"] <= filtros["taxa_max"]]
    if "duration_min" in filtros: df_f = df_f[df_f["duration"] >= filtros["duration_min"]]
    if "duration_max" in filtros: df_f = df_f[df_f["duration"] <= filtros["duration_max"]]
        
    return df_f

# --- FUNÇÃO RESTAURADA PARA AUDITORIA ---
def get_data_quality_report(df):
    """Gera relatório de qualidade dos dados"""
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

# --- AUXILIARES ---
def get_volume_summary(): return None

def get_top_volume(n=5):
    if not os.path.exists(DB_DEBENTURES): return pd.DataFrame()
    conn = sqlite3.connect(DB_DEBENTURES)
    try:
        last_date = pd.read_sql("SELECT MAX(data_base) as dt FROM negociacao_snd", conn).iloc[0]['dt']
        query = f"SELECT * FROM negociacao_snd WHERE data_base = '{last_date}' ORDER BY volume_total DESC LIMIT {n}"
        df = pd.read_sql(query, conn)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

def interpolar_taxa_curva(df_curva, dias, coluna_taxa):
    if df_curva.empty or coluna_taxa not in df_curva.columns: return None
    try:
        df_c = df_curva[['dias_corridos', coluna_taxa]].dropna().sort_values('dias_corridos')
        import numpy as np
        return np.interp(dias, df_c['dias_corridos'], df_c[coluna_taxa])
    except: return None

def adicionar_spreads_ao_df(df_ativos, df_curva):
    if df_ativos.empty or df_curva.empty or 'duration' not in df_ativos.columns: return df_ativos
    df_ativos['dias_interpolacao'] = df_ativos['duration'] * 252
    
    def calc_spread(row):
        try:
            dias = row.get('dias_interpolacao', 0)
            taxa = row.get('taxa', 0)
            idx = str(row.get('indexador', '')).upper()
            if dias <= 0 or taxa <= 0: return None
            
            if 'IPCA' in idx: taxa_livre = interpolar_taxa_curva(df_curva, dias, 'taxa_ipca')
            elif 'PRE' in idx: taxa_livre = interpolar_taxa_curva(df_curva, dias, 'taxa_pre')
            else: return None
                
            if taxa_livre: return (taxa - taxa_livre) * 100
        except: return None
    
    df_ativos['spread_bps'] = df_ativos.apply(calc_spread, axis=1)
    return df_ativos

def get_curvas_anbima_dates():
    if not os.path.exists(DB_CURVAS): return []
    conn = sqlite3.connect(DB_CURVAS)
    try:
        df = pd.read_sql("SELECT DISTINCT data_referencia FROM curvas_anbima", conn)
        return sorted(df['data_referencia'].tolist(), reverse=True)
    except: return []
    finally: conn.close()
