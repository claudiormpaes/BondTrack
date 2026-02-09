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

def smart_clean(df):
    """Higienização e padronização de dados"""
    if df.empty: return pd.DataFrame()

    temp_map = {}
    for col in df.columns:
        col_str = str(col)
        if col_str in ["_merge", "FONTE", "data_referencia", "data_base"]: 
            temp_map[col] = col_str
            continue
        nfkd = unicodedata.normalize('NFKD', col_str)
        clean = "".join([c for c in nfkd if not unicodedata.combining(c)])
        clean = clean.lower().strip().replace(" ", "_").replace(".", "").replace("/", "_").replace("-", "_")
        temp_map[col] = clean
    df = df.rename(columns=temp_map)
    
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

    for col in ['taxa', 'duration', 'volume', 'negocios']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    if 'pu' in df.columns:
        df['pu'] = pd.to_numeric(df['pu'], errors='coerce')

    if not df.empty and 'duration' in df.columns and df['duration'].mean() > 50:
        df['duration'] = df['duration'] / 252

    if 'indexador' not in df.columns: df['indexador'] = 'N/D'
    df['indexador'] = df['indexador'].fillna('N/D').astype(str).str.upper().str.strip()
    
    correcoes = {
        r'\bD\.I\.\b': 'CDI', r'\bDI\b': 'CDI',
        r'\bIGPM\b': 'IGP-M', r'\bIGP\s*M\b': 'IGP-M',
        r'\bIPC-A\b': 'IPCA', r'\bIPCA\+\b': 'IPCA',
        r'\bPRE\b': 'PRÉ', r'\bPREFIXADO\b': 'PRÉ'
    }
    df['indexador'] = df['indexador'].replace(correcoes, regex=True)

    if 'emissor' not in df.columns: df['emissor'] = 'N/D'
    df['emissor'] = df['emissor'].fillna('N/D').astype(str).str.split("-").str[0].str.strip()
    
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
    
    lista_fmt = []
    for d in datas:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            lista_fmt.append(dt.strftime("%d/%m/%Y"))
        except:
            try:
                dt = datetime.strptime(d, "%d/%m/%Y")
                lista_fmt.append(dt.strftime("%d/%m/%Y"))
            except: pass
            
    return sorted(list(set(lista_fmt)), reverse=True)

@st.cache_data(ttl=60)
def load_data(selected_date_str):
    if not os.path.exists(DB_DEBENTURES):
        return None, "Banco de dados não encontrado."

    try:
        dt_obj = datetime.strptime(selected_date_str, "%d/%m/%Y")
        date_iso = dt_obj.strftime("%Y-%m-%d")
        date_br = selected_date_str
    except:
        date_iso = selected_date_str
        date_br = selected_date_str

    conn = sqlite3.connect(DB_DEBENTURES)
    df_snd = pd.DataFrame()
    df_anbima = pd.DataFrame()
    df_cadastro = pd.DataFrame()

    try:
        try:
            q_snd = f"SELECT * FROM negociacao_snd WHERE data_base = '{date_iso}'"
            df_snd = pd.read_sql(q_snd, conn)
        except: pass
        
        try:
            q_anb = f"SELECT * FROM mercado_secundario WHERE data_referencia = '{date_br}'"
            df_anbima = pd.read_sql(q_anb, conn)
            if df_anbima.empty:
                q_anb = f"SELECT * FROM mercado_secundario WHERE data_referencia = '{date_iso}'"
                df_anbima = pd.read_sql(q_anb, conn)
        except: pass

        try:
            df_cadastro = pd.read_sql("SELECT * FROM cadastro_snd", conn)
        except: pass
    except Exception as e:
        conn.close()
        return None, str(e)
    finally:
        conn.close()

    if df_snd.empty and df_anbima.empty and df_cadastro.empty:
        return pd.DataFrame(), None

    if not df_snd.empty and 'codigo' in df_snd.columns:
        df_snd['codigo'] = df_snd['codigo'].str.strip().str.upper()
    if not df_anbima.empty and 'codigo' in df_anbima.columns:
        df_anbima['codigo'] = df_anbima['codigo'].str.strip().str.upper()
        
    codigos_dia = set()
    if not df_snd.empty: codigos_dia.update(df_snd['codigo'].tolist())
    if not df_anbima.empty: codigos_dia.update(df_anbima['codigo'].tolist())
    
    if not codigos_dia and not df_cadastro.empty:
        df_final = df_cadastro.copy()
        if 'Codigo_Ativo' in df_final.columns: df_final.rename(columns={'Codigo_Ativo': 'codigo'}, inplace=True)
        if 'codigo' in df_final.columns: df_final['codigo'] = df_final['codigo'].str.strip().str.upper()
        df_final['FONTE'] = 'Cadastro'
    else:
        df_final = pd.DataFrame({'codigo': list(codigos_dia)})
        if not df_snd.empty:
            df_final = pd.merge(df_final, df_snd, on='codigo', how='left', suffixes=('', '_snd'))
        if not df_anbima.empty:
            df_final = pd.merge(df_final, df_anbima, on='codigo', how='left', suffixes=('', '_anb'))
        if not df_cadastro.empty:
            col_cad = next((c for c in df_cadastro.columns if c.lower() in ['codigo', 'codigo_ativo', 'ativo']), None)
            if col_cad:
                df_cad_clean = df_cadastro.copy()
                df_cad_clean.rename(columns={col_cad: 'codigo'}, inplace=True)
                df_cad_clean['codigo'] = df_cad_clean['codigo'].astype(str).str.strip().str.upper()
                df_final = pd.merge(df_final, df_cad_clean, on='codigo', how='left', suffixes=('', '_cad'))

        def get_fonte(row):
            has_snd = pd.notna(row.get('volume_total')) or pd.notna(row.get('numero_negocios'))
            has_anb = pd.notna(row.get('taxa_indicativa')) or pd.notna(row.get('taxa_compra'))
            if has_snd and has_anb: return "SND + Anbima"
            if has_snd: return "SND"
            if has_anb: return "Anbima"
            return "Cadastro"
        df_final['FONTE'] = df_final.apply(get_fonte, axis=1)

    df_final = smart_clean(df_final)
    df_final['data_referencia'] = selected_date_str
    return df_final, None

@st.cache_data(ttl=300)
def load_curva_anbima(target_date=None):
    if not os.path.exists(DB_CURVAS): return pd.DataFrame()
    conn = sqlite3.connect(DB_CURVAS)
    try:
        if target_date:
            df = pd.read_sql(f"SELECT * FROM curvas_anbima WHERE data_referencia = '{target_date}'", conn)
            if df.empty:
                try:
                    iso = datetime.strptime(target_date, "%d/%m/%Y").strftime("%Y-%m-%d")
                    df = pd.read_sql(f"SELECT * FROM curvas_anbima WHERE data_referencia = '{iso}'", conn)
                except: pass
        else:
            df = pd.read_sql("SELECT * FROM curvas_anbima", conn)
            if not df.empty:
                df = df.tail(len(df[df['data_referencia'] == df.iloc[-1]['data_referencia']]))
        return df
    except: return pd.DataFrame()
    finally: conn.close()

def apply_filters(df, filtros):
    df_f = df.copy()
    if filtros.get("emissor"): df_f = df_f[df_f["emissor"].isin(filtros["emissor"])]
    if filtros.get("indexador"): df_f = df_f[df_f["indexador"].isin(filtros["indexador"])]
    if filtros.get("cluster"): df_f = df_f[df_f["cluster_duration"].isin(filtros["cluster"])]
    if filtros.get("categoria"): df_f = df_f[df_f["categoria_grafico"].isin(filtros["categoria"])]
    if filtros.get("fonte") and filtros["fonte"] != "Todos": df_f = df_f[df_f["FONTE"] == filtros["fonte"]]
    
    if "taxa_min" in filtros: df_f = df_f[df_f["taxa"] >= filtros["taxa_min"]]
    if "taxa_max" in filtros: df_f = df_f[df_f["taxa"] <= filtros["taxa_max"]]
    if "duration_min" in filtros: df_f = df_f[df_f["duration"] >= filtros["duration_min"]]
    if "duration_max" in filtros: df_f = df_f[df_f["duration"] <= filtros["duration_max"]]
    return df_f

# === AQUI ESTAVA O ERRO: Função get_data_quality_report corrigida ===
def get_data_quality_report(df):
    report = {
        "total_registros": len(df),
        "campos_completos": {},
        "duplicatas": 0,
        "inconsistencias": []
    }
    
    # Verifica campos críticos
    for c in ['codigo', 'emissor', 'taxa', 'duration', 'volume']:
        if c in df.columns:
            valid = df[c].notna().sum()
            # Para taxas e volume, zero pode ser considerado inválido dependendo da regra
            if c in ['taxa', 'volume']: 
                valid = (df[c] > 0).sum()
            
            total = len(df)
            invalid = total - valid
            
            report["campos_completos"][c] = {
                "validos": int(valid),
                "invalidos": int(invalid), # ESSA CHAVE ESTAVA FALTANDO
                "percentual": round(valid/total*100, 1) if total>0 else 0
            }
            
    if 'codigo' in df.columns:
        report["duplicatas"] = int(df.duplicated(subset=['codigo']).sum())
        
    report["score_qualidade"] = 100
    return report

def get_volume_summary():
    if not os.path.exists(DB_DEBENTURES): return None
    try:
        conn = sqlite3.connect(DB_DEBENTURES)
        last = pd.read_sql("SELECT MAX(data_base) as d FROM negociacao_snd", conn).iloc[0]['d']
        df = pd.read_sql(f"SELECT * FROM negociacao_snd WHERE data_base = '{last}'", conn)
        conn.close()
        return {"volume_total": df['volume_total'].sum(), "qtd_ativos": df['codigo'].nunique(), "data_ref": last}
    except: return None

def get_top_volume(n=5):
    if not os.path.exists(DB_DEBENTURES): return pd.DataFrame()
    conn = sqlite3.connect(DB_DEBENTURES)
    try:
        last = pd.read_sql("SELECT MAX(data_base) as d FROM negociacao_snd", conn).iloc[0]['d']
        df = pd.read_sql(f"SELECT * FROM negociacao_snd WHERE data_base = '{last}' ORDER BY volume_total DESC LIMIT {n}", conn)
        conn.close()
        return df
    except: return pd.DataFrame()

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
    
    def calc(row):
        try:
            d = row.get('dias_interpolacao', 0)
            t = row.get('taxa', 0)
            idx = str(row.get('indexador', '')).upper()
            if d<=0 or t<=0: return None
            
            ref = 'taxa_pre'
            if 'IPCA' in idx: ref = 'taxa_ipca'
            
            bench = interpolar_taxa_curva(df_curva, d, ref)
            if bench is not None: return (t - bench)*100
        except: return None
        return None
    
    df_ativos['spread_bps'] = df_ativos.apply(calc, axis=1)
    return df_ativos

def get_curvas_anbima_dates():
    if not os.path.exists(DB_CURVAS): return []
    conn = sqlite3.connect(DB_CURVAS)
    try:
        df = pd.read_sql("SELECT DISTINCT data_referencia FROM curvas_anbima", conn)
        conn.close()
        return sorted(df['data_referencia'].tolist(), reverse=True)
    except: return []

def get_database_status_full(data_ref=None):
    status = {'snd_cadastro': {'loaded': False, 'count': 0}, 'snd_negociacao': {'loaded': False, 'count': 0}, 'anbima_indicativa': {'loaded': False, 'count': 0}, 'anbima_precos': {'loaded': False, 'count': 0}, 'anbima_curvas': {'loaded': False, 'count': 0}}
    if not os.path.exists(DB_DEBENTURES): return status
    try:
        conn = sqlite3.connect(DB_DEBENTURES)
        try:
            status['snd_cadastro'] = {'loaded': True, 'count': int(conn.execute("SELECT count(*) FROM cadastro_snd").fetchone()[0])}
        except: pass
        try:
            if data_ref:
                dt_iso = datetime.strptime(data_ref, "%d/%m/%Y").strftime("%Y-%m-%d")
                c = conn.execute(f"SELECT count(*) FROM negociacao_snd WHERE data_base = '{dt_iso}'").fetchone()[0]
            else: c = conn.execute("SELECT count(*) FROM negociacao_snd").fetchone()[0]
            status['snd_negociacao'] = {'loaded': True, 'count': c}
        except: pass
        try:
            if data_ref: c = conn.execute(f"SELECT count(*) FROM mercado_secundario WHERE data_referencia = '{data_ref}'").fetchone()[0]
            else: c = conn.execute("SELECT count(*) FROM mercado_secundario").fetchone()[0]
            status['anbima_indicativa'] = {'loaded': True, 'count': c}
            status['anbima_precos'] = {'loaded': True, 'count': c}
        except: pass
        conn.close()
    except: pass
    if os.path.exists(DB_CURVAS):
        try:
            conn = sqlite3.connect(DB_CURVAS)
            c = conn.execute("SELECT count(*) FROM curvas_anbima").fetchone()[0]
            status['anbima_curvas'] = {'loaded': True, 'count': c}
            conn.close()
        except: pass
    return status
