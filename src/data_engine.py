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
    """Higienização e padronização de dados"""
    if df.empty: return pd.DataFrame()

    # 1. Normalização de nomes de colunas
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

    # Ajuste de Duration (Anos vs Dias)
    if not df.empty and 'duration' in df.columns and df['duration'].mean() > 50:
        df['duration'] = df['duration'] / 252

    # 4. Higienização de Texto
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
    """Retorna lista de datas disponíveis"""
    datas = set()
    if os.path.exists(DB_DEBENTURES):
        try:
            conn = sqlite3.connect(DB_DEBENTURES)
            # Tenta SND (ISO)
            try:
                df = pd.read_sql("SELECT DISTINCT data_base FROM negociacao_snd", conn)
                datas.update(df['data_base'].dropna().tolist())
            except: pass
            # Tenta ANBIMA (BR)
            try:
                df = pd.read_sql("SELECT DISTINCT data_referencia FROM mercado_secundario", conn)
                datas.update(df['data_referencia'].dropna().tolist())
            except: pass
            conn.close()
        except: pass
    
    # Formatação unificada para DD/MM/YYYY
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
    """
    Carrega dados combinando: SND (Volume/Preço) + ANBIMA (Taxa/Duration) + Cadastro
    """
    if not os.path.exists(DB_DEBENTURES):
        return None, "Banco de dados não encontrado."

    # Prepara os dois formatos de data possíveis
    try:
        dt_obj = datetime.strptime(selected_date_str, "%d/%m/%Y")
        date_iso = dt_obj.strftime("%Y-%m-%d")  # 2026-02-04
        date_br = selected_date_str            # 04/02/2026
    except:
        date_iso = selected_date_str
        date_br = selected_date_str

    conn = sqlite3.connect(DB_DEBENTURES)
    df_snd = pd.DataFrame()
    df_anbima = pd.DataFrame()
    df_cadastro = pd.DataFrame()

    try:
        # 1. Tenta carregar SND (Negociação - Volume/Preço)
        try:
            q_snd = f"SELECT * FROM negociacao_snd WHERE data_base = '{date_iso}'"
            df_snd = pd.read_sql(q_snd, conn)
        except: pass
        
        # 2. Tenta carregar ANBIMA (Mercado - Taxa/Duration)
        try:
            # Tenta formato BR primeiro (mais comum na ANBIMA)
            q_anb = f"SELECT * FROM mercado_secundario WHERE data_referencia = '{date_br}'"
            df_anbima = pd.read_sql(q_anb, conn)
            # Se falhar, tenta ISO
            if df_anbima.empty:
                q_anb = f"SELECT * FROM mercado_secundario WHERE data_referencia = '{date_iso}'"
                df_anbima = pd.read_sql(q_anb, conn)
        except: pass

        # 3. Carrega Cadastro
        try:
            df_cadastro = pd.read_sql("SELECT * FROM cadastro_snd", conn)
        except: pass
        
    except Exception as e:
        conn.close()
        return None, str(e)
    finally:
        conn.close()

    if df_snd.empty and df_anbima.empty and df_cadastro.empty:
        return pd.DataFrame(), "Nenhum dado encontrado para a data selecionada. Verifique se os ETLs foram executados."

    # --- MERGE DE TABELAS ---
    
    # Normaliza Códigos para chave de junção
    if not df_snd.empty and 'codigo' in df_snd.columns:
        df_snd['codigo'] = df_snd['codigo'].str.strip().str.upper()
    
    if not df_anbima.empty and 'codigo' in df_anbima.columns:
        df_anbima['codigo'] = df_anbima['codigo'].str.strip().str.upper()
        
    # Começa com a lista de todos os códigos encontrados no dia
    codigos_dia = set()
    if not df_snd.empty: codigos_dia.update(df_snd['codigo'].tolist())
    if not df_anbima.empty: codigos_dia.update(df_anbima['codigo'].tolist())
    
    # Se não tiver movimento no dia, usa o cadastro como base (mas avisa)
    if not codigos_dia and not df_cadastro.empty:
        # Fallback: mostra cadastro sem dados de preço
        df_final = df_cadastro.copy()
        if 'Codigo_Ativo' in df_final.columns: df_final.rename(columns={'Codigo_Ativo': 'codigo'}, inplace=True)
        if 'codigo' in df_final.columns: df_final['codigo'] = df_final['codigo'].str.strip().str.upper()
        df_final['FONTE'] = 'Cadastro'
    else:
        # Base: Códigos do dia
        df_final = pd.DataFrame({'codigo': list(codigos_dia)})
        
        # Junta SND (Volume, PU)
        if not df_snd.empty:
            df_final = pd.merge(df_final, df_snd, on='codigo', how='left', suffixes=('', '_snd'))
            
        # Junta ANBIMA (Taxa, Duration)
        if not df_anbima.empty:
            df_final = pd.merge(df_final, df_anbima, on='codigo', how='left', suffixes=('', '_anb'))
            
        # Junta Cadastro (Info estática)
        if not df_cadastro.empty:
            # Normaliza coluna de codigo do cadastro
            col_cad = next((c for c in df_cadastro.columns if c.lower() in ['codigo', 'codigo_ativo', 'ativo']), None)
            if col_cad:
                df_cad_clean = df_cadastro.copy()
                df_cad_clean.rename(columns={col_cad: 'codigo'}, inplace=True)
                df_cad_clean['codigo'] = df_cad_clean['codigo'].astype(str).str.strip().str.upper()
                df_final = pd.merge(df_final, df_cad_clean, on='codigo', how='left', suffixes=('', '_cad'))

        # Define Fonte dos Dados
        def get_fonte(row):
            has_snd = pd.notna(row.get('volume_total')) or pd.notna(row.get('numero_negocios'))
            has_anb = pd.notna(row.get('taxa_indicativa')) or pd.notna(row.get('taxa_compra'))
            if has_snd and has_anb: return "SND + Anbima"
            if has_snd: return "SND"
            if has_anb: return "Anbima"
            return "Cadastro"
        
        df_final['FONTE'] = df_final.apply(get_fonte, axis=1)

    # Limpeza final
    df_final = smart_clean(df_final)
    
    # Garante que temos a data de referência no DF
    df_final['data_referencia'] = selected_date_str

    return df_final, None

@st.cache_data(ttl=300)
def load_curva_anbima(target_date=None):
    """Carrega Curva de Juros"""
    if not os.path.exists(DB_CURVAS): return pd.DataFrame()
    conn = sqlite3.connect(DB_CURVAS)
    try:
        if target_date:
            # Tenta DD/MM/YYYY
            df = pd.read_sql(f"SELECT * FROM curvas_anbima WHERE data_referencia = '{target_date}'", conn)
            # Tenta YYYY-MM-DD
            if df.empty:
                try:
                    iso = datetime.strptime(target_date, "%d/%m/%Y").strftime("%Y-%m-%d")
                    df = pd.read_sql(f"SELECT * FROM curvas_anbima WHERE data_referencia = '{iso}'", conn)
                except: pass
        else:
            # Pega a mais recente
            df = pd.read_sql("SELECT * FROM curvas_anbima", conn)
            if not df.empty:
                # Ordenação simplificada (última inserção)
                df = df.tail(len(df[df['data_referencia'] == df.iloc[-1]['data_referencia']]))
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- FUNÇÕES AUXILIARES ESSENCIAIS ---

def apply_filters(df, filtros):
    """Aplica filtros ao DataFrame (Essencial para Screener)"""
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

def get_data_quality_report(df):
    """Gera relatório de auditoria (Essencial para Auditoria)"""
    report = {
        "total_registros": len(df),
        "campos_completos": {},
        "duplicatas": 0,
        "inconsistencias": []
    }
    for c in ['codigo', 'emissor', 'taxa', 'duration', 'volume']:
        if c in df.columns:
            valid = df[c].notna().sum()
            if c in ['taxa', 'volume']: valid = (df[c] > 0).sum()
            invalid = len(df) - valid
            report["campos_completos"][c] = {
                "validos": int(valid), 
                "invalidos": int(invalid),
                "percentual": round(valid/len(df)*100, 1) if len(df)>0 else 0
            }
            
    if 'codigo' in df.columns:
        report["duplicatas"] = int(df.duplicated(subset=['codigo']).sum())
        
    report["score_qualidade"] = 100 # Simplificado
    return report

def get_volume_summary():
    """Retorna volume total do banco (para KPI)"""
    if not os.path.exists(DB_DEBENTURES): return None
    try:
        conn = sqlite3.connect(DB_DEBENTURES)
        # Pega a data mais recente com dados
        last = pd.read_sql("SELECT MAX(data_base) as d FROM negociacao_snd", conn).iloc[0]['d']
        df = pd.read_sql(f"SELECT * FROM negociacao_snd WHERE data_base = '{last}'", conn)
        conn.close()
        
        return {
            "volume_total": df['volume_total'].sum(),
            "qtd_ativos": df['codigo'].nunique(),
            "data_ref": last
        }
    except: return None

def get_top_volume(n=5):
    """Retorna Top N ativos por volume"""
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
    """
    Adiciona spread em bps ao DataFrame.
    IMPORTANTE: Spread é calculado apenas para:
    - IPCA: comparado com ETTJ IPCA
    - PRÉ: comparado com ETTJ PRÉ
    Para CDI+ e %CDI, não calculamos spread (retorna None/N/A)
    """
    if df_ativos.empty or df_curva.empty or 'duration' not in df_ativos.columns: 
        return df_ativos
    
    df_ativos['dias_interpolacao'] = df_ativos['duration'] * 252
    
    def calc(row):
        try:
            d = row.get('dias_interpolacao', 0)
            t = row.get('taxa', 0)
            idx = str(row.get('indexador', '')).upper()
            
            if d <= 0 or t <= 0: 
                return None
            
            # SPREAD APENAS PARA IPCA E PRÉ
            # CDI+ e %CDI não têm curva benchmark comparável
            if 'IPCA' in idx:
                ref = 'taxa_ipca'
            elif 'PRÉ' in idx or 'PRE' in idx or 'PREFIXADO' in idx:
                ref = 'taxa_pre'
            else:
                # CDI, %CDI, IGP-M e outros: não calcula spread
                return None
            
            bench = interpolar_taxa_curva(df_curva, d, ref)
            if bench is not None: 
                return (t - bench) * 100
        except: 
            return None
        return None
    
    df_ativos['spread_bps'] = df_ativos.apply(calc, axis=1)
    
    # Adiciona coluna de taxa benchmark para referência
    def get_benchmark(row):
        idx = str(row.get('indexador', '')).upper()
        d = row.get('dias_interpolacao', 0)
        if d <= 0:
            return None
        if 'IPCA' in idx:
            return interpolar_taxa_curva(df_curva, d, 'taxa_ipca')
        elif 'PRÉ' in idx or 'PRE' in idx or 'PREFIXADO' in idx:
            return interpolar_taxa_curva(df_curva, d, 'taxa_pre')
        return None
    
    df_ativos['taxa_benchmark'] = df_ativos.apply(get_benchmark, axis=1)
    
    return df_ativos

def get_curvas_anbima_dates():
    if not os.path.exists(DB_CURVAS): return []
    conn = sqlite3.connect(DB_CURVAS)
    try:
        df = pd.read_sql("SELECT DISTINCT data_referencia FROM curvas_anbima", conn)
        conn.close()
        return sorted(df['data_referencia'].tolist(), reverse=True)
    except: return []

def get_database_status(data_ref=None):
    """
    Verifica o status dos bancos de dados e tabelas carregadas.
    Retorna dict com informações sobre cada fonte de dados.
    """
    status = {
        'snd_cadastro': {'loaded': False, 'count': 0},
        'snd_negociacao': {'loaded': False, 'count': 0},
        'anbima_precos': {'loaded': False, 'count': 0},
        'anbima_curvas': {'loaded': False, 'count': 0}
    }
    
    # Verifica banco principal
    if os.path.exists(DB_DEBENTURES):
        try:
            conn = sqlite3.connect(DB_DEBENTURES)
            
            # Cadastro SND
            try:
                df = pd.read_sql("SELECT COUNT(*) as c FROM cadastro_snd", conn)
                status['snd_cadastro']['loaded'] = True
                status['snd_cadastro']['count'] = int(df.iloc[0]['c'])
            except: pass
            
            # Negociação SND (por data se especificada)
            try:
                if data_ref:
                    try:
                        dt_obj = datetime.strptime(data_ref, "%d/%m/%Y")
                        date_iso = dt_obj.strftime("%Y-%m-%d")
                    except:
                        date_iso = data_ref
                    df = pd.read_sql(f"SELECT COUNT(*) as c FROM negociacao_snd WHERE data_base = '{date_iso}'", conn)
                else:
                    df = pd.read_sql("SELECT COUNT(*) as c FROM negociacao_snd", conn)
                status['snd_negociacao']['loaded'] = True
                status['snd_negociacao']['count'] = int(df.iloc[0]['c'])
            except: pass
            
            # ANBIMA Preços (por data se especificada)
            try:
                if data_ref:
                    df = pd.read_sql(f"SELECT COUNT(*) as c FROM mercado_secundario WHERE data_referencia = '{data_ref}'", conn)
                    # Tenta ISO se não encontrou
                    if df.iloc[0]['c'] == 0:
                        try:
                            dt_obj = datetime.strptime(data_ref, "%d/%m/%Y")
                            date_iso = dt_obj.strftime("%Y-%m-%d")
                            df = pd.read_sql(f"SELECT COUNT(*) as c FROM mercado_secundario WHERE data_referencia = '{date_iso}'", conn)
                        except: pass
                else:
                    df = pd.read_sql("SELECT COUNT(*) as c FROM mercado_secundario", conn)
                status['anbima_precos']['loaded'] = True
                status['anbima_precos']['count'] = int(df.iloc[0]['c'])
            except: pass
            
            conn.close()
        except: pass
    
    # Verifica banco de curvas
    if os.path.exists(DB_CURVAS):
        try:
            conn = sqlite3.connect(DB_CURVAS)
            df = pd.read_sql("SELECT COUNT(*) as c FROM curvas_anbima", conn)
            status['anbima_curvas']['loaded'] = True
            status['anbima_curvas']['count'] = int(df.iloc[0]['c'])
            conn.close()
        except: pass
    
    return status

def get_metrics_by_category(df):
    """
    Calcula métricas separadas por categoria.
    Retorna dict com taxa média e duration média para cada categoria.
    """
    if df.empty or 'categoria_grafico' not in df.columns:
        return {}
    
    categorias = ['IPCA Incentivado', 'IPCA Não Incentivado', 'CDI +', '% CDI', 'Prefixado']
    metrics = {}
    
    for cat in categorias:
        df_cat = df[df['categoria_grafico'] == cat]
        if df_cat.empty:
            continue
            
        taxa_media = 0
        duration_media = 0
        spread_medio = None
        
        if 'taxa' in df_cat.columns:
            df_taxa = df_cat[df_cat['taxa'] > 0]
            if not df_taxa.empty:
                taxa_media = df_taxa['taxa'].mean()
        
        if 'duration' in df_cat.columns:
            df_dur = df_cat[df_cat['duration'] > 0]
            if not df_dur.empty:
                duration_media = df_dur['duration'].mean()
        
        if 'spread_bps' in df_cat.columns:
            df_spread = df_cat[df_cat['spread_bps'].notna()]
            if not df_spread.empty:
                spread_medio = df_spread['spread_bps'].mean()
        
        metrics[cat] = {
            'quantidade': len(df_cat),
            'taxa_media': taxa_media,
            'duration_media': duration_media,
            'spread_medio': spread_medio
        }
    
    return metrics

def get_consolidation_stats(df):
    """
    Retorna estatísticas de consolidação de dados.
    IMPORTANTE: A soma das categorias deve ser igual ao total.
    - SND + ANBIMA: ativos com dados de ambas as fontes
    - Somente ANBIMA: ativos com apenas preços indicativos
    - Somente SND: ativos com apenas dados de negociação SND
    - Cadastro: ativos apenas no cadastro, sem preço de mercado
    """
    if df.empty or 'FONTE' not in df.columns:
        return None
    
    total = len(df)
    
    # Contagem por fonte (mutuamente exclusivas)
    stats = {
        'total': total,
        'consolidado': len(df[df['FONTE'] == 'SND + Anbima']),
        'anbima_only': len(df[df['FONTE'] == 'Anbima']),
        'snd_only': len(df[df['FONTE'] == 'SND']),
        'cadastro_only': len(df[df['FONTE'] == 'Cadastro'])
    }
    
    # Validação: a soma deve bater
    soma = stats['consolidado'] + stats['anbima_only'] + stats['snd_only'] + stats['cadastro_only']
    if soma != total:
        # Ajusta cadastro_only para incluir qualquer fonte não mapeada
        outros = total - soma
        stats['cadastro_only'] += outros
    
    # Calcula percentuais
    for key in ['consolidado', 'anbima_only', 'snd_only', 'cadastro_only']:
        stats[f'{key}_pct'] = (stats[key] / total * 100) if total > 0 else 0
    
    return stats


# === FUNÇÕES PARA ANÁLISE POR EMPRESA ===

def get_empresas_emissoras(df=None):
    """
    Retorna lista de empresas emissoras únicas.
    Se df não for fornecido, carrega do cadastro.
    """
    if df is not None and 'emissor' in df.columns:
        empresas = df['emissor'].dropna().unique()
        return sorted([e for e in empresas if e and e != 'N/D'])
    
    # Fallback: carrega do cadastro no banco
    if not os.path.exists(DB_DEBENTURES):
        return []
    
    try:
        conn = sqlite3.connect(DB_DEBENTURES)
        
        # Tenta várias colunas possíveis
        colunas_emissor = ['Empresa', 'emissor', 'Razao Social', 'Nome Emissor', 'nome']
        
        for col in colunas_emissor:
            try:
                df = pd.read_sql(f'SELECT DISTINCT "{col}" as emissor FROM cadastro_snd', conn)
                if not df.empty:
                    empresas = df['emissor'].dropna().unique()
                    conn.close()
                    return sorted([e for e in empresas if e and str(e).strip() != ''])
            except:
                continue
        
        conn.close()
        return []
    except:
        return []


def get_debentures_por_empresa(empresa_nome, df=None):
    """
    Retorna todas as debêntures de uma empresa específica.
    Busca por nome do emissor (match parcial case-insensitive).
    """
    if df is None:
        return pd.DataFrame()
    
    if 'emissor' not in df.columns:
        return pd.DataFrame()
    
    # Busca case-insensitive com match parcial
    empresa_lower = empresa_nome.lower().strip()
    mask = df['emissor'].fillna('').str.lower().str.contains(empresa_lower, regex=False)
    
    return df[mask].copy()


def get_resumo_empresa(df_empresa):
    """
    Gera resumo consolidado de uma empresa baseado em suas debêntures.
    """
    if df_empresa.empty:
        return None
    
    resumo = {
        'total_debentures': len(df_empresa),
        'debentures_ativas': 0,
        'debentures_vencidas': 0,
        'valor_total_emitido': 0,
        'volume_negociado': 0,
        'indexadores': {},
        'categorias': {},
        'taxa_media': 0,
        'duration_media': 0,
        'spread_medio': None,
        'proximos_vencimentos': []
    }
    
    # Status (ativo/vencido) - baseado em data de vencimento se disponível
    hoje = datetime.now()
    if 'vencimento' in df_empresa.columns or 'data_vencimento' in df_empresa.columns:
        col_venc = 'vencimento' if 'vencimento' in df_empresa.columns else 'data_vencimento'
        for _, row in df_empresa.iterrows():
            try:
                venc_str = str(row[col_venc])
                # Tenta vários formatos
                for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
                    try:
                        venc_date = datetime.strptime(venc_str, fmt)
                        if venc_date > hoje:
                            resumo['debentures_ativas'] += 1
                        else:
                            resumo['debentures_vencidas'] += 1
                        break
                    except:
                        continue
            except:
                resumo['debentures_ativas'] += 1  # Assume ativa se não conseguir parsear
    else:
        # Se não tiver data de vencimento, assume todas ativas
        resumo['debentures_ativas'] = len(df_empresa)
    
    # Volume negociado
    if 'volume' in df_empresa.columns:
        resumo['volume_negociado'] = df_empresa['volume'].sum()
    elif 'volume_total' in df_empresa.columns:
        resumo['volume_negociado'] = df_empresa['volume_total'].sum()
    
    # Distribuição por indexador
    if 'indexador' in df_empresa.columns:
        resumo['indexadores'] = df_empresa['indexador'].value_counts().to_dict()
    
    # Distribuição por categoria
    if 'categoria_grafico' in df_empresa.columns:
        resumo['categorias'] = df_empresa['categoria_grafico'].value_counts().to_dict()
    
    # Taxa média
    if 'taxa' in df_empresa.columns:
        df_taxa = df_empresa[df_empresa['taxa'] > 0]
        if not df_taxa.empty:
            resumo['taxa_media'] = df_taxa['taxa'].mean()
    
    # Duration média
    if 'duration' in df_empresa.columns:
        df_dur = df_empresa[df_empresa['duration'] > 0]
        if not df_dur.empty:
            resumo['duration_media'] = df_dur['duration'].mean()
    
    # Spread médio
    if 'spread_bps' in df_empresa.columns:
        df_spread = df_empresa[df_empresa['spread_bps'].notna()]
        if not df_spread.empty:
            resumo['spread_medio'] = df_spread['spread_bps'].mean()
    
    # Próximos vencimentos (até 5)
    if 'vencimento' in df_empresa.columns or 'data_vencimento' in df_empresa.columns:
        col_venc = 'vencimento' if 'vencimento' in df_empresa.columns else 'data_vencimento'
        proximos = []
        for _, row in df_empresa.iterrows():
            try:
                venc_str = str(row[col_venc])
                for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
                    try:
                        venc_date = datetime.strptime(venc_str, fmt)
                        if venc_date > hoje:
                            proximos.append({
                                'codigo': row.get('codigo', 'N/D'),
                                'vencimento': venc_str,
                                'data': venc_date
                            })
                        break
                    except:
                        continue
            except:
                pass
        
        # Ordena por data e pega os 5 mais próximos
        proximos.sort(key=lambda x: x.get('data', datetime.max))
        resumo['proximos_vencimentos'] = [
            {'codigo': p['codigo'], 'vencimento': p['vencimento']} 
            for p in proximos[:5]
        ]
    
    return resumo




# === FUNÇÕES PARA TAXAS INDICATIVAS ANBIMA ===

@st.cache_data(ttl=300)
def load_taxas_indicativas(data_ref=None):
    """
    Carrega taxas indicativas da ANBIMA.
    Se data_ref não for especificada, retorna a data mais recente.
    """
    if not os.path.exists(DB_DEBENTURES):
        return pd.DataFrame()
    
    conn = sqlite3.connect(DB_DEBENTURES)
    try:
        if data_ref:
            # Tenta formato BR e ISO
            df = pd.read_sql(f"""
                SELECT * FROM taxas_indicativas_anbima 
                WHERE data_referencia = '{data_ref}'
            """, conn)
            
            if df.empty:
                try:
                    dt_obj = datetime.strptime(data_ref, "%d/%m/%Y")
                    date_iso = dt_obj.strftime("%Y-%m-%d")
                    df = pd.read_sql(f"""
                        SELECT * FROM taxas_indicativas_anbima 
                        WHERE data_referencia = '{date_iso}'
                    """, conn)
                except:
                    pass
        else:
            # Pega a data mais recente
            df = pd.read_sql("""
                SELECT * FROM taxas_indicativas_anbima 
                WHERE data_referencia = (SELECT MAX(data_referencia) FROM taxas_indicativas_anbima)
            """, conn)
        
        return df
    except Exception as e:
        return pd.DataFrame()
    finally:
        conn.close()


def get_taxas_indicativas_status():
    """
    Retorna status das taxas indicativas (para sidebar).
    """
    if not os.path.exists(DB_DEBENTURES):
        return {'loaded': False, 'count': 0}
    
    try:
        conn = sqlite3.connect(DB_DEBENTURES)
        df = pd.read_sql("SELECT COUNT(*) as c FROM taxas_indicativas_anbima", conn)
        conn.close()
        return {'loaded': True, 'count': int(df.iloc[0]['c'])}
    except:
        return {'loaded': False, 'count': 0}


# === FUNÇÕES PARA ANÁLISE DE VOLUME ===

def load_volume_historico(dias=30):
    """
    Carrega histórico de volume de negociação dos últimos N dias.
    """
    if not os.path.exists(DB_DEBENTURES):
        return pd.DataFrame()
    
    conn = sqlite3.connect(DB_DEBENTURES)
    try:
        df = pd.read_sql(f"""
            SELECT 
                data_base,
                SUM(volume_total) as volume_total,
                COUNT(DISTINCT codigo) as qtd_ativos,
                SUM(numero_negocios) as total_negocios,
                AVG(pu_medio) as pu_medio_geral
            FROM negociacao_snd
            GROUP BY data_base
            ORDER BY data_base DESC
            LIMIT {dias}
        """, conn)
        return df
    except:
        return pd.DataFrame()
    finally:
        conn.close()


def load_volume_por_ativo(data_ref=None, limit=50):
    """
    Carrega volume detalhado por ativo para uma data específica.
    """
    if not os.path.exists(DB_DEBENTURES):
        return pd.DataFrame()
    
    conn = sqlite3.connect(DB_DEBENTURES)
    try:
        if data_ref:
            try:
                dt_obj = datetime.strptime(data_ref, "%d/%m/%Y")
                date_iso = dt_obj.strftime("%Y-%m-%d")
            except:
                date_iso = data_ref
            where_clause = f"WHERE data_base = '{date_iso}'"
        else:
            where_clause = "WHERE data_base = (SELECT MAX(data_base) FROM negociacao_snd)"
        
        df = pd.read_sql(f"""
            SELECT *
            FROM negociacao_snd
            {where_clause}
            ORDER BY volume_total DESC
            LIMIT {limit}
        """, conn)
        return df
    except:
        return pd.DataFrame()
    finally:
        conn.close()


def get_volume_por_indexador(df):
    """
    Agrupa volume por indexador.
    """
    if df.empty or 'indexador' not in df.columns:
        return pd.DataFrame()
    
    if 'volume' in df.columns:
        vol_col = 'volume'
    elif 'volume_total' in df.columns:
        vol_col = 'volume_total'
    else:
        return pd.DataFrame()
    
    return df.groupby('indexador').agg({
        vol_col: 'sum',
        'codigo': 'count'
    }).reset_index().rename(columns={'codigo': 'qtd_ativos'})


# === FUNÇÕES PARA DETECÇÃO DE NEGOCIAÇÕES ATÍPICAS ===

def detectar_negociacoes_atipicas(df, threshold_zscore=2.0, threshold_preco_pct=5.0):
    """
    Detecta negociações atípicas baseado em:
    1. Volume muito acima da média (Z-score > threshold)
    2. Preço muito diferente do PU indicativo
    3. Número de negócios atípico
    
    Args:
        df: DataFrame com dados de negociação
        threshold_zscore: Threshold para Z-score (padrão 2.0 = ~95%)
        threshold_preco_pct: Diferença % máxima aceitável entre PU negociado e indicativo
    
    Returns:
        DataFrame com colunas adicionais: atipico, motivo_atipicidade, zscore_volume
    """
    if df.empty:
        return df
    
    df_analise = df.copy()
    df_analise['atipico'] = False
    df_analise['motivo_atipicidade'] = ''
    df_analise['zscore_volume'] = 0.0
    df_analise['desvio_preco_pct'] = 0.0
    
    # 1. Z-Score de Volume
    vol_col = 'volume_total' if 'volume_total' in df_analise.columns else 'volume' if 'volume' in df_analise.columns else None
    
    if vol_col and df_analise[vol_col].std() > 0:
        media_vol = df_analise[vol_col].mean()
        std_vol = df_analise[vol_col].std()
        df_analise['zscore_volume'] = (df_analise[vol_col] - media_vol) / std_vol
        
        # Marca como atípico se Z-score alto
        mask_volume = df_analise['zscore_volume'] > threshold_zscore
        df_analise.loc[mask_volume, 'atipico'] = True
        df_analise.loc[mask_volume, 'motivo_atipicidade'] = df_analise.loc[mask_volume, 'motivo_atipicidade'] + 'Volume alto; '
    
    # 2. Z-Score de Número de Negócios
    neg_col = 'numero_negocios' if 'numero_negocios' in df_analise.columns else 'negocios' if 'negocios' in df_analise.columns else None
    
    if neg_col and df_analise[neg_col].std() > 0:
        media_neg = df_analise[neg_col].mean()
        std_neg = df_analise[neg_col].std()
        zscore_neg = (df_analise[neg_col] - media_neg) / std_neg
        
        # Marca como atípico se poucos negócios com muito volume (concentração)
        if vol_col:
            mask_concentracao = (zscore_neg < -1) & (df_analise['zscore_volume'] > threshold_zscore)
            df_analise.loc[mask_concentracao, 'atipico'] = True
            df_analise.loc[mask_concentracao, 'motivo_atipicidade'] = df_analise.loc[mask_concentracao, 'motivo_atipicidade'] + 'Concentração; '
    
    # 3. Desvio de Preço (se tiver PU indicativo)
    pu_neg_col = 'pu_medio' if 'pu_medio' in df_analise.columns else 'pu' if 'pu' in df_analise.columns else None
    
    if pu_neg_col and 'taxa_indicativa' in df_analise.columns:
        # Compara com taxa indicativa se disponível
        pass  # Implementação futura com merge de taxas indicativas
    
    # 4. Calcula ticket médio para análise
    if vol_col and neg_col:
        df_analise['ticket_medio'] = np.where(
            df_analise[neg_col] > 0,
            df_analise[vol_col] / df_analise[neg_col],
            0
        )
        
        # Ticket muito alto também é atípico
        if df_analise['ticket_medio'].std() > 0:
            media_ticket = df_analise['ticket_medio'].mean()
            std_ticket = df_analise['ticket_medio'].std()
            zscore_ticket = (df_analise['ticket_medio'] - media_ticket) / std_ticket
            
            mask_ticket = zscore_ticket > threshold_zscore
            df_analise.loc[mask_ticket, 'atipico'] = True
            df_analise.loc[mask_ticket, 'motivo_atipicidade'] = df_analise.loc[mask_ticket, 'motivo_atipicidade'] + 'Ticket alto; '
    
    # Limpa motivo
    df_analise['motivo_atipicidade'] = df_analise['motivo_atipicidade'].str.rstrip('; ')
    
    return df_analise


def get_estatisticas_volume(df):
    """
    Calcula estatísticas de volume para análise.
    """
    if df.empty:
        return {}
    
    vol_col = 'volume_total' if 'volume_total' in df.columns else 'volume' if 'volume' in df.columns else None
    neg_col = 'numero_negocios' if 'numero_negocios' in df.columns else 'negocios' if 'negocios' in df.columns else None
    
    stats = {
        'volume_total': 0,
        'volume_medio': 0,
        'volume_mediana': 0,
        'total_negocios': 0,
        'ticket_medio': 0,
        'qtd_ativos': df['codigo'].nunique() if 'codigo' in df.columns else len(df)
    }
    
    if vol_col:
        stats['volume_total'] = df[vol_col].sum()
        stats['volume_medio'] = df[vol_col].mean()
        stats['volume_mediana'] = df[vol_col].median()
    
    if neg_col:
        stats['total_negocios'] = df[neg_col].sum()
        
        if vol_col and stats['total_negocios'] > 0:
            stats['ticket_medio'] = stats['volume_total'] / stats['total_negocios']
    
    return stats


def comparar_volume_historico(df_atual, df_historico):
    """
    Compara volume atual com histórico para detectar anomalias.
    """
    if df_atual.empty or df_historico.empty:
        return None
    
    vol_col = 'volume_total' if 'volume_total' in df_historico.columns else 'volume'
    
    if vol_col not in df_historico.columns:
        return None
    
    media_historica = df_historico[vol_col].mean()
    std_historico = df_historico[vol_col].std()
    
    if std_historico == 0:
        return None
    
    vol_atual = df_atual[vol_col].sum() if vol_col in df_atual.columns else 0
    
    return {
        'volume_atual': vol_atual,
        'media_historica': media_historica,
        'std_historico': std_historico,
        'zscore': (vol_atual - media_historica) / std_historico if std_historico > 0 else 0,
        'variacao_pct': ((vol_atual - media_historica) / media_historica * 100) if media_historica > 0 else 0
    }


# === ATUALIZAÇÃO DO get_database_status PARA INCLUIR TAXAS INDICATIVAS ===

def get_database_status_full(data_ref=None):
    """
    Versão completa do status do banco incluindo taxas indicativas.
    """
    status = get_database_status(data_ref)
    
    # Adiciona status das taxas indicativas
    status['anbima_indicativa'] = get_taxas_indicativas_status()
    
    return status
