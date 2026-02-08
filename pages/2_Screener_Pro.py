"""
Screener Pro - Filtros Avançados e Scatter Plot Risco x Retorno
"""
import streamlit as st
import sys
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

try:
    import data_engine as engine
    import visuals
except ImportError as e:
    st.error(f"Erro ao importar módulos: {e}")
    st.stop()

st.set_page_config(page_title="Screener Pro", page_icon="🔍", layout="wide")

# CSS Customizado
st.markdown("""
<style>
    [data-testid="stMetricValue"] { font-size: 1.5rem; font-weight: 700; }
    h1, h2, h3 { color: #AB63FA; }
</style>
""", unsafe_allow_html=True)

# ===== SIDEBAR COM MEMÓRIA =====
with st.sidebar:
    logo_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logo.png")
    if os.path.exists(logo_path): st.image(logo_path, use_container_width=True)
    else: st.title("BondTrack")
    
    st.title("Screener Pro")
    
    try:
        datas = engine.get_available_dates()
    except: datas = []
    
    if not datas:
        st.error("Nenhuma data disponível")
        st.stop()
    
    # Lógica de Memória
    if 'global_data_ref' not in st.session_state:
        st.session_state['global_data_ref'] = datas[0]
    if st.session_state['global_data_ref'] not in datas:
        st.session_state['global_data_ref'] = datas[0]
        
    data_ref = st.selectbox(
        "Data de Referência", 
        datas,
        index=datas.index(st.session_state['global_data_ref']),
        key='screener_date_widget',
        on_change=lambda: st.session_state.update({'global_data_ref': st.session_state.screener_date_widget})
    )
    st.session_state['global_data_ref'] = data_ref

# ===== CARREGAR DADOS =====
df_full, erro = engine.load_data(data_ref)

if erro or df_full is None or df_full.empty:
    st.error(f"Erro ao carregar dados: {erro}")
    st.stop()

# Carregar curva ANBIMA e adicionar spreads
try:
    curva_df = engine.load_curva_anbima(data_ref)
    if not curva_df.empty:
        df_full = engine.adicionar_spreads_ao_df(df_full, curva_df)
        curva_disponivel = True
    else:
        curva_disponivel = False
except:
    curva_disponivel = False

# ===== CONTEÚDO PRINCIPAL =====
st.title("Screener Pro - Filtros Avançados")
st.caption(f"Dados referentes a: {data_ref}")

if curva_disponivel:
    try:
        curva_dates = engine.get_curvas_anbima_dates()
        if curva_dates: st.caption(f"Curva ANBIMA disponível.")
    except: pass

st.divider()

# ===== FILTROS ACCORDION =====
st.markdown("### Filtros")

filtros = {}

# Accordion 1 - Filtros de Mercado
with st.expander("Filtros de Mercado", expanded=True):
    col_m1, col_m2 = st.columns(2)
    
    with col_m1:
        # Categoria
        if 'categoria_grafico' in df_full.columns:
            cats = sorted(df_full['categoria_grafico'].dropna().unique())
            sel_cats = st.multiselect("Categoria", cats, default=cats)
            filtros['categoria'] = sel_cats
        
        # Indexador
        if 'indexador' in df_full.columns:
            idxs = sorted(df_full['indexador'].dropna().unique())
            sel_idxs = st.multiselect("Indexador", idxs)
            filtros['indexador'] = sel_idxs if sel_idxs else None
    
    with col_m2:
        # Fonte
        if 'FONTE' in df_full.columns:
            fontes = ["Todos"] + sorted(df_full['FONTE'].dropna().unique().tolist())
            sel_fonte = st.selectbox("Fonte de Dados", fontes)
            filtros['fonte'] = sel_fonte
        
        # Emissor
        if 'emissor' in df_full.columns:
            emissores = sorted(df_full['emissor'].dropna().unique())
            sel_emi = st.multiselect("Emissor", emissores)
            filtros['emissor'] = sel_emi if sel_emi else None

# Accordion 2 - Filtros de Crédito/Risco
with st.expander("Filtros de Crédito e Risco"):
    col_c1, col_c2 = st.columns(2)
    
    with col_c1:
        if 'taxa' in df_full.columns:
            max_val = float(df_full['taxa'].max()) if df_full['taxa'].max() > 0 else 20.0
            taxa_min, taxa_max = st.slider("Taxa (%)", 0.0, max_val, (0.0, max_val), 0.1)
            filtros['taxa_min'] = taxa_min
            filtros['taxa_max'] = taxa_max
    
    with col_c2:
        if 'duration' in df_full.columns:
            max_dur = float(df_full['duration'].max()) if df_full['duration'].max() > 0 else 10.0
            dur_min, dur_max = st.slider("Duration (anos)", 0.0, max_dur, (0.0, max_dur), 0.1)
            filtros['duration_min'] = dur_min
            filtros['duration_max'] = dur_max

# Accordion 3 - Filtros de Spread
with st.expander("Filtros de Spread vs Curva ANBIMA"):
    if curva_disponivel and 'spread_bps' in df_full.columns:
        col_s1, col_s2 = st.columns(2)
        df_spread_valid = df_full[df_full['spread_bps'].notna()]
        
        if not df_spread_valid.empty:
            min_s = float(df_spread_valid['spread_bps'].min())
            max_s = float(df_spread_valid['spread_bps'].max())
            
            with col_s1:
                s_min, s_max = st.slider("Spread (bps)", min_s, max_s, (min_s, max_s), 1.0)
                filtros['spread_min'] = s_min
                filtros['spread_max'] = s_max
            
            with col_s2:
                st.info("Spread positivo = taxa acima da curva (prêmio de risco).")
        else:
            st.warning("Sem dados de spread calculados.")
    else:
        st.warning("Curva ANBIMA indisponível para esta data.")

# Accordion 4 - Filtros de Liquidez
with st.expander("Filtros de Liquidez"):
    if 'cluster_duration' in df_full.columns:
        clusters = sorted(df_full['cluster_duration'].unique())
        sel_clust = st.multiselect("Prazo (Cluster)", clusters)
        filtros['cluster'] = sel_clust if sel_clust else None

st.divider()

# ===== APLICAR FILTROS =====
df = engine.apply_filters(df_full, filtros)

# Filtro manual de spread (já que engine.apply_filters pode não ter isso implementado ainda)
if 'spread_min' in filtros and 'spread_bps' in df.columns:
    df = df[
        (df['spread_bps'].isna()) | 
        ((df['spread_bps'] >= filtros['spread_min']) & (df['spread_bps'] <= filtros['spread_max']))
    ]

if df.empty:
    st.warning("Nenhum ativo encontrado com esses filtros.")
    st.stop()

# ===== RESULTADOS =====
st.markdown(f"### Resultados: {len(df)} ativos")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Ativos", len(df))
c2.metric("Taxa Média", f"{df[df['taxa']>0]['taxa'].mean():.2f}%" if 'taxa' in df else "0")
c3.metric("Duration Médio", f"{df[df['duration']>0]['duration'].mean():.2f}" if 'duration' in df else "0")
c4.metric("Incentivados", len(df[df['categoria_grafico'] == 'IPCA Incentivado']) if 'categoria_grafico' in df else 0)

if curva_disponivel and 'spread_bps' in df.columns:
    mean_spread = df[df['spread_bps'].notna()]['spread_bps'].mean()
    c5.metric("Spread Médio", f"{mean_spread:.0f} bps")
else:
    c5.metric("Spread Médio", "N/D")

st.divider()

# ===== GRÁFICOS =====
tab1, tab2 = st.tabs(["Risco x Retorno", "Spread x Duration"])

with tab1:
    if 'taxa' in df.columns and 'duration' in df.columns:
        df_g = df[(df['taxa'] > 0) & (df['duration'] > 0)].copy()
        if not df_g.empty:
            fig = visuals.create_scatter_risco_retorno(df_g, height=600)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Dados insuficientes para gráfico.")

with tab2:
    if curva_disponivel and 'spread_bps' in df.columns:
        df_s = df[(df['spread_bps'].notna()) & (df['duration'] > 0)].copy()
        if not df_s.empty:
            fig_s = px.scatter(
                df_s, x='duration', y='spread_bps', color='categoria_grafico',
                hover_name='codigo' if 'codigo' in df_s else None,
                title="Spread vs Duration", template='plotly_dark', height=600
            )
            fig_s.add_hline(y=0, line_dash="dash", line_color="white")
            fig_s.update_layout(paper_bgcolor='#0e1117', plot_bgcolor='#1a1d24')
            st.plotly_chart(fig_s, use_container_width=True)
        else:
            st.info("Sem dados de spread para gráfico.")
    else:
        st.warning("Curva indisponível.")

st.divider()

# ===== TABELA =====
st.markdown("### Tabela Detalhada")

# Colunas
cols = ['codigo', 'emissor', 'categoria_grafico', 'indexador', 'taxa', 'duration', 'pu']
if curva_disponivel and 'spread_bps' in df.columns: cols.extend(['spread_bps'])
cols = [c for c in cols if c in df.columns]

st.dataframe(
    df[cols].sort_values('taxa', ascending=False),
    use_container_width=True,
    height=500,
    column_config={
        "taxa": st.column_config.NumberColumn(format="%.2f%%"),
        "duration": st.column_config.NumberColumn(format="%.2f"),
        "pu": st.column_config.NumberColumn(format="R$ %.2f"),
        "spread_bps": st.column_config.NumberColumn(format="%.0f bps")
    }
)
