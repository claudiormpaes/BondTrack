"""
Radar de Mercado - Top Movers, Heatmap, Curvas de Juros
"""
import streamlit as st
import sys
import os
import pandas as pd

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

import data_engine as engine
import visuals
import financial_math as fm
import sidebar_utils

st.set_page_config(page_title="Radar de Mercado", page_icon="📡", layout="wide")

# CSS Customizado
st.markdown("""
<style>
    [data-testid="stMetricValue"] {
        font-size: 1.5rem;
        font-weight: 700;
    }
    h1, h2, h3 {
        color: #00CC96;
    }
</style>
""", unsafe_allow_html=True)

# ===== SIDEBAR =====
with st.sidebar:
    sidebar_utils.render_logo()
    st.title("Radar de Mercado")
    
    datas_disponiveis = engine.get_available_dates()
    
    if not datas_disponiveis:
        st.error("Nenhuma data disponível")
        st.stop()
    
    data_ref = st.selectbox("Data de Referência", datas_disponiveis)
    
    st.divider()
    
    # Filtros
    st.markdown("### Filtros")
    
    df_full, erro = engine.load_data(data_ref)
    
    if erro or df_full is None or df_full.empty:
        st.error(f"Erro ao carregar dados: {erro}")
        st.stop()
    
    # Filtro de Categoria
    categorias_disponiveis = sorted(df_full['categoria_grafico'].unique())
    categorias_selecionadas = st.multiselect(
        "Categoria",
        categorias_disponiveis,
        default=categorias_disponiveis
    )
    
    # Filtro de Indexador
    indexadores_disponiveis = sorted(df_full['indexador'].unique())
    indexadores_selecionados = st.multiselect(
        "Indexador",
        indexadores_disponiveis
    )
    
    # Filtro de Fonte
    fonte = st.selectbox(
        "Fonte",
        ["Todos"] + sorted(df_full['FONTE'].unique().tolist())
    )

# ===== APLICA FILTROS =====
filtros = {
    "categoria": categorias_selecionadas,
    "indexador": indexadores_selecionados if indexadores_selecionados else None,
    "fonte": fonte
}

df = engine.apply_filters(df_full, filtros)

# ===== CONTEÚDO PRINCIPAL =====
st.title("Radar de Mercado")
st.markdown(f"**Data de Referência:** {data_ref}")

if df.empty:
    st.warning("Nenhum dado disponível com os filtros selecionados")
    st.stop()

# ===== KPIs =====
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total de Ativos", f"{len(df):,}")

with col2:
    taxa_media = df[df['taxa'] > 0]['taxa'].mean() if len(df[df['taxa'] > 0]) > 0 else 0
    st.metric("Taxa Média", f"{taxa_media:.2f}%")

with col3:
    duration_media = df[df['duration'] > 0]['duration'].mean() if len(df[df['duration'] > 0]) > 0 else 0
    st.metric("Duration Médio", f"{duration_media:.2f} anos")

with col4:
    taxa_max = df['taxa'].max() if len(df[df['taxa'] > 0]) > 0 else 0
    st.metric("Maior Taxa", f"{taxa_max:.2f}%")

st.divider()

# ===== HEATMAP DE RISCO =====
st.markdown("### Heatmap de Taxas por Indexador")

if not df.empty and len(df) > 0:
    # Selecionar principais indexadores
    indexadores_principais = df['indexador'].value_counts().nlargest(5).index.tolist()
    
    fig_heatmap = visuals.create_heatmap_indexador(df, indexadores_principais)
    st.plotly_chart(fig_heatmap, use_container_width=True)
else:
    st.info("Dados insuficientes para gerar heatmap")

st.divider()

# ===== CURVAS DE JUROS =====
st.markdown("### Curvas de Juros por Indexador")

col_curva1, col_curva2 = st.columns(2)

# Curva IPCA
with col_curva1:
    df_ipca = df[df['indexador'].str.contains('IPCA', na=False)]
    df_ipca = df_ipca[(df_ipca['taxa'] > 0) & (df_ipca['duration'] > 0)]
    
    if not df_ipca.empty:
        fig_ipca = visuals.create_curva_juros(df_ipca, "IPCA", color="#00CC96")
        st.plotly_chart(fig_ipca, use_container_width=True)
    else:
        st.info("Sem dados para curva IPCA")

# Curva CDI
with col_curva2:
    df_cdi = df[df['indexador'].str.contains('CDI', na=False)]
    df_cdi = df_cdi[(df_cdi['taxa'] > 0) & (df_cdi['duration'] > 0)]
    
    if not df_cdi.empty:
        fig_cdi = visuals.create_curva_juros(df_cdi, "CDI", color="#636EFA")
        st.plotly_chart(fig_cdi, use_container_width=True)
    else:
        st.info("Sem dados para curva CDI")

st.divider()

# ===== TOP MOVERS =====
st.markdown("### Top Performers")

col_top1, col_top2 = st.columns(2)

with col_top1:
    st.markdown("#### Top 10 Maiores Taxas")
    
    # Filtro por indexador para Top Taxas
    indexadores_top = ["Todos"] + sorted(df[df['taxa'] > 0]['indexador'].dropna().unique().tolist())
    filtro_indexador_top = st.selectbox(
        "Filtrar por Indexador",
        indexadores_top,
        key="filtro_indexador_top_taxas"
    )
    
    df_for_top = df[df['taxa'] > 0].copy()
    if filtro_indexador_top != "Todos":
        df_for_top = df_for_top[df_for_top['indexador'] == filtro_indexador_top]
    
    df_top_taxas = df_for_top.nlargest(10, 'taxa')[['codigo', 'emissor', 'taxa', 'indexador', 'duration']]
    st.dataframe(
        df_top_taxas.style.format({'taxa': '{:.2f}%', 'duration': '{:.2f}'}),
        hide_index=True,
        use_container_width=True,
        height=350
    )

with col_top2:
    st.markdown("#### Top 10 Maiores Durations")
    df_top_duration = df[df['duration'] > 0].nlargest(10, 'duration')[['codigo', 'emissor', 'duration', 'indexador', 'taxa']]
    st.dataframe(
        df_top_duration.style.format({'taxa': '{:.2f}%', 'duration': '{:.2f}'}),
        hide_index=True,
        use_container_width=True,
        height=400
    )

st.divider()

# ===== DISTRIBUIÇÕES =====
st.markdown("### Distribuições")

col_dist1, col_dist2 = st.columns(2)

with col_dist1:
    fig_box_taxa = visuals.create_box_plot_categoria(
        df[df['taxa'] > 0],
        x_col='categoria_grafico',
        y_col='taxa',
        title="Distribuição de Taxas por Categoria"
    )
    st.plotly_chart(fig_box_taxa, use_container_width=True)

with col_dist2:
    fig_box_duration = visuals.create_box_plot_categoria(
        df[df['duration'] > 0],
        x_col='categoria_grafico',
        y_col='duration',
        title="Distribuição de Duration por Categoria"
    )
    st.plotly_chart(fig_box_duration, use_container_width=True)

st.divider()

# ===== TABELA COMPLETA =====
with st.expander("Ver Tabela Completa"):
    cols_view = ['codigo', 'emissor', 'categoria_grafico', 'indexador', 'taxa', 'duration', 'pu', 'FONTE']
    cols_disponiveis = [c for c in cols_view if c in df.columns]
    
    st.dataframe(
        df[cols_disponiveis].sort_values('taxa', ascending=False),
        use_container_width=True,
        hide_index=True
    )
