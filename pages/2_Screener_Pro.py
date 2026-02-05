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

import data_engine as engine
import visuals
import financial_math as fm
import sidebar_utils

st.set_page_config(page_title="Screener Pro", page_icon="🔍", layout="wide")

# CSS Customizado
st.markdown("""
<style>
    [data-testid="stMetricValue"] {
        font-size: 1.5rem;
        font-weight: 700;
    }
    h1, h2, h3 {
        color: #AB63FA;
    }
</style>
""", unsafe_allow_html=True)

# ===== SIDEBAR =====
with st.sidebar:
    sidebar_utils.render_logo()
    st.title("Screener Pro")
    
    datas_disponiveis = engine.get_available_dates()
    
    if not datas_disponiveis:
        st.error("Nenhuma data disponível")
        st.stop()
    
    data_ref = st.selectbox("Data de Referência", datas_disponiveis)

# ===== CARREGAR DADOS =====
df_full, erro = engine.load_data(data_ref)

if erro or df_full is None or df_full.empty:
    st.error(f"Erro ao carregar dados: {erro}")
    st.stop()

# Carregar curva ANBIMA e adicionar spreads
curva_df = engine.load_curva_anbima()
curva_disponivel = not curva_df.empty

if curva_disponivel:
    df_full = engine.adicionar_spreads_ao_df(df_full, curva_df)

# ===== CONTEÚDO PRINCIPAL =====
st.title("Screener Pro - Filtros Avançados")
st.markdown(f"**Data de Referência:** {data_ref}")

if curva_disponivel:
    curva_dates = engine.get_curvas_anbima_dates()
    if curva_dates:
        st.caption(f"Curva ANBIMA: {curva_dates[0]}")

st.divider()

# ===== FILTROS ACCORDION =====
st.markdown("### Filtros")

filtros = {}

# Accordion 1 - Filtros de Mercado
with st.expander("Filtros de Mercado", expanded=True):
    col_m1, col_m2 = st.columns(2)
    
    with col_m1:
        # Filtro de Categoria
        categorias_disponiveis = sorted(df_full['categoria_grafico'].unique())
        categorias_selecionadas = st.multiselect(
            "Categoria",
            categorias_disponiveis,
            default=categorias_disponiveis,
            help="Selecione as categorias de debêntures"
        )
        filtros['categoria'] = categorias_selecionadas
        
        # Filtro de Indexador
        indexadores_disponiveis = sorted(df_full['indexador'].unique())
        indexadores_selecionados = st.multiselect(
            "Indexador",
            indexadores_disponiveis,
            help="Selecione os indexadores"
        )
        filtros['indexador'] = indexadores_selecionados if indexadores_selecionados else None
    
    with col_m2:
        # Filtro de Fonte
        fonte = st.selectbox(
            "Fonte de Dados",
            ["Todos"] + sorted(df_full['FONTE'].unique().tolist())
        )
        filtros['fonte'] = fonte
        
        # Filtro de Emissor
        emissores_disponiveis = sorted(df_full['emissor'].unique())
        emissores_selecionados = st.multiselect(
            "Emissor",
            emissores_disponiveis,
            help="Selecione os emissores"
        )
        filtros['emissor'] = emissores_selecionados if emissores_selecionados else None

# Accordion 2 - Filtros de Crédito/Risco
with st.expander("Filtros de Crédito e Risco"):
    col_c1, col_c2 = st.columns(2)
    
    with col_c1:
        # Range de Taxa
        taxa_min, taxa_max = st.slider(
            "Taxa (%)",
            min_value=0.0,
            max_value=float(df_full['taxa'].max()) if df_full['taxa'].max() > 0 else 20.0,
            value=(0.0, float(df_full['taxa'].max()) if df_full['taxa'].max() > 0 else 20.0),
            step=0.1
        )
        filtros['taxa_min'] = taxa_min
        filtros['taxa_max'] = taxa_max
    
    with col_c2:
        # Range de Duration
        duration_min, duration_max = st.slider(
            "Duration (anos)",
            min_value=0.0,
            max_value=float(df_full['duration'].max()) if df_full['duration'].max() > 0 else 10.0,
            value=(0.0, float(df_full['duration'].max()) if df_full['duration'].max() > 0 else 10.0),
            step=0.1
        )
        filtros['duration_min'] = duration_min
        filtros['duration_max'] = duration_max

# Accordion 3 - Filtros de Spread (NOVO)
with st.expander("Filtros de Spread vs Curva ANBIMA"):
    if curva_disponivel and 'spread_bps' in df_full.columns:
        col_s1, col_s2 = st.columns(2)
        
        df_spread_valid = df_full[df_full['spread_bps'].notna()]
        
        if not df_spread_valid.empty:
            spread_min_data = float(df_spread_valid['spread_bps'].min())
            spread_max_data = float(df_spread_valid['spread_bps'].max())
            
            with col_s1:
                spread_min, spread_max = st.slider(
                    "Spread (bps)",
                    min_value=spread_min_data,
                    max_value=spread_max_data,
                    value=(spread_min_data, spread_max_data),
                    step=1.0,
                    help="Spread em relação à curva ANBIMA"
                )
                filtros['spread_min'] = spread_min
                filtros['spread_max'] = spread_max
            
            with col_s2:
                st.info("""
                **Interpretação do Spread:**
                - Spread positivo = taxa acima da curva ANBIMA
                - Spread negativo = taxa abaixo da curva ANBIMA
                """)
        else:
            st.warning("Nenhum título com spread calculado disponível")
    else:
        st.warning("Curva ANBIMA não disponível para cálculo de spreads")

# Accordion 4 - Filtros de Liquidez
with st.expander("Filtros de Liquidez"):
    col_l1, col_l2 = st.columns(2)
    
    with col_l1:
        # Filtro de Cluster Duration
        clusters_disponiveis = sorted(
            df_full['cluster_duration'].unique(),
            key=lambda x: ['Sem Prazo', '0-1 ano', '1-3 anos', '3-5 anos', '5-10 anos', '10+ anos'].index(x) if x in ['Sem Prazo', '0-1 ano', '1-3 anos', '3-5 anos', '5-10 anos', '10+ anos'] else 999
        )
        clusters_selecionados = st.multiselect(
            "Prazo (Cluster)",
            clusters_disponiveis,
            help="Selecione os prazos"
        )
        filtros['cluster'] = clusters_selecionados if clusters_selecionados else None

st.divider()

# ===== APLICAR FILTROS =====
df = engine.apply_filters(df_full, filtros)

# Aplicar filtro de spread adicional
if 'spread_min' in filtros and 'spread_max' in filtros and 'spread_bps' in df.columns:
    df = df[(df['spread_bps'].isna()) | ((df['spread_bps'] >= filtros['spread_min']) & (df['spread_bps'] <= filtros['spread_max']))]

if df.empty:
    st.warning("Nenhum dado disponível com os filtros selecionados")
    st.stop()

# ===== RESULTADOS =====
st.markdown(f"### Resultados: {len(df)} ativos encontrados")

# KPIs
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Ativos Filtrados", f"{len(df):,}")

with col2:
    taxa_media = df[df['taxa'] > 0]['taxa'].mean() if len(df[df['taxa'] > 0]) > 0 else 0
    st.metric("Taxa Média", f"{taxa_media:.2f}%")

with col3:
    duration_media = df[df['duration'] > 0]['duration'].mean() if len(df[df['duration'] > 0]) > 0 else 0
    st.metric("Duration Médio", f"{duration_media:.2f} anos")

with col4:
    ipca_incent = len(df[df['categoria_grafico'] == 'IPCA Incentivado'])
    st.metric("IPCA Incentivados", f"{ipca_incent}")

with col5:
    if curva_disponivel and 'spread_bps' in df.columns:
        df_spread = df[df['spread_bps'].notna()]
        spread_medio = df_spread['spread_bps'].mean() if not df_spread.empty else 0
        st.metric("Spread Médio", f"{spread_medio:.0f} bps")
    else:
        indexadores = df['indexador'].nunique()
        st.metric("Indexadores", f"{indexadores}")

st.divider()

# ===== GRÁFICOS =====
tab1, tab2 = st.tabs(["Risco x Retorno", "Spread x Duration"])

with tab1:
    st.markdown("### Mapa Risco x Retorno")
    
    df_grafico = df[(df['taxa'] > 0) & (df['duration'] > 0)].copy()
    
    if not df_grafico.empty:
        fig_scatter = visuals.create_scatter_risco_retorno(
            df_grafico,
            title="Scatter Plot: Duration (Risco) x Taxa (Retorno)",
            height=600
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        
        st.info("""
        **Interpretação:**
        - **Eixo X (Duration):** Risco de taxa de juros - maior duration = maior sensibilidade
        - **Eixo Y (Taxa):** Retorno esperado
        - **Cores:** Categoria (IPCA Incentivado em verde, CDI+ em azul, etc.)
        - **Símbolos:** Fonte dos dados (SND, Anbima, ou ambos)
        """)
    else:
        st.warning("Dados insuficientes para gerar o gráfico")

with tab2:
    st.markdown("### Spread vs Duration")
    
    if curva_disponivel and 'spread_bps' in df.columns:
        df_spread_plot = df[(df['spread_bps'].notna()) & (df['duration'] > 0)].copy()
        
        if not df_spread_plot.empty:
            # Criar gráfico de Spread x Duration
            fig_spread = px.scatter(
                df_spread_plot,
                x='duration',
                y='spread_bps',
                color='categoria_grafico',
                hover_name='codigo',
                hover_data={
                    'emissor': True,
                    'taxa': ':.2f',
                    'spread_bps': ':.0f',
                    'duration': ':.2f',
                    'taxa_benchmark': ':.2f'
                },
                title="Spread (bps) vs Duration",
                labels={
                    'duration': 'Duration (anos)',
                    'spread_bps': 'Spread (bps)',
                    'categoria_grafico': 'Categoria'
                },
                color_discrete_map={
                    "IPCA Incentivado": "#00CC96",
                    "IPCA Não Incentivado": "#19D3F3",
                    "% CDI": "#AB63FA",
                    "CDI +": "#636EFA",
                    "Prefixado": "#EF553B",
                    "Outros": "#FFA15A"
                }
            )
            
            # Adicionar linha de spread = 0
            fig_spread.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.5)
            
            fig_spread.update_layout(
                template='plotly_dark',
                height=600,
                paper_bgcolor='#0e1117',
                plot_bgcolor='#1a1d24'
            )
            
            st.plotly_chart(fig_spread, use_container_width=True)
            
            st.info("""
            **Interpretação do Spread:**
            - **Acima de 0:** Título paga prêmio sobre a curva ANBIMA (risco de crédito)
            - **Abaixo de 0:** Título está abaixo da curva soberana
            - **IPCA Incentivado:** Comparado com curva IPCA+ ANBIMA
            - **CDI/Pré:** Comparado com curva prefixada ANBIMA
            """)
        else:
            st.warning("Dados insuficientes para gerar o gráfico de spread")
    else:
        st.warning("Curva ANBIMA não disponível para gráfico de spread")

st.divider()

# ===== TABELA DE RESULTADOS =====
st.markdown("### Tabela de Resultados")

# Colunas para exibir
cols_view = ['codigo', 'emissor', 'categoria_grafico', 'indexador', 'taxa', 'duration']

if curva_disponivel and 'spread_bps' in df.columns:
    cols_view.extend(['spread_bps', 'taxa_benchmark'])

cols_view.extend(['pu', 'cluster_duration', 'FONTE'])

cols_disponiveis = [c for c in cols_view if c in df.columns]

# Opções de ordenação
col_sort1, col_sort2 = st.columns([3, 1])

opcoes_ordenacao = ['taxa', 'duration', 'codigo', 'emissor']
if curva_disponivel and 'spread_bps' in df.columns:
    opcoes_ordenacao.insert(2, 'spread_bps')

with col_sort1:
    ordenar_por = st.selectbox(
        "Ordenar por",
        opcoes_ordenacao,
        index=0
    )

with col_sort2:
    ordem = st.radio("Ordem", ['Decrescente', 'Crescente'], horizontal=True)

df_sorted = df[cols_disponiveis].sort_values(
    ordenar_por, 
    ascending=(ordem == 'Crescente'),
    na_position='last'
)

# Formatação
format_dict = {
    'taxa': '{:.2f}%', 
    'duration': '{:.2f}', 
    'pu': '{:.2f}'
}
if 'spread_bps' in cols_disponiveis:
    format_dict['spread_bps'] = '{:.0f}'
if 'taxa_benchmark' in cols_disponiveis:
    format_dict['taxa_benchmark'] = '{:.2f}%'

st.dataframe(
    df_sorted.style.format(format_dict, na_rep='-'),
    use_container_width=True,
    hide_index=True,
    height=500
)

# Botão de Download
csv = df_sorted.to_csv(index=False).encode('utf-8-sig')
st.download_button(
    label="Download CSV",
    data=csv,
    file_name=f"bondtrack_screener_{data_ref.replace('/', '')}.csv",
    mime='text/csv'
)
