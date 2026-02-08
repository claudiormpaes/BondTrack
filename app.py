"""
BondTrack - Plataforma Profissional de Análise de Debêntures
Entry Point e Landing Page
"""
import streamlit as st
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    import data_engine as engine
    import visuals
except ImportError as e:
    st.error(f"Erro imports: {e}")
    st.stop()

st.set_page_config(page_title="BondTrack", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

# ===== SIDEBAR COM PERSISTÊNCIA =====
with st.sidebar:
    logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
    if os.path.exists(logo_path): st.image(logo_path, use_container_width=True)
    else: st.title("BondTrack")
    
    try:
        datas = engine.get_available_dates()
    except: datas = []

    if datas:
        # --- LÓGICA DE MEMÓRIA (SESSION STATE) ---
        # Se não tiver data na memória, usa a primeira (mais recente)
        if 'global_data_ref' not in st.session_state:
            st.session_state['global_data_ref'] = datas[0]
        
        # Se a data da memória não estiver na lista (ex: lista atualizou), reseta
        if st.session_state['global_data_ref'] not in datas:
             st.session_state['global_data_ref'] = datas[0]

        # O widget atualiza a memória
        data_ref = st.selectbox(
            "Data de Referência",
            datas,
            index=datas.index(st.session_state['global_data_ref']),
            key='global_data_ref_widget', # Chave interna do widget
            on_change=lambda: st.session_state.update({'global_data_ref': st.session_state.global_data_ref_widget})
        )
        # Atualiza a variável principal
        st.session_state['global_data_ref'] = data_ref
        
    else:
        st.warning("Sem datas.")
        st.stop()
    
    st.divider()
    st.page_link("app.py", label="Home", icon="🏠")
    if os.path.exists("pages/1_Radar_Mercado.py"): st.page_link("pages/1_Radar_Mercado.py", label="Radar", icon="📡")
    if os.path.exists("pages/2_Screener_Pro.py"): st.page_link("pages/2_Screener_Pro.py", label="Screener", icon="🔍")
    if os.path.exists("pages/3_Analise_Ativo.py"): st.page_link("pages/3_Analise_Ativo.py", label="Análise", icon="📈")
    if os.path.exists("pages/4_Auditoria.py"): st.page_link("pages/4_Auditoria.py", label="Auditoria", icon="🔎")

# ===== CONTEÚDO PRINCIPAL =====
st.title(f"BondTrack - Visão Geral ({data_ref})")

df, erro = engine.load_data(data_ref)
if erro: st.error(erro); st.stop()
if df.empty: st.warning("Sem dados."); st.stop()

curva_df = engine.load_curva_anbima(data_ref)
if not curva_df.empty: df = engine.adicionar_spreads_ao_df(df, curva_df)

# (O restante do layout de KPIs e gráficos continua igual, só a lógica de data mudou)
# ... Código de KPIs e Gráficos ...
col1, col2, col3, col4 = st.columns(4)
with col1: st.metric("Total Ativos", len(df))
with col2: st.metric("Taxa Média", f"{df[df['taxa']>0]['taxa'].mean():.2f}%" if 'taxa' in df else "0")
with col3: st.metric("Duration Médio", f"{df[df['duration']>0]['duration'].mean():.2f}" if 'duration' in df else "0")
with col4: st.metric("Vol. Negociado", "Ver Detalhes")

st.divider()
col_L, col_R = st.columns([2,1])
with col_L:
    if 'taxa' in df and 'duration' in df:
        fig = visuals.create_scatter_risco_retorno(df[(df['taxa']>0)], title=f"Risco x Retorno ({data_ref})")
        st.plotly_chart(fig, use_container_width=True)
with col_R:
    if 'categoria_grafico' in df:
        fig2 = visuals.create_pie_distribuicao(df, names_col='categoria_grafico')
        st.plotly_chart(fig2, use_container_width=True)
