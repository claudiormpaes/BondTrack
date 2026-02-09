"""
BondTrack - Plataforma Profissional de Análise de Debêntures
Entry Point e Landing Page
"""
import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Tenta importar os módulos
try:
    import data_engine as engine
    import visuals
except ImportError as e:
    st.error(f"Erro ao importar módulos internos: {e}")
    st.stop()

# ===== CONFIGURAÇÃO DA PÁGINA =====
st.set_page_config(
    page_title="BondTrack - Análise de Debêntures",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===== CSS CUSTOMIZADO =====
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    [data-testid="stMetricValue"] { font-size: 1.8rem; font-weight: 700; color: #00CC96; }
    h1 { color: #fafafa; font-weight: 800; }
    h2, h3 { color: #00CC96; }
    [data-testid="stSidebar"] { background-color: #1a1d24; }
    .stAlert { background-color: #1a1d24; border-left: 4px solid #00CC96; }
    .stButton>button { background-color: #00CC96; color: #0e1117; font-weight: 600; border-radius: 8px; border: none; }
    .stButton>button:hover { background-color: #AB63FA; color: #fafafa; }
</style>
""", unsafe_allow_html=True)

# ===== SIDEBAR =====
with st.sidebar:
    logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
    if os.path.exists(logo_path): st.image(logo_path, use_container_width=True)
    else: st.title("BondTrack")
    
    st.markdown("**Plataforma Profissional**")
    st.divider()
    
    try:
        datas_disponiveis = engine.get_available_dates()
    except: datas_disponiveis = []
    
    if datas_disponiveis:
        if 'global_data_ref' not in st.session_state:
            st.session_state['global_data_ref'] = datas_disponiveis[0]
        if st.session_state['global_data_ref'] not in datas_disponiveis:
             st.session_state['global_data_ref'] = datas_disponiveis[0]

        data_ref = st.selectbox(
            "Data de Referência",
            datas_disponiveis,
            index=datas_disponiveis.index(st.session_state['global_data_ref']),
            key='app_date_widget',
            on_change=lambda: st.session_state.update({'global_data_ref': st.session_state.app_date_widget})
        )
        st.session_state['global_data_ref'] = data_ref
    else:
        st.error("Sem dados")
        data_ref = None
    
    st.divider()
    st.markdown("### 📊 Status")
    try:
        db_status = engine.get_database_status_full(data_ref)
        if db_status['snd_negociacao']['loaded']:
            st.caption(f"✅ SND: {db_status['snd_negociacao']['count']} regs")
        if db_status['anbima_indicativa']['loaded']:
            st.caption(f"✅ ANBIMA: {db_status['anbima_indicativa']['count']} regs")
    except: pass
    
    st.divider()
    st.markdown("### Sobre")
    st.info("BondTrack v1.2\nSND + ANBIMA")

# ===== CONTEÚDO PRINCIPAL =====
st.title("BondTrack - Visão Geral")

if data_ref:
    try:
        df, erro = engine.load_data(data_ref)
    except Exception as e:
        st.error(f"Erro crítico: {e}")
        st.stop()
    
    if erro: st.error(erro); st.stop()
    if df is None or df.empty: st.warning("Sem dados"); st.stop()
    
    try:
        curva_df = engine.load_curva_anbima(data_ref)
        if not curva_df.empty:
            df = engine.adicionar_spreads_ao_df(df, curva_df)
            curva_disponivel = True
        else: curva_disponivel = False
    except: curva_disponivel = False
    
    # --- CORREÇÃO DE TIPOS (EVITA O CRASH DO NLARGEST) ---
    # Força a conversão para numérico, transformando erros em NaN
    cols_numericas = ['taxa', 'duration', 'spread_bps', 'volume_total']
    for col in cols_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    # -----------------------------------------------------

    volume_summary = engine.get_volume_summary()
    
    # ===== KPIs =====
    st.markdown(f"### Indicadores - {data_ref}")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    
    total = len(df)
    c1.metric("Ativos", total)
    
    consol = len(df[df['FONTE'] == 'SND + Anbima']) if 'FONTE' in df else 0
    c2.metric("Consolidados", f"{consol}", f"{consol/total*100:.0f}%" if total>0 else "0%")
    
    df_ok = df[df['taxa'] > 0]
    c3.metric("Taxa Média", f"{df_ok['taxa'].mean():.2f}%" if not df_ok.empty else "0%")
    
    df_dur = df[df['duration'] > 0]
    c4.metric("Duration", f"{df_dur['duration'].mean():.2f} anos" if not df_dur.empty else "0")
    
    if curva_disponivel and 'spread_bps' in df.columns:
        # Pega apenas spreads válidos (não nulos) para a média
        df_spr = df[df['spread_bps'].notna()]
        spr = df_spr['spread_bps'].mean() if not df_spr.empty else 0
        c5.metric("Spread", f"{spr:.0f} bps")
    else: c5.metric("Spread", "N/D")
    
    if volume_summary:
        vol = volume_summary['volume_total']
        val_fmt = f"R$ {vol/1e9:.1f}B" if vol > 1e9 else f"R$ {vol/1e6:.1f}M"
        c6.metric("Volume", val_fmt)
    else: c6.metric("Volume", "N/D")
    
    st.divider()
    
    # ===== GRÁFICOS =====
    cl, cr = st.columns([2, 1])
    with cl:
        st.markdown("### Risco x Retorno")
        if 'taxa' in df and 'duration' in df:
            dfg = df[(df['taxa']>0) & (df['duration']>0)].copy()
            if not dfg.empty:
                fig = visuals.create_scatter_risco_retorno(dfg, title=f"Curva {data_ref}", height=500)
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("Sem dados gráficos")
    
    with cr:
        st.markdown("### Categoria")
        if 'categoria_grafico' in df and not df.empty:
            fig2 = visuals.create_pie_distribuicao(df, names_col='categoria_grafico', hole=0.5)
            st.plotly_chart(fig2, use_container_width=True)
            
    st.divider()
    
    # ===== DESTAQUES =====
    st.markdown("### Destaques")
    cd1, cd2 = st.columns(2)
    
    with cd1:
        st.markdown("#### Maiores Taxas")
        if 'taxa' in df:
            cols = ['codigo', 'emissor', 'taxa', 'indexador']
            if 'spread_bps' in df: cols.append('spread_bps')
            cols = [c for c in cols if c in df.columns]
            
            # Filtra NaN antes do nlargest para segurança extra
            df_taxa_clean = df[df['taxa'].notna() & (df['taxa'] > 0)]
            
            if not df_taxa_clean.empty:
                top_tx = df_taxa_clean.nlargest(5, 'taxa')[cols]
                
                st.dataframe(
                    top_tx,
                    column_config={
                        "taxa": st.column_config.NumberColumn("Taxa", format="%.2f%%"),
                        "spread_bps": st.column_config.NumberColumn("Spread", format="%.0f bps"),
                    },
                    hide_index=True, 
                    use_container_width=True
                )
            else:
                st.info("Sem dados de taxa.")
            
    with cd2:
        st.markdown("#### Maiores Durations")
        if 'duration' in df:
            cols = ['codigo', 'emissor', 'duration']
            cols = [c for c in cols if c in df.columns]
            
            df_dur_clean = df[df['duration'].notna() & (df['duration'] > 0)]
            
            if not df_dur_clean.empty:
                top_dur = df_dur_clean.nlargest(5, 'duration')[cols]
                st.dataframe(
                    top_dur,
                    column_config={
                        "duration": st.column_config.NumberColumn("Duration (Anos)", format="%.2f")
                    },
                    hide_index=True, 
                    use_container_width=True
                )
            else:
                st.info("Sem dados de duration.")

    col_d3, col_d4 = st.columns(2)
    
    with col_d3:
        st.markdown("#### IPCA Incentivados")
        if 'categoria_grafico' in df:
            df_inc = df[df['categoria_grafico'] == 'IPCA Incentivado'].copy()
            if not df_inc.empty:
                cols_inc = ['codigo', 'emissor', 'taxa']
                if 'spread_bps' in df: cols_inc.append('spread_bps')
                cols_inc = [c for c in cols_inc if c in df.columns]
                
                # Lógica Segura para Ordenação
                if 'spread_bps' in df_inc.columns and df_inc['spread_bps'].count() > 0:
                    top_inc = df_inc.nlargest(5, 'spread_bps')[cols_inc]
                elif 'taxa' in df_inc.columns:
                    top_inc = df_inc.nlargest(5, 'taxa')[cols_inc]
                else:
                    top_inc = df_inc.head(5)[cols_inc]
                
                st.dataframe(
                    top_inc,
                    column_config={
                        "taxa": st.column_config.NumberColumn("Taxa", format="%.2f%%"),
                        "spread_bps": st.column_config.NumberColumn("Spread", format="%.0f bps"),
                    },
                    hide_index=True, 
                    use_container_width=True
                )
            else:
                st.info("Nenhum IPCA Incentivado.")

    with col_d4:
        st.markdown("#### Maior Volume")
        try:
            df_vol = engine.get_top_volume(5)
            if not df_vol.empty:
                # Garante numérico aqui também
                if 'volume_total' in df_vol.columns:
                    df_vol['volume_total'] = pd.to_numeric(df_vol['volume_total'], errors='coerce')
                
                cols_v = ['codigo', 'emissor', 'volume_total']
                cols_v = [c for c in cols_v if c in df_vol.columns]
                
                st.dataframe(
                    df_vol[cols_v],
                    column_config={
                        "volume_total": st.column_config.NumberColumn("Volume", format="R$ %.0f")
                    },
                    hide_index=True,
                    use_container_width=True
                )
        except: st.info("Sem dados de volume.")

    # ===== RESUMO INDEXADORES =====
    st.markdown("### Indexadores")
    if 'indexador' in df:
        idxs = ['IPCA', 'CDI', 'PRÉ']
        data_res = []
        for i in idxs:
            d = df[df['indexador'].str.contains(i, na=False, case=False)]
            if not d.empty:
                row = {
                    'Indexador': i,
                    'Qtd': len(d),
                    'Taxa Média': d[d['taxa']>0]['taxa'].mean() if 'taxa' in d else 0,
                    'Spread Médio': d[d['spread_bps'].notna()]['spread_bps'].mean() if 'spread_bps' in d else 0
                }
                data_res.append(row)
        
        if data_res:
            dfr = pd.DataFrame(data_res)
            st.dataframe(
                dfr,
                column_config={
                    "Taxa Média": st.column_config.NumberColumn(format="%.2f%%"),
                    "Spread Médio": st.column_config.NumberColumn(format="%.0f bps"),
                },
                hide_index=True, 
                use_container_width=True
            )

    # Acesso Rápido
    st.divider()
    c_a, c_b, c_c = st.columns(3)
    if c_a.button("Radar", use_container_width=True): st.switch_page("pages/1_Radar_Mercado.py")
    if c_b.button("Screener", use_container_width=True): st.switch_page("pages/2_Screener_Pro.py")
    if c_c.button("Auditoria", use_container_width=True): st.switch_page("pages/4_Auditoria.py")

else:
    st.error("Banco de dados não encontrado.")
