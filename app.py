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

# ===== CSS CUSTOMIZADO (DARK MODE PROFISSIONAL) =====
st.markdown("""
<style>
    /* Background principal */
    .main { background-color: #0e1117; }
    
    /* Métricas */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
        color: #00CC96;
    }
    [data-testid="stMetricLabel"] {
        font-size: 1rem;
        color: #fafafa;
    }
    
    /* Títulos */
    h1 { color: #fafafa; font-weight: 800; }
    h2, h3 { color: #00CC96; }
    
    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #1a1d24; }
    
    /* Cards */
    .stAlert {
        background-color: #1a1d24;
        border-left: 4px solid #00CC96;
    }
    
    /* Botões */
    .stButton>button {
        background-color: #00CC96;
        color: #0e1117;
        font-weight: 600;
        border-radius: 8px;
        border: none;
        padding: 0.5rem 2rem;
    }
    .stButton>button:hover {
        background-color: #AB63FA;
        color: #fafafa;
    }
    
    /* Logo container */
    .logo-container {
        text-align: center;
        padding: 10px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ===== SIDEBAR =====
with st.sidebar:
    # Logo
    logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
    if os.path.exists(logo_path):
       st.image(logo_path, use_container_width=True)
    else:
        st.title("BondTrack")
    
    st.markdown("**Plataforma Profissional de Análise de Debêntures**")
    st.divider()
    
    # --- SELEÇÃO DE DATA COM MEMÓRIA (CRUCIAL PARA NÃO RESETAR) ---
    try:
        datas_disponiveis = engine.get_available_dates()
    except:
        datas_disponiveis = []
    
    if datas_disponiveis:
        # Se não tiver data na memória, usa a primeira (mais recente)
        if 'global_data_ref' not in st.session_state:
            st.session_state['global_data_ref'] = datas_disponiveis[0]
        
        # Validação: Se a data da memória sumiu da lista, reseta
        if st.session_state['global_data_ref'] not in datas_disponiveis:
             st.session_state['global_data_ref'] = datas_disponiveis[0]

        # O widget atualiza a memória
        data_ref = st.selectbox(
            "Data de Referência",
            datas_disponiveis,
            index=datas_disponiveis.index(st.session_state['global_data_ref']),
            key='app_date_widget', # Chave única para este widget
            on_change=lambda: st.session_state.update({'global_data_ref': st.session_state.app_date_widget}),
            help="Selecione a data base para análise"
        )
        # Atualiza a variável principal para uso local
        st.session_state['global_data_ref'] = data_ref
        
    else:
        st.error("Nenhuma data disponível no banco de dados")
        data_ref = None
    
    st.divider()
    
    # Navegação
    st.markdown("### Navegação")
    st.page_link("app.py", label="Home", icon="🏠")
    if os.path.exists("pages/1_Radar_Mercado.py"):
        st.page_link("pages/1_Radar_Mercado.py", label="Radar de Mercado", icon="📡")
    if os.path.exists("pages/2_Screener_Pro.py"):
        st.page_link("pages/2_Screener_Pro.py", label="Screener Pro", icon="🔍")
    if os.path.exists("pages/3_Analise_Ativo.py"):
        st.page_link("pages/3_Analise_Ativo.py", label="Análise de Ativo", icon="📈")
    if os.path.exists("pages/4_Auditoria.py"):
        st.page_link("pages/4_Auditoria.py", label="Auditoria de Dados", icon="🔎")
    
    st.divider()
    
    # Informações do Sistema
    st.markdown("### Sobre")
    st.info("""
    **BondTrack v1.2**
    
    Desenvolvido para análise profissional 
    do mercado de debêntures brasileiro.
    
    Dados: SND + ANBIMA  
    Curva de Juros: ANBIMA  
    Atualização: Diária  
    """)

# ===== CONTEÚDO PRINCIPAL =====
st.title("BondTrack - Central de Inteligência de Debêntures")

# Carregar dados
if data_ref:
    try:
        df, erro = engine.load_data(data_ref)
    except Exception as e:
        st.error(f"Erro crítico: {e}")
        st.stop()
    
    if erro:
        st.error(f"Erro ao carregar dados: {erro}")
        st.stop()
    
    if df is None or df.empty:
        st.warning("Nenhum dado disponível para a data selecionada")
        st.stop()
    
    # Carregar curva ANBIMA e adicionar spreads
    try:
        curva_df = engine.load_curva_anbima(data_ref)
        if not curva_df.empty:
            df = engine.adicionar_spreads_ao_df(df, curva_df)
            curva_disponivel = True
        else:
            curva_disponivel = False
    except:
        curva_disponivel = False
    
    # Carregar dados de volume
    try:
        volume_summary = engine.get_volume_summary()
    except:
        volume_summary = None
    
    # ===== KPIs DO DIA (RESTAURADO) =====
    st.markdown(f"### Indicadores do Mercado - {data_ref}")
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        total_ativos = len(df)
        st.metric("Total de Ativos", f"{total_ativos:,}")
    
    with col2:
        if 'FONTE' in df.columns:
            consolidados = len(df[df['FONTE'] == 'SND + Anbima'])
            pct = (consolidados/total_ativos*100) if total_ativos > 0 else 0
            st.metric("Consolidados", f"{consolidados:,}", f"{pct:.0f}%")
        else:
            st.metric("Consolidados", "N/D")
    
    with col3:
        if 'taxa' in df.columns:
            df_com_preco = df[df['taxa'] > 0]
            taxa_media = df_com_preco['taxa'].mean() if not df_com_preco.empty else 0
            st.metric("Taxa Média", f"{taxa_media:.2f}%")
        else:
            st.metric("Taxa Média", "N/D")
    
    with col4:
        if 'duration' in df.columns:
            duration_media = df[df['duration'] > 0]['duration'].mean() if len(df[df['duration'] > 0]) > 0 else 0
            st.metric("Duration Médio", f"{duration_media:.2f} anos")
        else:
            st.metric("Duration Médio", "N/D")
    
    with col5:
        # Spread médio (se disponível)
        if curva_disponivel and 'spread_bps' in df.columns:
            df_spread = df[df['spread_bps'].notna()]
            spread_medio = df_spread['spread_bps'].mean() if not df_spread.empty else 0
            st.metric("Spread Médio", f"{spread_medio:.0f} bps")
        elif 'indexador' in df.columns:
            indexadores_unicos = df['indexador'].nunique()
            st.metric("Indexadores", f"{indexadores_unicos}")
        else:
            st.metric("Spread Médio", "N/D")
    
    with col6:
        # Volume Total Negociado
        if volume_summary:
            volume_total = volume_summary.get('volume_total', 0)
            if volume_total >= 1_000_000_000:
                volume_str = f"R$ {volume_total/1_000_000_000:.1f}B"
            elif volume_total >= 1_000_000:
                volume_str = f"R$ {volume_total/1_000_000:.1f}M"
            else:
                volume_str = f"R$ {volume_total:,.0f}"
            
            st.metric("Vol. Negociado", volume_str, f"{volume_summary.get('qtd_ativos', 0)} ativos")
        else:
            st.metric("Vol. Negociado", "N/D", "Sem dados")
    
    st.divider()
    
    # ===== VISÃO GERAL DO MERCADO (GRÁFICOS) =====
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.markdown("### Mapa de Risco x Retorno")
        
        if 'taxa' in df.columns and 'duration' in df.columns:
            # Filtrar dados válidos para o gráfico
            df_grafico = df[(df['taxa'] > 0) & (df['duration'] > 0)].copy()
            
            if not df_grafico.empty:
                fig = visuals.create_scatter_risco_retorno(
                    df_grafico,
                    title=f"Curva de Juros - {data_ref}",
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Dados insuficientes para gerar o gráfico")
        else:
            st.warning("Colunas de Taxa ou Duration ausentes.")
    
    with col_right:
        st.markdown("### Distribuição por Categoria")
        
        if 'categoria_grafico' in df.columns:
            if not df.empty:
                fig_pie = visuals.create_pie_distribuicao(
                    df,
                    names_col='categoria_grafico',
                    title="",
                    hole=0.5
                )
                st.plotly_chart(fig_pie, use_container_width=True)
        
        st.markdown("### Distribuição por Fonte")
        
        if 'FONTE' in df.columns:
            fonte_counts = df['FONTE'].value_counts()
            for fonte, count in fonte_counts.items():
                pct = (count / total_ativos * 100)
                st.metric(
                    label=fonte,
                    value=f"{count:,}",
                    delta=f"{pct:.1f}%"
                )
    
    st.divider()
    
    # ===== DESTAQUES DO MERCADO (TABELAS RESTAURADAS) =====
    st.markdown("### Destaques do Mercado")
    
    col_dest1, col_dest2 = st.columns(2)
    
    with col_dest1:
        st.markdown("#### Maiores Taxas")
        cols_taxa = ['codigo', 'emissor', 'taxa', 'indexador']
        if curva_disponivel and 'spread_bps' in df.columns:
            cols_taxa.append('spread_bps')
        
        # Garante colunas
        cols_ok = [c for c in cols_taxa if c in df.columns]
        
        if 'taxa' in df.columns:
            df_top_taxas = df[df['taxa'] > 0].nlargest(5, 'taxa')[cols_ok]
            
            format_dict = {'taxa': '{:.2f}%'}
            if 'spread_bps' in df_top_taxas.columns:
                format_dict['spread_bps'] = '{:.0f} bps'
            
            st.dataframe(
                df_top_taxas.style.format(format_dict),
                hide_index=True,
                use_container_width=True
            )
    
    with col_dest2:
        st.markdown("#### Maiores Durations")
        cols_dur = ['codigo', 'emissor', 'duration', 'indexador']
        cols_ok = [c for c in cols_dur if c in df.columns]
        
        if 'duration' in df.columns:
            df_top_duration = df[df['duration'] > 0].nlargest(5, 'duration')[cols_ok]
            st.dataframe(
                df_top_duration.style.format({'duration': '{:.2f}'}),
                hide_index=True,
                use_container_width=True
            )
    
    col_dest3, col_dest4 = st.columns(2)
    
    with col_dest3:
        st.markdown("#### IPCA Incentivados (Top Spreads)")
        if 'categoria_grafico' in df.columns:
            df_incentivados = df[df['categoria_grafico'] == 'IPCA Incentivado']
            
            if not df_incentivados.empty:
                if 'spread_bps' in df_incentivados.columns:
                    # Se tiver spread, ordena por spread
                    df_show = df_incentivados[df_incentivados['spread_bps'].notna()].nlargest(5, 'spread_bps')
                    cols = ['codigo', 'emissor', 'taxa', 'spread_bps']
                    fmt = {'taxa': '{:.2f}%', 'spread_bps': '{:.0f} bps'}
                else:
                    # Senão, ordena por taxa
                    df_show = df_incentivados.nlargest(5, 'taxa')
                    cols = ['codigo', 'emissor', 'taxa']
                    fmt = {'taxa': '{:.2f}%'}
                
                cols_ok = [c for c in cols if c in df_show.columns]
                
                st.dataframe(
                    df_show[cols_ok].style.format(fmt),
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.info("Nenhum ativo incentivado encontrado.")
        else:
            st.info("Categoria não disponível.")
    
    with col_dest4:
        st.markdown("#### Ativos Mais Negociados")
        try:
            df_top_volume = engine.get_top_volume(5)
            
            if df_top_volume is not None and not df_top_volume.empty:
                # Formatar volume
                if 'volume_total' in df_top_volume.columns:
                    df_top_volume['volume_fmt'] = df_top_volume['volume_total'].apply(
                        lambda x: f"R$ {x/1_000_000:.1f}M" if x >= 1_000_000 else f"R$ {x:,.0f}"
                    )
                    cols_show = ['codigo', 'emissor', 'volume_fmt', 'numero_negocios']
                    cols_final = [c for c in cols_show if c in df_top_volume.columns]
                    
                    df_display = df_top_volume[cols_final].rename(columns={
                        'volume_fmt': 'Volume',
                        'numero_negocios': 'Negócios'
                    })
                    st.dataframe(df_display, hide_index=True, use_container_width=True)
            else:
                st.info("Dados de volume não disponíveis.")
        except:
            st.info("Erro ao buscar volume.")
    
    st.divider()
    
    # ===== RESUMO POR INDEXADOR =====
    st.markdown("### Resumo por Indexador")
    
    if 'indexador' in df.columns:
        indexadores_principais = ['IPCA', 'CDI', 'PRÉ', 'IGP-M']
        resumo_data = []
        
        for idx in indexadores_principais:
            # Filtro case insensitive seguro
            df_idx = df[df['indexador'].str.contains(idx, na=False, case=False)]
            
            if not df_idx.empty:
                row_data = {
                    'Indexador': idx,
                    'Quantidade': len(df_idx),
                    'Taxa Média': df_idx[df_idx['taxa'] > 0]['taxa'].mean() if 'taxa' in df_idx else 0,
                    'Duration Médio': df_idx[df_idx['duration'] > 0]['duration'].mean() if 'duration' in df_idx else 0
                }
                
                if 'spread_bps' in df_idx.columns:
                    df_spr = df_idx[df_idx['spread_bps'].notna()]
                    row_data['Spread Médio (bps)'] = df_spr['spread_bps'].mean() if not df_spr.empty else 0
                
                resumo_data.append(row_data)
        
        if resumo_data:
            df_resumo = pd.DataFrame(resumo_data)
            format_dict = {
                'Quantidade': '{:,.0f}',
                'Taxa Média': '{:.2f}%',
                'Duration Médio': '{:.2f} anos'
            }
            if 'Spread Médio (bps)' in df_resumo.columns:
                format_dict['Spread Médio (bps)'] = '{:.0f}'
            
            st.dataframe(
                df_resumo.style.format(format_dict),
                hide_index=True,
                use_container_width=True
            )
    
    # ===== INFO CURVA ANBIMA =====
    if curva_disponivel:
        with st.expander("Detalhes da Curva de Juros (ANBIMA)"):
            try:
                curva_dates = engine.get_curvas_anbima_dates()
                data_curva = curva_dates[0] if curva_dates else "Desconhecida"
                st.success(f"Dados interpolados da ANBIMA. Referência: {data_curva}")
                
                col_c1, col_c2, col_c3 = st.columns(3)
                
                # Taxa para 2 anos (~504 dias úteis)
                taxa_2y = engine.interpolar_taxa_curva(curva_df, 504, 'taxa_ipca')
                taxa_pre_2y = engine.interpolar_taxa_curva(curva_df, 504, 'taxa_pre')
                infl_2y = engine.interpolar_taxa_curva(curva_df, 504, 'inflacao_implicita')
                
                with col_c1:
                    st.metric("IPCA+ 2 anos", f"{taxa_2y:.2f}%" if taxa_2y else "N/D")
                with col_c2:
                    st.metric("Pré 2 anos", f"{taxa_pre_2y:.2f}%" if taxa_pre_2y else "N/D")
                with col_c3:
                    st.metric("Inflação Implícita 2 anos", f"{infl_2y:.2f}%" if infl_2y else "N/D")
            except Exception as e:
                st.warning(f"Não foi possível calcular detalhes da curva: {e}")

    st.divider()
    
    # ===== ACESSO RÁPIDO =====
    st.markdown("### Acesso Rápido")
    
    col_a, col_b, col_c, col_d = st.columns(4)
    
    with col_a:
        if st.button("Radar de Mercado", use_container_width=True):
            st.switch_page("pages/1_Radar_Mercado.py")
    
    with col_b:
        if st.button("Screener Pro", use_container_width=True):
            st.switch_page("pages/2_Screener_Pro.py")
    
    with col_c:
        if st.button("Análise de Ativo", use_container_width=True):
            st.switch_page("pages/3_Analise_Ativo.py")
    
    with col_d:
        if st.button("Auditoria", use_container_width=True):
            st.switch_page("pages/4_Auditoria.py")

else:
    st.error("Nenhum dado disponível. Verifique se o banco de dados está configurado corretamente.")
    st.info("""
    ### Para começar:
    
    1. Execute o script `extrator_snd.py` para coletar dados do SND
    2. O banco de dados `debentures_anbima.db` deve estar na pasta `/data`
    3. Reinicie a aplicação
    """)

# ===== FOOTER =====
st.divider()
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p><strong>BondTrack v1.2</strong> | Plataforma de Análise de Debêntures</p>
    <p>Dados: SND + ANBIMA | Curva de Juros: ANBIMA | Atualização Diária</p>
</div>
""", unsafe_allow_html=True)
