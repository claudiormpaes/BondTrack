"""
Análise de Ativo - Dossiê Completo do Ativo Selecionado
"""
import streamlit as st
import sys
import os
import pandas as pd
from datetime import datetime

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

import data_engine as engine
import visuals
import financial_math as fm
import sidebar_utils

st.set_page_config(page_title="Análise de Ativo", page_icon="📈", layout="wide")

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
    .ficha-tecnica {
        background-color: #1a1d24;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #00CC96;
    }
    .spread-positive { color: #00CC96; font-weight: bold; }
    .spread-negative { color: #EF553B; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ===== SIDEBAR =====
with st.sidebar:
    sidebar_utils.render_logo()
    st.title("Análise de Ativo")
    
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
st.title("Análise Detalhada de Ativo")
st.markdown(f"**Data de Referência:** {data_ref}")

st.divider()

# ===== SELEÇÃO DE ATIVO =====
st.markdown("### Selecione o Ativo")

col_busca1, col_busca2 = st.columns([2, 1])

with col_busca1:
    # Preparar lista de ativos com informações adicionais
    df_full['label_busca'] = df_full['codigo'] + " - " + df_full['emissor'] + " (" + df_full['indexador'] + ")"
    ativos_disponiveis = sorted(df_full['label_busca'].unique())
    
    ativo_selecionado = st.selectbox(
        "Digite ou selecione o código do ativo",
        ativos_disponiveis,
        help="Busque por código, emissor ou indexador"
    )
    
    # Extrair código do label
    codigo_selecionado = ativo_selecionado.split(" - ")[0] if ativo_selecionado else None

with col_busca2:
    # Filtro adicional por categoria
    categorias = sorted(df_full['categoria_grafico'].unique())
    categoria_filtro = st.selectbox("Filtrar por Categoria", ["Todas"] + categorias)

if not codigo_selecionado:
    st.info("Selecione um ativo para começar a análise")
    st.stop()

# Filtrar dados do ativo
df_ativo = df_full[df_full['codigo'] == codigo_selecionado].copy()

if df_ativo.empty:
    st.error(f"Nenhum dado encontrado para o ativo {codigo_selecionado}")
    st.stop()

# Pegar primeira linha (dados mais recentes)
ativo_data = df_ativo.iloc[0]

st.divider()

# ===== HEADER DO ATIVO =====
st.markdown(f"## {codigo_selecionado}")
st.markdown(f"**{ativo_data['emissor']}**")

col_header1, col_header2, col_header3 = st.columns(3)

with col_header1:
    st.markdown(f"**Categoria:** {ativo_data['categoria_grafico']}")
    st.markdown(f"**Indexador:** {ativo_data['indexador']}")

with col_header2:
    st.markdown(f"**Fonte:** {ativo_data['FONTE']}")
    st.markdown(f"**Cluster Duration:** {ativo_data['cluster_duration']}")

with col_header3:
    incentivada = ativo_data.get('incentivada', 'N/D')
    st.markdown(f"**Incentivada:** {incentivada}")

st.divider()

# ===== MÉTRICAS PRINCIPAIS =====
st.markdown("### Métricas Principais")

col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)

taxa = ativo_data['taxa'] if ativo_data['taxa'] > 0 else 0
pu = ativo_data.get('pu', 0)
duration = ativo_data['duration'] if ativo_data['duration'] > 0 else 0

with col_m1:
    st.metric("Taxa Indicativa", f"{taxa:.2f}%")

with col_m2:
    st.metric("PU (Preço Unitário)", f"R$ {pu:.2f}" if pd.notna(pu) and pu > 0 else "N/D")

with col_m3:
    st.metric("Duration", f"{duration:.2f} anos")

with col_m4:
    # Calcular DV01 se possível
    if pd.notna(pu) and pu > 0 and duration > 0:
        dv01 = fm.calcular_dv01(pu, duration)
        st.metric("DV01", f"R$ {dv01:.4f}")
    else:
        st.metric("DV01", "N/D")

with col_m5:
    # Mostrar Spread vs Curva ANBIMA
    if curva_disponivel and 'spread_bps' in ativo_data.index and pd.notna(ativo_data['spread_bps']):
        spread = ativo_data['spread_bps']
        delta_color = "normal" if spread >= 0 else "inverse"
        st.metric(
            "Spread vs ANBIMA",
            f"{spread:.0f} bps",
            delta=f"sobre curva {ativo_data.get('tipo_curva', 'N/D')}"
        )
    else:
        st.metric("Spread vs ANBIMA", "N/D")

st.divider()

# ===== ANÁLISE DE SPREAD (NOVO) =====
if curva_disponivel:
    st.markdown("### Análise de Spread vs Curva ANBIMA")
    
    col_s1, col_s2, col_s3 = st.columns(3)
    
    with col_s1:
        if 'taxa_benchmark' in ativo_data.index and pd.notna(ativo_data['taxa_benchmark']):
            st.metric(
                "Taxa Benchmark (Curva)",
                f"{ativo_data['taxa_benchmark']:.2f}%",
                help="Taxa da curva ANBIMA para o mesmo prazo"
            )
        else:
            st.metric("Taxa Benchmark", "N/D")
    
    with col_s2:
        if 'spread_bps' in ativo_data.index and pd.notna(ativo_data['spread_bps']):
            spread = ativo_data['spread_bps']
            if spread >= 0:
                st.markdown(f"<p style='font-size:24px; color:#00CC96; font-weight:bold;'>+{spread:.0f} bps</p>", unsafe_allow_html=True)
                st.caption("Prêmio sobre a curva ANBIMA")
            else:
                st.markdown(f"<p style='font-size:24px; color:#EF553B; font-weight:bold;'>{spread:.0f} bps</p>", unsafe_allow_html=True)
                st.caption("Abaixo da curva ANBIMA")
        else:
            st.info("Spread não disponível")
    
    with col_s3:
        if 'tipo_curva' in ativo_data.index and pd.notna(ativo_data['tipo_curva']):
            curva_tipo = ativo_data['tipo_curva']
            curva_nome = {
                'taxa_ipca': 'IPCA+ (Real)',
                'taxa_pre': 'Prefixada',
                'inflacao_implicita': 'Inflação Implícita'
            }.get(curva_tipo, curva_tipo)
            st.metric("Curva de Referência", curva_nome)
        else:
            st.metric("Curva de Referência", "N/D")
    
    st.divider()

# ===== FICHA TÉCNICA =====
st.markdown("### Ficha Técnica Completa")

ficha_data = {
    "Campo": [],
    "Valor": []
}

# Campos disponíveis
campos_ficha = [
    ('codigo', 'Código'),
    ('emissor', 'Emissor'),
    ('indexador', 'Indexador'),
    ('taxa', 'Taxa (%)'),
    ('taxa_benchmark', 'Taxa Benchmark ANBIMA (%)'),
    ('spread_bps', 'Spread (bps)'),
    ('pu', 'PU'),
    ('duration', 'Duration (anos)'),
    ('categoria_grafico', 'Categoria'),
    ('cluster_duration', 'Prazo'),
    ('FONTE', 'Fonte'),
    ('vencimento', 'Data de Vencimento'),
    ('emissao', 'Data de Emissão'),
    ('incentivada', 'Debênture Incentivada')
]

for campo, label in campos_ficha:
    if campo in ativo_data.index:
        valor = ativo_data[campo]
        
        # Formatação especial
        if campo == 'taxa' and pd.notna(valor):
            valor = f"{valor:.2f}%"
        elif campo == 'taxa_benchmark' and pd.notna(valor):
            valor = f"{valor:.2f}%"
        elif campo == 'spread_bps' and pd.notna(valor):
            valor = f"{valor:.0f} bps"
        elif campo in ['pu', 'duration'] and pd.notna(valor):
            valor = f"{valor:.2f}"
        elif pd.isna(valor):
            valor = "N/D"
        
        ficha_data["Campo"].append(label)
        ficha_data["Valor"].append(str(valor))

df_ficha = pd.DataFrame(ficha_data)
st.dataframe(df_ficha, hide_index=True, use_container_width=True)

# ===== SIMULAÇÃO DE CENÁRIOS =====
st.markdown("### Simulação de Cenários de Taxa")

if pd.notna(pu) and pu > 0 and duration > 0:
    # Estimar convexidade (placeholder - idealmente precisaríamos dos fluxos)
    convexidade = duration * 0.5  # Aproximação simples
    
    df_cenarios = fm.simular_cenarios_taxa(
        pu_atual=pu,
        duration=duration,
        convexidade=convexidade,
        cenarios=[-0.02, -0.01, 0, 0.01, 0.02]
    )
    
    st.dataframe(
        df_cenarios,
        hide_index=True,
        use_container_width=True,
        column_config={
            "cenario_taxa": "Cenário (variação taxa)",
            "pu_estimado": st.column_config.NumberColumn("PU Estimado", format="R$ %.2f"),
            "variacao_pct": st.column_config.NumberColumn("Variação", format="%.2f%%")
        }
    )
    
    st.info("""
    **Interpretação:**
    - Cenários simulam variações de taxa de juros
    - Valores negativos = queda na taxa = aumento no preço
    - Valores positivos = alta na taxa = queda no preço
    - Cálculo usa Duration e Convexidade estimada
    """)
else:
    st.warning("Dados insuficientes para simulação de cenários")

st.divider()

# ===== COMPARAÇÃO COM SIMILARES =====
st.markdown("### Ativos Similares")

# Buscar ativos similares (mesmo indexador e categoria)
df_similares = df_full[
    (df_full['indexador'] == ativo_data['indexador']) &
    (df_full['categoria_grafico'] == ativo_data['categoria_grafico']) &
    (df_full['codigo'] != codigo_selecionado)
].nlargest(5, 'taxa')

if not df_similares.empty:
    cols_comp = ['codigo', 'emissor', 'taxa', 'duration', 'spread_bps']
    cols_disponiveis = [c for c in cols_comp if c in df_similares.columns]
    
    format_dict = {'taxa': '{:.2f}%', 'duration': '{:.2f}'}
    if 'spread_bps' in cols_disponiveis:
        format_dict['spread_bps'] = '{:.0f}'
    
    st.dataframe(
        df_similares[cols_disponiveis].style.format(format_dict, na_rep='N/D'),
        hide_index=True,
        use_container_width=True
    )
else:
    st.info("Nenhum ativo similar encontrado")
