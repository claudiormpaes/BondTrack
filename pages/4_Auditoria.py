"""
Auditoria de Dados - Data Quality Center
"""
import streamlit as st
import sys
import os
import pandas as pd

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

import data_engine as engine
import visuals
import sidebar_utils

st.set_page_config(page_title="Auditoria de Dados", page_icon="🔎", layout="wide")

# CSS Customizado
st.markdown("""
<style>
    [data-testid="stMetricValue"] {
        font-size: 1.5rem;
        font-weight: 700;
    }
    h1, h2, h3 {
        color: #EF553B;
    }
    .score-alto {
        color: #00CC96;
        font-size: 3rem;
        font-weight: 900;
    }
    .score-medio {
        color: #FFA15A;
        font-size: 3rem;
        font-weight: 900;
    }
    .score-baixo {
        color: #EF553B;
        font-size: 3rem;
        font-weight: 900;
    }
</style>
""", unsafe_allow_html=True)

# ===== SIDEBAR =====
with st.sidebar:
    sidebar_utils.render_logo()
    st.title("Auditoria de Dados")
    
    datas_disponiveis = engine.get_available_dates()
    
    if not datas_disponiveis:
        st.error("Nenhuma data disponível")
        st.stop()
    
    data_ref = st.selectbox("Data de Referência", datas_disponiveis)
    
    st.divider()
    
    st.markdown("""
    ### O que analisamos?
    
    - **Completude:** % de campos preenchidos
    - **Duplicação:** Ativos repetidos
    - **Inconsistências:** Taxas/durations negativas
    - **Cobertura:** SND vs Anbima
    """)

# ===== CARREGAR DADOS =====
df_full, erro = engine.load_data(data_ref)

if erro or df_full is None or df_full.empty:
    st.error(f"❌ Erro ao carregar dados: {erro}")
    st.stop()

# ===== CONTEÚDO PRINCIPAL =====
st.title("🔎 Centro de Auditoria e Qualidade de Dados")
st.markdown(f"**Data de Referência:** {data_ref}")

st.divider()

# ===== SCORE DE QUALIDADE =====
st.markdown("### 🎯 Score de Qualidade dos Dados")

report = engine.get_data_quality_report(df_full)

score = report['score_qualidade']

# Determinar classe CSS baseada no score
if score >= 80:
    score_class = "score-alto"
    emoji = "🎉"
    status = "Excelente"
elif score >= 60:
    score_class = "score-medio"
    emoji = "⚠️"
    status = "Bom"
else:
    score_class = "score-baixo"
    emoji = "❌"
    status = "Crítico"

col_score1, col_score2, col_score3 = st.columns([1, 2, 1])

with col_score2:
    st.markdown(f"<div style='text-align: center;'><p class='{score_class}'>{emoji} {score:.1f}/100</p><p style='font-size: 1.5rem;'>{status}</p></div>", unsafe_allow_html=True)

st.divider()

# ===== MÉTRICAS GERAIS =====
st.markdown("### 📊 Métricas Gerais")

col_m1, col_m2, col_m3, col_m4 = st.columns(4)

with col_m1:
    st.metric("Total de Registros", f"{report['total_registros']:,}")

with col_m2:
    st.metric("Duplicatas Detectadas", f"{report['duplicatas']}", delta=f"{(report['duplicatas']/report['total_registros']*100):.1f}%" if report['total_registros'] > 0 else "0%")

with col_m3:
    inconsist_count = len(report['inconsistencias'])
    st.metric("Inconsistências", f"{inconsist_count}")

with col_m4:
    snd_anbima = len(df_full[df_full['FONTE'] == 'SND + Anbima'])
    cobertura = (snd_anbima / report['total_registros'] * 100) if report['total_registros'] > 0 else 0
    st.metric("Cobertura SND+Anbima", f"{cobertura:.1f}%")

st.divider()

# ===== COMPLETUDE POR CAMPO =====
st.markdown("### 📋 Análise de Completude por Campo")

if report['campos_completos']:
    completude_data = []
    for campo, stats in report['campos_completos'].items():
        completude_data.append({
            'Campo': campo.upper(),
            'Válidos': stats['validos'],
            'Inválidos': stats['invalidos'],
            'Completude (%)': stats['percentual']
        })
    
    df_completude = pd.DataFrame(completude_data)
    
    # Gráfico de barras
    import plotly.graph_objects as go
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Válidos',
        x=df_completude['Campo'],
        y=df_completude['Válidos'],
        marker_color='#00CC96'
    ))
    
    fig.add_trace(go.Bar(
        name='Inválidos',
        x=df_completude['Campo'],
        y=df_completude['Inválidos'],
        marker_color='#EF553B'
    ))
    
    fig.update_layout(
        barmode='stack',
        title='Completude de Campos Críticos',
        xaxis_title='Campo',
        yaxis_title='Quantidade',
        template='plotly_dark',
        paper_bgcolor='#0e1117',
        plot_bgcolor='#0e1117',
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Tabela detalhada
    st.dataframe(
        df_completude.style.format({'Completude (%)': '{:.2f}%'}),
        hide_index=True,
        use_container_width=True
    )
else:
    st.warning("⚠️ Nenhum dado de completude disponível")

st.divider()

# ===== LOG DE INCONSISTÊNCIAS =====
st.markdown("### ⚠️ Log de Inconsistências")

if report['inconsistencias']:
    for inconsistencia in report['inconsistencias']:
        st.warning(f"⚠️ {inconsistencia}")
else:
    st.success("✅ Nenhuma inconsistência detectada!")

st.divider()

# ===== DUPLICATAS =====
st.markdown("### 🔄 Análise de Duplicatas")

if report['duplicatas'] > 0:
    # Encontrar duplicatas
    if 'codigo' in df_full.columns and 'data_referencia' in df_full.columns:
        df_duplicatas = df_full[df_full.duplicated(subset=['codigo', 'data_referencia'], keep=False)]
        df_duplicatas = df_duplicatas.sort_values(['codigo', 'data_referencia'])
        
        st.warning(f"⚠️ {report['duplicatas']} registros duplicados encontrados")
        
        cols_dup = ['codigo', 'emissor', 'data_referencia', 'taxa', 'duration', 'FONTE']
        cols_disponiveis = [c for c in cols_dup if c in df_duplicatas.columns]
        
        st.dataframe(
            df_duplicatas[cols_disponiveis],
            hide_index=True,
            use_container_width=True,
            height=300
        )
        
        st.info("""
        💡 **Ação Recomendada:**
        - Duplicatas podem ocorrer por múltiplas fontes (SND + Anbima)
        - Verifique se os dados são realmente duplicados ou apenas cruzamento de fontes
        - Considere implementar regra de deduplicação no ETL
        """)
else:
    st.success("✅ Nenhuma duplicata detectada!")

st.divider()

# ===== DISTRIBUIÇÃO POR FONTE =====
st.markdown("### 📊 Distribuição por Fonte de Dados")

col_fonte1, col_fonte2 = st.columns([2, 1])

with col_fonte1:
    fig_fonte = visuals.create_pie_distribuicao(
        df_full,
        names_col='FONTE',
        title="Distribuição de Registros por Fonte",
        hole=0.4
    )
    st.plotly_chart(fig_fonte, use_container_width=True)

with col_fonte2:
    st.markdown("#### Detalhamento")
    
    fonte_counts = df_full['FONTE'].value_counts()
    for fonte, count in fonte_counts.items():
        pct = (count / report['total_registros'] * 100)
        st.metric(
            label=fonte,
            value=f"{count:,}",
            delta=f"{pct:.1f}%"
        )
    
    st.info("""
    **Legenda:**
    - **SND + Anbima:** Dados consolidados
    - **Anbima:** Apenas mercado secundário
    - **SND:** Apenas cadastro
    """)

st.divider()

# ===== CAMPOS VAZIOS POR ATIVO =====
st.markdown("### 🔍 Ativos com Mais Campos Vazios")

# Contar campos vazios por ativo
campos_analise = ['taxa', 'duration', 'pu', 'vencimento', 'emissao']
campos_disponiveis = [c for c in campos_analise if c in df_full.columns]

if campos_disponiveis:
    df_full['campos_vazios'] = df_full[campos_disponiveis].isna().sum(axis=1)
    df_top_vazios = df_full.nlargest(10, 'campos_vazios')[['codigo', 'emissor', 'campos_vazios', 'FONTE']]
    
    st.dataframe(
        df_top_vazios,
        hide_index=True,
        use_container_width=True,
        column_config={
            "campos_vazios": "Campos Vazios (de " + str(len(campos_disponiveis)) + ")"
        }
    )
    
    st.info("""
    💡 **Interpretação:**
    - Ativos apenas do cadastro SND terão mais campos vazios (sem preço de mercado)
    - Ativos consolidados (SND + Anbima) devem ter completude maior
    """)
else:
    st.warning("⚠️ Campos de análise não disponíveis")

st.divider()

# ===== RECOMENDAÇÕES =====
st.markdown("### 💡 Recomendações")

if score >= 80:
    st.success("""
    ✅ **Qualidade Excelente!**
    
    - Dados estão em ótimo estado
    - Continue monitorando diariamente
    - Considere expandir fontes de dados
    """)
elif score >= 60:
    st.warning("""
    ⚠️ **Qualidade Boa, mas pode melhorar:**
    
    - Revise campos com baixa completude
    - Investigue duplicatas
    - Automatize validações no ETL
    """)
else:
    st.error("""
    ❌ **Qualidade Crítica - Ação Necessária:**
    
    - Revisar processo de ETL urgentemente
    - Corrigir inconsistências detectadas
    - Implementar validações automáticas
    - Considerar re-importação dos dados
    """)

st.divider()

# ===== EXPORT DE RELATÓRIO =====
st.markdown("### 💾 Exportar Relatório")

if st.button("📊 Gerar Relatório Completo (JSON)"):
    import json
    
    relatorio_json = json.dumps(report, indent=2, ensure_ascii=False)
    
    st.download_button(
        label="💾 Download Relatório JSON",
        data=relatorio_json,
        file_name=f"bondtrack_auditoria_{data_ref.replace('/', '')}.json",
        mime='application/json'
    )
    
    st.success("✅ Relatório gerado com sucesso!")
