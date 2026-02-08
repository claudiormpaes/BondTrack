"""
Visuals - Módulo de Geração de Gráficos
Responsável por criar os objetos de figura Plotly para o App.
Blindado contra colunas ausentes e erros de renderização.
"""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Paleta de Cores BondTrack (Dark Mode)
COLOR_SCHEME = {
    "IPCA Incentivado": "#00CC96",        # Verde Neon
    "IPCA Não Incentivado": "#EF553B",    # Vermelho/Laranja
    "CDI +": "#636EFA",                    # Azul
    "% CDI": "#AB63FA",                    # Roxo Neon
    "Prefixado": "#FFA15A",               # Laranja
    "Outros": "#B6E880",                   # Verde Claro
    "Geral": "#00CC96"                     # Fallback
}

# Configuração Base Dark Mode
LAYOUT_DARK = {
    "template": "plotly_dark",
    "paper_bgcolor": "#0e1117",
    "plot_bgcolor": "#0e1117",
    "font": {"color": "#fafafa", "family": "Arial, sans-serif"},
    "xaxis": {"gridcolor": "#2d3139"},
    "yaxis": {"gridcolor": "#2d3139"}
}

def create_scatter_risco_retorno(df, x_col="duration", y_col="taxa", color_col="categoria_grafico", 
                                 size_col="pu_size", symbol_col="FONTE", hover_name="codigo",
                                 title="Mapa Risco x Retorno", height=600):
    """
    Cria scatter plot de risco vs retorno de forma segura.
    """
    if df.empty:
        return go.Figure()

    # --- PROTEÇÃO CONTRA COLUNAS FALTANTES ---
    df_plot = df.copy()
    
    # 1. Garante colunas de Eixo X e Y
    if x_col not in df_plot.columns or y_col not in df_plot.columns:
        return go.Figure().add_annotation(text="Dados insuficientes para o gráfico", showarrow=False)

    # 2. Garante Cor (Categoria)
    if color_col not in df_plot.columns:
        df_plot[color_col] = "Geral"

    # 3. Garante Símbolo (Fonte)
    if symbol_col not in df_plot.columns:
        df_plot[symbol_col] = "Desconhecida"

    # 4. Garante Tamanho (PU) - CRÍTICO PARA O ERRO DO DIA 04/02
    use_size = False
    if size_col in df_plot.columns:
        # Verifica se não é tudo nulo
        if not df_plot[size_col].isna().all():
            df_plot[size_col] = df_plot[size_col].fillna(1000)
            use_size = True
    
    # Se não tiver coluna de tamanho segura, passamos None para o Plotly
    final_size_col = size_col if use_size else None

    # 5. Prepara Hover Data (Tooltips)
    hover_data = {x_col: ":.2f", y_col: ":.2f"}
    
    # Adiciona colunas extras ao tooltip apenas se existirem
    for col in ["emissor", symbol_col, color_col]:
        if col in df_plot.columns:
            hover_data[col] = True
            
    try:
        fig = px.scatter(
            df_plot, 
            x=x_col, 
            y=y_col,
            color=color_col,
            symbol=symbol_col if symbol_col in df_plot.columns else None,
            size=final_size_col, # Usa None se a coluna não existir
            hover_name=hover_name if hover_name in df_plot.columns else None,
            hover_data=hover_data,
            title=title,
            labels={x_col: "Duration (anos)", y_col: "Taxa (%)", size_col: "Preço"},
            color_discrete_map=COLOR_SCHEME,
            height=height
        )
        
        fig.update_layout(**LAYOUT_DARK)
        fig.update_traces(marker=dict(line=dict(width=0.5, color='white')))
        
        # Se não usou coluna de tamanho, define um tamanho fixo visível
        if not final_size_col:
            fig.update_traces(marker=dict(size=10))
        
        return fig

    except Exception as e:
        # Fallback de segurança máxima
        fig = go.Figure()
        fig.add_annotation(text=f"Erro ao renderizar gráfico: {e}", showarrow=False)
        return fig

def create_heatmap_indexador(df, indexadores, cluster_duration_order=None):
    """
    Cria heatmap de taxa média por indexador e duration
    """
    if df.empty: return go.Figure()

    # Proteção: Verifica se as colunas necessárias existem
    if 'indexador' not in df.columns or 'cluster_duration' not in df.columns or 'taxa' not in df.columns:
        return go.Figure()

    if cluster_duration_order is None:
        cluster_duration_order = ["0-1 ano", "1-3 anos", "3-5 anos", "5-10 anos", "10+ anos"]
    
    try:
        df_filtered = df[df['indexador'].isin(indexadores)]
        
        if df_filtered.empty: return go.Figure()
        
        pivot_table = df_filtered.pivot_table(
            values='taxa',
            index='indexador',
            columns='cluster_duration',
            aggfunc='mean'
        )
        
        # Reordenar colunas
        pivot_table = pivot_table.reindex(columns=[c for c in cluster_duration_order if c in pivot_table.columns])
        
        values = pivot_table.values
        x_cols = pivot_table.columns
        y_idxs = pivot_table.index
        
        # Criar texto customizado
        text_values = []
        for row in values:
            text_row = []
            for val in row:
                if pd.isna(val) or val == 0:
                    text_row.append("-")
                else:
                    text_row.append(f"{val:.2f}%")
            text_values.append(text_row)
        
        fig = go.Figure(data=go.Heatmap(
            z=values,
            x=x_cols,
            y=y_idxs,
            colorscale='Viridis',
            text=text_values,
            texttemplate='%{text}',
            textfont={"size": 12},
            colorbar=dict(title="Taxa (%)")
        ))
        
        fig.update_layout(
            title="Heatmap de Taxas por Indexador e Duration",
            xaxis_title="Duration",
            yaxis_title="Indexador",
            **LAYOUT_DARK,
            height=400
        )
        
        return fig
    except Exception:
        return go.Figure()

def create_curva_juros(df, indexador, color="#00CC96"):
    """
    Cria curva de juros para um indexador específico
    """
    if df.empty or 'duration' not in df.columns or 'taxa' not in df.columns:
        return go.Figure()

    try:
        df_sorted = df.sort_values('duration').copy()
        
        # Hover seguro
        custom_data = []
        has_cod = 'codigo' in df_sorted.columns
        has_emi = 'emissor' in df_sorted.columns
        
        for _, row in df_sorted.iterrows():
            codigo = row['codigo'] if has_cod else 'N/D'
            emissor = row['emissor'] if has_emi else 'N/D'
            custom_data.append([codigo, emissor])
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df_sorted['duration'],
            y=df_sorted['taxa'],
            mode='lines+markers',
            name=indexador,
            line=dict(color=color, width=3),
            marker=dict(size=8),
            customdata=custom_data,
            hovertemplate="<b>%{customdata[0]}</b><br>Emissor: %{customdata[1]}<br>Duration: %{x:.2f}<br>Taxa: %{y:.2f}%<extra></extra>"
        ))
        
        fig.update_layout(
            title=f"Curva de Juros - {indexador}",
            xaxis_title="Duration (anos)",
            yaxis_title="Taxa (%)",
            **LAYOUT_DARK,
            height=400,
            showlegend=False
        )
        
        return fig
    except Exception:
        return go.Figure()

def create_bar_top_movers(df, top_n=10, color="#AB63FA"):
    """Cria gráfico de barras simples"""
    if df.empty or 'variacao' not in df.columns: return go.Figure()
    
    try:
        df_sorted = df.nlargest(top_n, 'variacao')
        fig = go.Figure([
            go.Bar(
                x=df_sorted['variacao'],
                y=df_sorted['codigo'],
                orientation='h',
                marker=dict(color=color),
                text=df_sorted['variacao'].apply(lambda x: f"{x:+.2f}%"),
                textposition='outside'
            )
        ])
        fig.update_layout(title=f"Top {top_n} Maiores Altas", **LAYOUT_DARK, height=400)
        return fig
    except: return go.Figure()

def create_pie_distribuicao(df, values_col='codigo', names_col='categoria_grafico', title="Distribuição", hole=0.5):
    """Cria gráfico de pizza seguro"""
    if df.empty or names_col not in df.columns: return go.Figure()
    
    try:
        contagem = df[names_col].value_counts().reset_index()
        contagem.columns = [names_col, 'count']
        
        fig = px.pie(
            contagem,
            values='count',
            names=names_col,
            hole=hole,
            color=names_col,
            color_discrete_map=COLOR_SCHEME,
            title=title
        )
        fig.update_layout(**LAYOUT_DARK, height=400)
        return fig
    except: return go.Figure()

def apply_bondtrack_theme(fig):
    fig.update_layout(**LAYOUT_DARK)
    return fig
