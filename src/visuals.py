"""Templates e Configurações de Visualização Plotly"""
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
    "Outros": "#B6E880"                    # Verde Claro
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
    Cria scatter plot de risco vs retorno
    
    Args:
        df: DataFrame com os dados
        x_col: Coluna para eixo X (geralmente duration = risco)
        y_col: Coluna para eixo Y (geralmente taxa = retorno)
        color_col: Coluna para cor dos pontos
        size_col: Coluna para tamanho dos pontos
        symbol_col: Coluna para símbolo dos pontos
        hover_name: Coluna para nome no hover
        title: Título do gráfico
        height: Altura do gráfico
    
    Returns:
        Figura Plotly
    """
    fig = px.scatter(
        df, 
        x=x_col, 
        y=y_col,
        color=color_col,
        symbol=symbol_col,
        size=size_col,
        hover_name=hover_name,
        hover_data={"emissor": True, "FONTE": True, "taxa": ":.2f", "duration": ":.2f", 
                    "pu_size": False, color_col: True},
        title=title,
        labels={x_col: "Duration (anos)", y_col: "Taxa (%)"},
        color_discrete_map=COLOR_SCHEME,
        height=height
    )
    
    fig.update_layout(**LAYOUT_DARK)
    fig.update_traces(marker=dict(line=dict(width=0.5, color='white')))
    
    return fig

def create_heatmap_indexador(df, indexadores, cluster_duration_order=None):
    """
    Cria heatmap de taxa média por indexador e duration
    
    Args:
        df: DataFrame com os dados
        indexadores: Lista de indexadores a incluir
        cluster_duration_order: Ordem dos clusters de duration
    
    Returns:
        Figura Plotly
    """
    import numpy as np
    
    if cluster_duration_order is None:
        cluster_duration_order = ["0-1 ano", "1-3 anos", "3-5 anos", "5-10 anos", "10+ anos"]
    
    df_filtered = df[df['indexador'].isin(indexadores)]
    
    pivot_table = df_filtered.pivot_table(
        values='taxa',
        index='indexador',
        columns='cluster_duration',
        aggfunc='mean'
    )
    
    # Reordenar colunas
    pivot_table = pivot_table.reindex(columns=[c for c in cluster_duration_order if c in pivot_table.columns])
    
    # Tratar valores NaN/None - substituir por NaN para não mostrar texto
    values = pivot_table.values.copy()
    
    # Criar texto customizado: mostrar "-" para valores nulos, senão mostrar o valor
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
        x=pivot_table.columns,
        y=pivot_table.index,
        colorscale='Viridis',
        text=text_values,
        texttemplate='%{text}',
        textfont={"size": 12},
        colorbar=dict(title="Taxa (%)"),
        hovertemplate="<b>Indexador:</b> %{y}<br>" +
                      "<b>Duration:</b> %{x}<br>" +
                      "<b>Taxa Média:</b> %{z:.2f}%<extra></extra>"
    ))
    
    fig.update_layout(
        title="Heatmap de Taxas por Indexador e Duration",
        xaxis_title="Duration",
        yaxis_title="Indexador",
        **LAYOUT_DARK,
        height=400
    )
    
    return fig

def create_curva_juros(df, indexador, color="#00CC96"):
    """
    Cria curva de juros para um indexador específico
    
    Args:
        df: DataFrame filtrado por indexador
        indexador: Nome do indexador
        color: Cor da curva
    
    Returns:
        Figura Plotly
    """
    df_sorted = df.sort_values('duration').copy()
    
    # Preparar dados para hover (código e emissor se disponíveis)
    custom_data = []
    for _, row in df_sorted.iterrows():
        codigo = row.get('codigo', 'N/D') if 'codigo' in row.index else 'N/D'
        emissor = row.get('emissor', 'N/D') if 'emissor' in row.index else 'N/D'
        custom_data.append([codigo, emissor])
    
    fig = go.Figure()
    
    # Linha da curva
    fig.add_trace(go.Scatter(
        x=df_sorted['duration'],
        y=df_sorted['taxa'],
        mode='lines+markers',
        name=indexador,
        line=dict(color=color, width=3),
        marker=dict(size=8),
        customdata=custom_data,
        hovertemplate="<b>%{customdata[0]}</b><br>" +
                      "<b>Emissor:</b> %{customdata[1]}<br>" +
                      "<b>Duration:</b> %{x:.2f} anos<br>" +
                      "<b>Taxa:</b> %{y:.2f}%<br>" +
                      f"<b>Indexador:</b> {indexador}<extra></extra>"
    ))
    
    fig.update_layout(
        title=f"Curva de Juros - {indexador}",
        xaxis_title="Duration (anos)",
        yaxis_title="Taxa (%)",
        **LAYOUT_DARK,
        height=400,
        showlegend=False
    )
    
    # Adicionar anotação explicativa
    fig.add_annotation(
        text=f"Cada ponto representa uma debênture indexada a {indexador}",
        xref="paper", yref="paper",
        x=0.5, y=-0.15,
        showarrow=False,
        font=dict(size=10, color="#888888")
    )
    
    return fig

def create_bar_top_movers(df, top_n=10, color="#AB63FA"):
    """
    Cria gráfico de barras dos maiores movimentos (top movers)
    
    Args:
        df: DataFrame com coluna 'variacao' calculada
        top_n: Número de ativos a mostrar
        color: Cor das barras
    
    Returns:
        Figura Plotly
    """
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
    
    fig.update_layout(
        title=f"Top {top_n} Maiores Altas",
        xaxis_title="Variação (%)",
        yaxis_title="",
        **LAYOUT_DARK,
        height=400
    )
    
    return fig

def create_pie_distribuicao(df, values_col='codigo', names_col='categoria_grafico', 
                            title="Distribuição por Categoria", hole=0.5):
    """
    Cria gráfico de pizza/donut
    
    Args:
        df: DataFrame
        values_col: Coluna para valores (ou contagem)
        names_col: Coluna para categorias
        title: Título
        hole: Tamanho do buraco central (0 = pizza, >0 = donut)
    
    Returns:
        Figura Plotly
    """
    # Contar ocorrências
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
    
    fig.update_layout(
        **LAYOUT_DARK,
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
    )
    
    return fig

def create_line_historico(df, codigo, date_col='data_referencia', value_col='taxa', 
                          title="Histórico", color="#00CC96"):
    """
    Cria gráfico de linha temporal
    
    Args:
        df: DataFrame filtrado por código
        codigo: Código do ativo
        date_col: Coluna de data
        value_col: Coluna de valor
        title: Título
        color: Cor da linha
    
    Returns:
        Figura Plotly
    """
    df_sorted = df.sort_values(date_col)
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=pd.to_datetime(df_sorted[date_col], dayfirst=True),
        y=df_sorted[value_col],
        mode='lines+markers',
        name=codigo,
        line=dict(color=color, width=3),
        marker=dict(size=6)
    ))
    
    fig.update_layout(
        title=f"{title} - {codigo}",
        xaxis_title="Data",
        yaxis_title=value_col.capitalize(),
        **LAYOUT_DARK,
        height=400
    )
    
    return fig

def create_box_plot_categoria(df, x_col='categoria_grafico', y_col='taxa', 
                              title="Distribuição de Taxas por Categoria"):
    """
    Cria box plot para análise de distribuição
    
    Args:
        df: DataFrame
        x_col: Coluna para categorias
        y_col: Coluna para valores
        title: Título
    
    Returns:
        Figura Plotly
    """
    fig = px.box(
        df,
        x=x_col,
        y=y_col,
        color=x_col,
        color_discrete_map=COLOR_SCHEME,
        title=title
    )
    
    fig.update_layout(
        **LAYOUT_DARK,
        height=400,
        showlegend=False
    )
    
    return fig

def apply_bondtrack_theme(fig):
    """
    Aplica tema BondTrack a qualquer figura Plotly
    
    Args:
        fig: Figura Plotly
    
    Returns:
        Figura com tema aplicado
    """
    fig.update_layout(**LAYOUT_DARK)
    return fig
