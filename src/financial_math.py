"""Cálculos Financeiros - Duration, Convexidade, Spreads"""
import pandas as pd
import numpy as np
from datetime import datetime

def calcular_duration_macaulay(fluxos, taxa_desconto):
    """
    Calcula Duration de Macaulay
    
    Args:
        fluxos: Lista de tuplas (periodo_anos, valor_fluxo)
        taxa_desconto: Taxa de desconto anual (ex: 0.12 para 12%)
    
    Returns:
        Duration em anos
    """
    if not fluxos or taxa_desconto < 0:
        return 0
    
    vp_total = 0
    vp_ponderado = 0
    
    for periodo, valor in fluxos:
        vp = valor / ((1 + taxa_desconto) ** periodo)
        vp_total += vp
        vp_ponderado += periodo * vp
    
    return vp_ponderado / vp_total if vp_total > 0 else 0

def calcular_duration_modified(duration_macaulay, taxa_desconto, freq_cupom=2):
    """
    Calcula Duration Modificada
    
    Args:
        duration_macaulay: Duration de Macaulay em anos
        taxa_desconto: Taxa de desconto anual
        freq_cupom: Frequência de pagamento de cupom por ano (2 = semestral)
    
    Returns:
        Duration modificada
    """
    return duration_macaulay / (1 + taxa_desconto / freq_cupom)

def calcular_convexidade(fluxos, taxa_desconto):
    """
    Calcula Convexidade
    
    Args:
        fluxos: Lista de tuplas (periodo_anos, valor_fluxo)
        taxa_desconto: Taxa de desconto anual
    
    Returns:
        Convexidade
    """
    if not fluxos or taxa_desconto < 0:
        return 0
    
    vp_total = 0
    convexidade_soma = 0
    
    for periodo, valor in fluxos:
        vp = valor / ((1 + taxa_desconto) ** periodo)
        vp_total += vp
        convexidade_soma += periodo * (periodo + 1) * vp
    
    return convexidade_soma / (vp_total * ((1 + taxa_desconto) ** 2)) if vp_total > 0 else 0

def calcular_spread(taxa_ativo, taxa_benchmark):
    """
    Calcula spread simples em pontos base
    
    Args:
        taxa_ativo: Taxa do ativo (em %)
        taxa_benchmark: Taxa do benchmark (em %)
    
    Returns:
        Spread em pontos base (bps)
    """
    return (taxa_ativo - taxa_benchmark) * 100

def estimar_preco_mudanca_taxa(preco_atual, duration_mod, convexidade, delta_taxa):
    """
    Estima variação de preço usando Duration e Convexidade
    
    Args:
        preco_atual: Preço atual do título
        duration_mod: Duration modificada
        convexidade: Convexidade do título
        delta_taxa: Variação da taxa (em decimal, ex: 0.01 para +1%)
    
    Returns:
        Novo preço estimado
    """
    variacao_preco = (-duration_mod * delta_taxa + 0.5 * convexidade * (delta_taxa ** 2)) * preco_atual
    return preco_atual + variacao_preco

def calcular_ytm_aproximado(pu, taxa_cupom, anos_vencimento, valor_face=1000):
    """
    Calcula YTM (Yield to Maturity) aproximado
    Método simplificado - para cálculo preciso usar solver numérico
    
    Args:
        pu: Preço unitário
        taxa_cupom: Taxa do cupom anual (em %)
        anos_vencimento: Anos até vencimento
        valor_face: Valor de face do título
    
    Returns:
        YTM aproximado (em %)
    """
    if anos_vencimento <= 0 or pu <= 0:
        return 0
    
    cupom_anual = (taxa_cupom / 100) * valor_face
    
    # Fórmula aproximada
    ytm = (cupom_anual + (valor_face - pu) / anos_vencimento) / ((valor_face + pu) / 2)
    
    return ytm * 100  # Retorna em percentual

def calcular_retorno_periodo(pu_inicial, pu_final, cupons_recebidos=0):
    """
    Calcula retorno total do período
    
    Args:
        pu_inicial: PU no início do período
        pu_final: PU no final do período
        cupons_recebidos: Soma dos cupons recebidos no período
    
    Returns:
        Retorno em percentual
    """
    if pu_inicial <= 0:
        return 0
    
    return ((pu_final - pu_inicial + cupons_recebidos) / pu_inicial) * 100

def calcular_duration_portfolio(pesos, durations):
    """
    Calcula duration de um portfólio
    
    Args:
        pesos: Lista com pesos de cada ativo (soma = 1)
        durations: Lista com duration de cada ativo
    
    Returns:
        Duration do portfólio
    """
    if len(pesos) != len(durations):
        return 0
    
    return sum([p * d for p, d in zip(pesos, durations)])

def calcular_dv01(preco, duration_modified):
    """
    Calcula DV01 (Dollar Value of 01)
    Variação em R$ para mudança de 1 ponto base (0.01%) na taxa
    
    Args:
        preco: Preço do título
        duration_modified: Duration modificada
    
    Returns:
        DV01 em R$
    """
    return preco * duration_modified * 0.0001

def classificar_risco_credito(rating):
    """
    Classifica risco de crédito baseado em rating
    
    Args:
        rating: Rating da debênture (ex: 'AAA', 'AA+', 'BBB-')
    
    Returns:
        Categoria de risco: 'Baixo', 'Médio', 'Alto', 'Muito Alto'
    """
    rating_upper = str(rating).upper().strip()
    
    if any(x in rating_upper for x in ['AAA', 'AA']):
        return 'Baixo'
    elif any(x in rating_upper for x in ['A', 'BBB']):
        return 'Médio'
    elif any(x in rating_upper for x in ['BB', 'B']):
        return 'Alto'
    else:
        return 'Muito Alto'

def simular_cenarios_taxa(pu_atual, duration, convexidade, cenarios=[-0.02, -0.01, 0, 0.01, 0.02]):
    """
    Simula preços para diferentes cenários de taxa
    
    Args:
        pu_atual: PU atual
        duration: Duration modificada
        convexidade: Convexidade
        cenarios: Lista de variações de taxa (ex: -0.02 = -2%)
    
    Returns:
        DataFrame com cenários e preços estimados
    """
    resultados = []
    
    for delta in cenarios:
        novo_pu = estimar_preco_mudanca_taxa(pu_atual, duration, convexidade, delta)
        variacao_pct = ((novo_pu - pu_atual) / pu_atual) * 100
        
        resultados.append({
            'cenario_taxa': f"{delta*100:+.1f}%",
            'pu_estimado': round(novo_pu, 2),
            'variacao_pct': round(variacao_pct, 2)
        })
    
    return pd.DataFrame(resultados)
