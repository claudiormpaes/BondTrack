import pandas as pd
import sqlite3
import requests
import numpy as np
from scipy.interpolate import PchipInterpolator
from io import BytesIO
import datetime
import re
import os

print("🚀 Iniciando ETL FAIR RATE (Motor: ANBIMA)...")

# --- 1. CONFIGURAÇÕES ---
URL_ANBIMA = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"
DB_DIR = os.path.join(os.path.dirname(__file__), 'data')
DB_PATH = os.path.join(DB_DIR, 'curvas_anbima.db')


def get_ultimos_dias_uteis(n=3):
    """
    Retorna lista com os últimos N dias úteis (seg-sex)
    """
    dias = []
    hoje = datetime.datetime.now()
    d = hoje
    
    while len(dias) < n:
        d = d - datetime.timedelta(days=1)
        # Dias úteis: segunda (0) a sexta (4)
        if d.weekday() < 5:
            dias.append(d)
    
    return dias


def baixar_dados_anbima():
    """Baixa dados da ANBIMA e retorna conteúdo + data de referência"""
    try:
        response = requests.get(URL_ANBIMA)
        response.raise_for_status()
        conteudo = response.content.decode('latin-1')
        
        # Detectar data do arquivo
        padrao_data = r"(\d{2}/\d{2}/\d{4})"
        linhas = conteudo.split('\n')
        data_arquivo = datetime.datetime.now().strftime("%d/%m/%Y")
        
        for linha in linhas[:5]:
            match = re.search(padrao_data, linha)
            if match:
                data_arquivo = match.group(1)
                break
        
        return conteudo, data_arquivo
    except Exception as e:
        print(f"❌ Erro no download: {e}")
        return None, None


def parsear_ettj(conteudo):
    """Parseia o conteúdo ANBIMA e retorna DataFrame com ETTJ"""
    linhas = conteudo.split('\n')
    
    ettj_dados = {
        'Vertices': [],
        'ETTJ_IPCA': [],
        'ETTJ_PREF': [],
        'Inflacao_Implicita': []
    }
    
    section = False
    
    for linha in linhas:
        linha = linha.strip()
        
        if "ETTJ Inflação Implicita" in linha or "ETTJ Inflação Implícita" in linha:
            section = True
            continue
            
        if section and "Vertices" in linha:
            continue 
            
        if section and (not linha or 'PREFIXADOS' in linha or 'Erro Título' in linha):
            break
            
        if section and ';' in linha:
            parts = linha.split(';')
            try:
                if len(parts) > 3:
                    v = int(parts[0].replace('.', '').strip())
                    ipca = float(parts[1].replace(',', '.').strip())
                    pre = float(parts[2].replace(',', '.').strip())
                    inf = float(parts[3].replace(',', '.').strip())
                    
                    ettj_dados['Vertices'].append(v)
                    ettj_dados['ETTJ_IPCA'].append(ipca)
                    ettj_dados['ETTJ_PREF'].append(pre)
                    ettj_dados['Inflacao_Implicita'].append(inf)
            except:
                continue

    return pd.DataFrame(ettj_dados)


def interpolar_pchip(df):
    """Aplica interpolação PCHIP ao DataFrame"""
    if df.empty:
        return pd.DataFrame()
    
    df = df.sort_values('Vertices').drop_duplicates(subset=['Vertices'])
    max_dias = df['Vertices'].max()
    novos_vertices = np.arange(1, max_dias + 1)
    
    pchip_ipca = PchipInterpolator(df['Vertices'], df['ETTJ_IPCA'])
    pchip_pre = PchipInterpolator(df['Vertices'], df['ETTJ_PREF'])
    pchip_inf = PchipInterpolator(df['Vertices'], df['Inflacao_Implicita'])
    
    return pd.DataFrame({
        'dias_corridos': novos_vertices,
        'taxa_ipca': pchip_ipca(novos_vertices),
        'taxa_pre': pchip_pre(novos_vertices),
        'inflacao_implicita': pchip_inf(novos_vertices)
    })


def salvar_com_upsert(df_final, data_referencia):
    """
    Salva dados no banco com UPSERT (atualiza se existe, insere se não existe)
    Chave única: data_referencia + dias_corridos
    """
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Criar tabela se não existir (com índice único composto)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS curvas_anbima (
            dias_corridos INTEGER,
            taxa_ipca REAL,
            taxa_pre REAL,
            inflacao_implicita REAL,
            data_referencia TEXT,
            UNIQUE(data_referencia, dias_corridos)
        )
    """)
    
    # Criar índice para buscas rápidas
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_curvas_data ON curvas_anbima(data_referencia)")
    
    # UPSERT: INSERT OR REPLACE
    registros_inseridos = 0
    for _, row in df_final.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO curvas_anbima 
            (dias_corridos, taxa_ipca, taxa_pre, inflacao_implicita, data_referencia)
            VALUES (?, ?, ?, ?, ?)
        """, (
            int(row['dias_corridos']),
            float(row['taxa_ipca']),
            float(row['taxa_pre']),
            float(row['inflacao_implicita']),
            data_referencia
        ))
        registros_inseridos += 1
    
    # Atualizar metadata
    cursor.execute("CREATE TABLE IF NOT EXISTS metadata (chave TEXT PRIMARY KEY, valor TEXT)")
    cursor.execute("INSERT OR REPLACE INTO metadata (chave, valor) VALUES ('ultima_atualizacao', ?)", 
                   (data_referencia,))
    
    conn.commit()
    conn.close()
    
    return registros_inseridos


def processar_dados_anbima():
    """
    Processa dados da ANBIMA.
    NOTA: A ANBIMA disponibiliza apenas dados do dia mais recente via URL.
    O arquivo baixado já contém a data de referência.
    """
    print("⏳ Baixando dados da ANBIMA...")
    
    conteudo, data_arquivo = baixar_dados_anbima()
    
    if not conteudo:
        return
    
    print(f"📅 Data de Referência encontrada: {data_arquivo}")
    
    # Parsear dados
    df_raw = parsear_ettj(conteudo)
    
    if df_raw.empty:
        print("⚠️ Atenção: A tabela veio vazia.")
        return
    
    # Interpolar
    print("➗ Calculando Interpolação (PCHIP)...")
    df_final = interpolar_pchip(df_raw)
    
    if df_final.empty:
        print("❌ Erro na interpolação.")
        return
    
    # Salvar com UPSERT
    registros = salvar_com_upsert(df_final, data_arquivo)
    
    print(f"💾 Sucesso! {registros} linhas salvas/atualizadas para {data_arquivo}")
    
    # Mostrar estatísticas do banco
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT data_referencia) FROM curvas_anbima")
    total_datas = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM curvas_anbima")
    total_registros = cursor.fetchone()[0]
    conn.close()
    
    print(f"📊 Total no banco: {total_datas} datas, {total_registros} registros")


if __name__ == "__main__":
    processar_dados_anbima()
