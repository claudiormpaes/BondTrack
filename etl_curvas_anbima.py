import pandas as pd
import sqlite3
import requests
import numpy as np
from scipy.interpolate import PchipInterpolator
import datetime
import re
import os
import sys

# --- CONFIGURAÇÕES ---
URL_ANBIMA = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"

# Define caminhos absolutos para evitar erro de pasta
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "curvas_anbima.db")

# Garante que a pasta data existe
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def processar_dados_anbima():
    print(f"🚀 Iniciando ETL FAIR RATE (Motor: ANBIMA)...")
    
    # 1. Download do Arquivo
    print("⏳ Baixando dados da ANBIMA...")
    try:
        response = requests.get(URL_ANBIMA)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Erro no download: {e}")
        return

    # 2. Ler o conteúdo
    conteudo = response.content.decode('latin-1')
    linhas = conteudo.split('\n')
    
    print("✅ Download concluído. Processando arquivo...")

    # --- IDENTIFICAR A DATA DO ARQUIVO ---
    data_arquivo = datetime.datetime.now().strftime("%d/%m/%Y") 
    padrao_data = r"(\d{2}/\d{2}/\d{4})"
    
    for linha in linhas[:10]:
        match = re.search(padrao_data, linha)
        if match:
            data_arquivo = match.group(1)
            print(f"📅 Data de Referência encontrada no arquivo: {data_arquivo}")
            break
            
    # 3. Parser (Extração dos dados)
    ettj_dados = {
        'Vertices': [],
        'ETTJ_IPCA': [],
        'ETTJ_PREF': [],
        'Inflacao_Implicita': []
    }
    
    section = False
    
    for linha in linhas:
        linha = linha.strip()
        
        # Identifica o início da tabela certa
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

    df = pd.DataFrame(ettj_dados)
    
    if df.empty:
        print("⚠️ Atenção: A tabela veio vazia. Verifique se o layout da ANBIMA mudou.")
        return

    # 4. Interpolação PCHIP
    print("➗ Calculando Interpolação (PCHIP)...")
    df = df.sort_values('Vertices').drop_duplicates(subset=['Vertices'])
    
    max_dias = df['Vertices'].max()
    novos_vertices = np.arange(1, max_dias + 1)
    
    try:
        pchip_ipca = PchipInterpolator(df['Vertices'], df['ETTJ_IPCA'])
        pchip_pre = PchipInterpolator(df['Vertices'], df['ETTJ_PREF'])
        pchip_inf = PchipInterpolator(df['Vertices'], df['Inflacao_Implicita'])
        
        df_final = pd.DataFrame({
            'dias_corridos': novos_vertices,
            'taxa_ipca': pchip_ipca(novos_vertices),
            'taxa_pre': pchip_pre(novos_vertices),
            'inflacao_implicita': pchip_inf(novos_vertices)
        })
        
        df_final['data_referencia'] = data_arquivo
        
        # 5. Salvar no Banco
        print(f"💾 Salvando banco em: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        
        # Salva tabela (append ou replace? Replace para atualizar a curva do dia)
        # Se quiser histórico, mude para append, mas curvas costumam ser sobrescritas ou ter data na chave
        df_final.to_sql('curvas_anbima', conn, if_exists='replace', index=False)
        
        # Metadata
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS metadata (chave TEXT PRIMARY KEY, valor TEXT)")
        cursor.execute("INSERT OR REPLACE INTO metadata (chave, valor) VALUES ('ultima_atualizacao', ?)", (data_arquivo,))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Sucesso! {len(df_final)} linhas salvas.")
        
    except Exception as e:
        print(f"❌ Erro na interpolação ou salvamento: {e}")

if __name__ == "__main__":
    processar_dados_anbima()
