import os
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
import time
import io
import sys

# --- CONFIGURAÇÕES ---
URL_FORM = "https://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_f.asp"
URL_BASE_DOWNLOAD = "https://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_e.asp"

# Caminhos Absolutos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "debentures_anbima.db")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads_temp")

if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

def log(msg):
    print(f"[ETL PREÇOS] {msg}")
    sys.stdout.flush()

def get_d_minus_1():
    hoje = datetime.now()
    if hoje.weekday() == 0: d1 = hoje - timedelta(days=3)
    elif hoje.weekday() == 6: d1 = hoje - timedelta(days=2)
    else: d1 = hoje - timedelta(days=1)
    return d1

def extract_snd(data_alvo=None, headless=True, use_system_chrome=True):
    log("🚀 Iniciando Extração (Web Scraping)...")
    
    d1_obj = data_alvo if data_alvo else get_d_minus_1()
    data_br = d1_obj.strftime('%d/%m/%Y')
    data_link = d1_obj.strftime('%Y%m%d')
    
    log(f"📅 Data alvo: {data_br}")
    arquivo_baixado = None

    with sync_playwright() as p:
        browser = None
        launch_args = {"headless": headless, "args": ["--ignore-certificate-errors"]}
        
        # Tenta Chrome do Sistema (se permitido)
        if use_system_chrome:
            try:
                browser = p.chromium.launch(channel="chrome", **launch_args)
                log("✅ Usando Chrome do Sistema.")
            except:
                pass
        
        # Fallback para Chromium
        if browser is None:
            try:
                browser = p.chromium.launch(**launch_args)
                log("✅ Usando Chromium (Playwright).")
            except Exception as e:
                log(f"❌ Erro crítico: Navegador não abriu. {e}")
                return None
        
        context = browser.new_context(accept_downloads=True, ignore_https_errors=True)
        page = context.new_page()

        try:
            # Acessa para pegar cookie
            page.goto(URL_FORM, timeout=60000)
            
            # Download Direto
            link = f"{URL_BASE_DOWNLOAD}?op_exc=False&emissor=&isin=&ativo=&dt_ini={data_link}&dt_fim={data_link}"
            log(f"🔗 Baixando: {link}")
            
            with page.expect_download(timeout=60000) as download_info:
                try: page.goto(link)
                except: pass

            download = download_info.value
            fname = f"snd_precos_{data_link}.xls"
            caminho = os.path.join(DOWNLOAD_DIR, fname)
            download.save_as(caminho)
            log(f"✅ Arquivo salvo em: {caminho}")
            arquivo_baixado = caminho

        except Exception as e:
            log(f"❌ Erro no download: {e}")
        finally:
            browser.close()
            
    return arquivo_baixado

def transform_data(file_path):
    if not file_path or not os.path.exists(file_path): return None
    log("⚙️ Processando arquivo...")
    
    df = None
    
    # 1. Tenta ler como HTML
    try:
        with open(file_path, 'rb') as f:
            content = f.read().decode('latin-1', errors='ignore')
        dfs = pd.read_html(io.StringIO(content), decimal=',', thousands='.')
        for d in dfs:
            if any(c in str(d.columns) for c in ['Código', 'Emissor', 'Preço']):
                df = d
                log("   -> Formato HTML detectado.")
                break
    except: pass

    # 2. Tenta ler como TXT
    if df is None:
        try:
            df = pd.read_csv(file_path, sep='\t', encoding='latin-1', on_bad_lines='skip')
            # Busca cabeçalho dinâmico
            cols_chave = ['Código', 'Emissor', 'PU Médio']
            if not any(k in str(df.columns) for k in cols_chave):
                log("   -> Procurando cabeçalho nas linhas...")
                for i in range(1, 20):
                    df_temp = pd.read_csv(file_path, sep='\t', encoding='latin-1', skiprows=i, on_bad_lines='skip')
                    if any(k in str(df_temp.columns) for k in cols_chave):
                        df = df_temp
                        log(f"   -> Cabeçalho achado na linha {i}")
                        break
        except Exception as e:
            log(f"❌ Erro ao ler arquivo: {e}")
            return None

    if df is None or df.empty:
        log("⚠️ Arquivo vazio ou ilegível.")
        return None

    # Normalização
    df.columns = [str(c).strip() for c in df.columns]
    mapa = {}
    for c in df.columns:
        cl = c.lower()
        if 'código' in cl or 'codigo' in cl: mapa[c] = 'codigo'
        elif 'emissor' in cl: mapa[c] = 'emissor'
        elif 'médio' in cl: mapa[c] = 'pu_medio'
        elif 'quantidade' in cl: mapa[c] = 'quantidade'
        elif 'negócios' in cl: mapa[c] = 'numero_negocios'
        elif 'mínimo' in cl: mapa[c] = 'pu_minimo'
        elif 'máximo' in cl: mapa[c] = 'pu_maximo'
    
    df = df.rename(columns=mapa)
    
    if 'codigo' not in df.columns:
        log("❌ Coluna 'Código' não encontrada.")
        return None
        
    df = df[df['codigo'].notna()]
    df = df[~df['codigo'].astype(str).str.contains('Código', case=False)]

    # Limpeza Numérica
    for col in ['pu_medio', 'quantidade', 'pu_minimo', 'pu_maximo', 'numero_negocios']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0

    df['data_base'] = get_d_minus_1().strftime('%Y-%m-%d')
    df['volume_total'] = df['pu_medio'] * df['quantidade']
    df['data_atualizacao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Padroniza Código
    df['codigo'] = df['codigo'].astype(str).str.strip().str.upper()
    
    cols_finais = ['data_base', 'codigo', 'emissor', 'pu_minimo', 'pu_medio', 'pu_maximo', 'quantidade', 'numero_negocios', 'volume_total', 'data_atualizacao']
    for c in cols_finais: 
        if c not in df.columns: df[c] = None
            
    log(f"📊 {len(df)} linhas processadas com sucesso.")
    return df[cols_finais]

def load_data(df):
    if df is None or df.empty: return
    log(f"💾 Salvando no banco: {DB_PATH}")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS negociacao_snd (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_base TEXT, codigo TEXT, emissor TEXT,
                pu_minimo REAL, pu_medio REAL, pu_maximo REAL,
                quantidade INTEGER, numero_negocios INTEGER,
                volume_total REAL, data_atualizacao TEXT
            )
        """)
        
        # Remove duplicatas do dia
        dt = df['data_base'].iloc[0]
        cursor.execute("DELETE FROM negociacao_snd WHERE data_base = ?", (dt,))
        
        df.to_sql('negociacao_snd', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()
        log("✅ Dados salvos com sucesso!")
    except Exception as e:
        log(f"❌ Erro de Banco: {e}")

if __name__ == "__main__":
    # CONFIGURAÇÃO AUTOMÁTICA GITHUB VS LOCAL
    is_github = os.getenv('GITHUB_ACTIONS') == 'true'
    
    # Se for GitHub: Roda escondido (Headless) e NÃO usa Chrome do Sistema
    headless = True if is_github else False
    use_system = False if is_github else True
    
    log(f"Ambiente: {'GITHUB' if is_github else 'LOCAL'} | Headless: {headless}")
    
    arquivo = extract_snd(headless=headless, use_system_chrome=use_system)
    if arquivo:
        df = transform_data(arquivo)
        load_data(df)
        try: os.remove(arquivo)
        except: pass
