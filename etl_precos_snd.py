"""
ETL para extrair dados de Volume Negociado do SND (Sistema Nacional de Debêntures)
Captura preços de negociação e calcula volume total por ativo
Versão Corrigida: Leitura Híbrida (HTML/TXT) e Busca Dinâmica de Cabeçalho
"""
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

# Caminho do banco de dados (Salva dentro da pasta data/)
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "debentures_anbima.db")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads_temp")

# Garante que os diretórios existem
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)


def get_d_minus_1():
    """Retorna D-1 (dia útil anterior)"""
    hoje = datetime.now()
    if hoje.weekday() == 0:  # Segunda
        d1 = hoje - timedelta(days=3)
    elif hoje.weekday() == 6:  # Domingo
        d1 = hoje - timedelta(days=2)
    else:
        d1 = hoje - timedelta(days=1)
    return d1


def extract_snd(data_alvo=None, headless=True, use_system_chrome=True):
    """
    Extrai dados de negociação do SND via web scraping
    """
    print("🚀 [ETL] Iniciando Extração SND - Preços de Negociação...")
    
    if data_alvo is None:
        d1_obj = get_d_minus_1()
    else:
        d1_obj = data_alvo
        
    data_br = d1_obj.strftime('%d/%m/%Y')
    data_link = d1_obj.strftime('%Y%m%d')
    
    print(f"📅 [ETL] Data alvo: {data_br}")
    arquivo_baixado = None

    with sync_playwright() as p:
        print("🕵️ [BROWSER] Abrindo navegador...")
        
        launch_args = {
            "headless": headless,
            "args": ["--ignore-certificate-errors", "--disable-blink-features=AutomationControlled"]
        }
        
        browser = None
        if use_system_chrome:
            try:
                # Tenta canal Chrome estável
                browser = p.chromium.launch(channel="chrome", **launch_args)
            except:
                pass
        
        if browser is None:
            try:
                # Tenta Chromium padrão
                browser = p.chromium.launch(**launch_args)
            except Exception as e:
                print(f"❌ [ERRO] Navegador não iniciado: {e}")
                return None
        
        context = browser.new_context(accept_downloads=True, ignore_https_errors=True)
        page = context.new_page()

        try:
            # Acessa home para gerar cookie de sessão
            page.goto(URL_FORM, timeout=60000)
            
            # Link direto para download (Bypass no formulário)
            link_direto = f"{URL_BASE_DOWNLOAD}?op_exc=False&emissor=&isin=&ativo=&dt_ini={data_link}&dt_fim={data_link}"
            print(f"🔗 [SNIPER] Baixando: {link_direto}")
            
            with page.expect_download(timeout=60000) as download_info:
                try:
                    page.goto(link_direto)
                except:
                    pass # Ignora erro de navegação se o download iniciar

            download = download_info.value
            nome_arquivo = f"snd_precos_{data_link}.xls"
            caminho_final = os.path.join(DOWNLOAD_DIR, nome_arquivo)
            download.save_as(caminho_final)
            print(f"✅ [SUCESSO] Arquivo salvo: {caminho_final}")
            arquivo_baixado = caminho_final

        except Exception as e:
            print(f"❌ [ERRO EXTRAÇÃO]: {e}")
        finally:
            browser.close()
            
    return arquivo_baixado


def transform_data(file_path):
    """
    Transforma os dados brutos. Tenta ler HTML e TXT.
    """
    if not file_path or not os.path.exists(file_path):
        return None
    print("⚙️ [TRANSFORM] Processando arquivo...")
    
    df = None
    
    # 1. TENTATIVA HTML (SND costuma mandar HTML com extensão .xls)
    try:
        with open(file_path, 'rb') as f:
            content = f.read().decode('latin-1', errors='ignore')
        
        # Busca tabelas
        dfs = pd.read_html(io.StringIO(content), decimal=',', thousands='.')
        for d in dfs:
            # Verifica se é a tabela certa procurando colunas chave
            if any(col in str(d.columns) for col in ['Código', 'Emissor', 'Preço']):
                df = d
                print("   -> Formato detectado: HTML Table")
                break
    except Exception as e:
        print(f"   -> Leitura HTML falhou, tentando texto...")

    # 2. TENTATIVA TEXTO/TAB (Fallback)
    if df is None:
        try:
            # Lê tudo e procura onde começa o cabeçalho
            df = pd.read_csv(file_path, sep='\t', encoding='latin-1', on_bad_lines='skip')
            
            # Se a primeira linha não for cabeçalho, procura ela
            colunas_chave = ['Código', 'Emissor', 'PU Médio', 'Quantidade']
            
            # Verifica se o cabeçalho está nas primeiras 20 linhas
            if not any(k in str(df.columns) for k in colunas_chave):
                print("   -> Procurando cabeçalho nas linhas...")
                for i in range(1, 20):
                    df_temp = pd.read_csv(file_path, sep='\t', encoding='latin-1', skiprows=i, on_bad_lines='skip')
                    if any(k in str(df_temp.columns) for k in colunas_chave):
                        df = df_temp
                        print(f"   -> Cabeçalho encontrado na linha {i}")
                        break
        except Exception as e:
            print(f"❌ Erro ao ler arquivo: {e}")
            return None

    if df is None or df.empty:
        print("⚠️ [AVISO] Não foi possível extrair dados estruturados.")
        return None

    # --- NORMALIZAÇÃO DE COLUNAS ---
    # Remove espaços e converte para string
    df.columns = [str(c).strip() for c in df.columns]
    
    mapa = {}
    for col in df.columns:
        c_low = col.lower()
        if 'código' in c_low or 'codigo' in c_low: mapa[col] = 'codigo'
        elif 'emissor' in c_low: mapa[col] = 'emissor'
        elif 'mínimo' in c_low or 'minimo' in c_low: mapa[col] = 'pu_minimo'
        elif 'médio' in c_low or 'medio' in c_low: mapa[col] = 'pu_medio'
        elif 'máximo' in c_low or 'maximo' in c_low: mapa[col] = 'pu_maximo'
        elif 'quantidade' in c_low: mapa[col] = 'quantidade'
        elif 'negócios' in c_low: mapa[col] = 'numero_negocios'

    df = df.rename(columns=mapa)
    
    # Filtra apenas linhas com código válido
    if 'codigo' in df.columns:
        df = df[df['codigo'].notna()]
        df = df[~df['codigo'].astype(str).str.contains('Código', case=False, na=False)]
    else:
        print("❌ Coluna 'Código' não encontrada.")
        return None

    # --- LIMPEZA DE DADOS ---
    cols_num = ['pu_minimo', 'pu_medio', 'pu_maximo', 'quantidade', 'numero_negocios']
    
    for col in cols_num:
        if col in df.columns:
            # Brasileiro (1.000,00) -> Python (1000.00)
            df[col] = df[col].astype(str).str.replace('R$', '', regex=False)
            df[col] = df[col].str.replace('.', '', regex=False)
            df[col] = df[col].str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0

    # Adiciona Datas
    df['data_base'] = get_d_minus_1().strftime('%Y-%m-%d')
    df['data_atualizacao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Calcula Volume
    df['volume_total'] = df['pu_medio'] * df['quantidade']
    
    # Formato Final
    cols_finais = [
        'data_base', 'codigo', 'emissor', 'pu_minimo', 'pu_medio', 
        'pu_maximo', 'quantidade', 'numero_negocios', 'volume_total', 'data_atualizacao'
    ]
    
    # Garante que todas colunas existem
    for c in cols_finais:
        if c not in df.columns: df[c] = None
            
    df_final = df[cols_finais].copy()
    
    # Padroniza código
    if 'codigo' in df_final.columns:
        df_final['codigo'] = df_final['codigo'].astype(str).str.strip().str.upper()

    print(f"📊 [DADOS] {len(df_final)} linhas processadas.")
    return df_final


def load_data(df):
    """Salva no SQLite"""
    if df is None or df.empty:
        return False
        
    print(f"💾 [LOAD] Salvando em: {DB_PATH}")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Cria tabela se não existir
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS negociacao_snd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_base TEXT,
            codigo TEXT,
            emissor TEXT,
            pu_minimo REAL,
            pu_medio REAL,
            pu_maximo REAL,
            quantidade INTEGER,
            numero_negocios INTEGER,
            volume_total REAL,
            data_atualizacao TEXT
        )
        """)
        
        # Remove dados duplicados da mesma data para re-inserir
        data_ref = df['data_base'].iloc[0]
        cursor.execute("DELETE FROM negociacao_snd WHERE data_base = ?", (data_ref,))
        
        df.to_sql('negociacao_snd', conn, if_exists='append', index=False)
        
        conn.commit()
        conn.close()
        print("✅ Dados salvos com sucesso.")
        return True
    except Exception as e:
        print(f"❌ Erro ao salvar no banco: {e}")
        return False


if __name__ == "__main__":
    # Execução principal
    arquivo = extract_snd()
    if arquivo:
        df_tratado = transform_data(arquivo)
        load_data(df_tratado)
        
        # Limpeza
        try:
            os.remove(arquivo)
        except:
            pass
