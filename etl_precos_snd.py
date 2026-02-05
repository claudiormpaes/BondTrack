"""
ETL para extrair dados de Volume Negociado do SND (Sistema Nacional de Debêntures)
Captura preços de negociação e calcula volume total por ativo
"""
import os
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
import time
import io

# --- CONFIGURAÇÕES ---
URL_FORM = "https://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_f.asp"
URL_BASE_DOWNLOAD = "https://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_e.asp"

# Caminho do banco de dados (relativo ao projeto)
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "debentures_anbima.db")

DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads_temp")

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
    Args:
        data_alvo: datetime object para data específica (opcional, padrão D-1)
        headless: Se True, roda sem janela visível (padrão True)
        use_system_chrome: Se True, usa Chrome do sistema ao invés do Chromium do Playwright
    Returns:
        Caminho do arquivo baixado ou None
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
        
        # Tenta usar Chrome do sistema primeiro (não precisa de playwright install)
        # Se falhar, tenta usar Chromium do Playwright
        browser = None
        launch_args = {
            "headless": headless,
            "args": ["--ignore-certificate-errors", "--disable-blink-features=AutomationControlled"]
        }
        
        if use_system_chrome:
            try:
                print("   -> Tentando usar Chrome do sistema...")
                browser = p.chromium.launch(channel="chrome", **launch_args)
                print("   ✅ Chrome do sistema encontrado!")
            except Exception as e:
                print(f"   ⚠️ Chrome não encontrado: {e}")
                browser = None
        
        if browser is None:
            try:
                print("   -> Tentando usar Chromium do Playwright...")
                browser = p.chromium.launch(**launch_args)
                print("   ✅ Chromium do Playwright encontrado!")
            except Exception as e:
                print(f"❌ [ERRO] Nenhum navegador disponível!")
                print("   Para resolver, execute uma das opções:")
                print("   1. Instale o Chrome no sistema")
                print("   2. Execute: playwright install chromium")
                return None
        
        context = browser.new_context(accept_downloads=True, ignore_https_errors=True)
        page = context.new_page()

        try:
            print(f"🌍 [NAVEGAÇÃO] Criando sessão...")
            page.goto(URL_FORM, timeout=60000)
            page.fill("input[name='dt_ini']", data_br)
            page.fill("input[name='dt_fim']", data_br)
            
            # Link direto para download
            link_direto = f"{URL_BASE_DOWNLOAD}?op_exc=False&emissor=&isin=&ativo=&dt_ini={data_link}&dt_fim={data_link}"
            print(f"🔗 [SNIPER] Disparando link direto...")
            
            with page.expect_download(timeout=60000) as download_info:
                try:
                    page.goto(link_direto)
                except:
                    pass

            download = download_info.value
            nome_arquivo = f"snd_precos_{data_link}.xls"
            caminho_final = os.path.join(DOWNLOAD_DIR, nome_arquivo)
            download.save_as(caminho_final)
            print(f"✅ [SUCESSO] Arquivo salvo: {caminho_final}")
            arquivo_baixado = caminho_final

        except Exception as e:
            print(f"❌ [ERRO EXTRAÇÃO]: {e}")
        finally:
            print("🔒 Fechando navegador...")
            time.sleep(2)
            browser.close()
            
    return arquivo_baixado


def transform_data(file_path):
    """
    Transforma os dados brutos do SND em formato estruturado
    Calcula volume_total = pu_medio * quantidade
    """
    if not file_path or not os.path.exists(file_path):
        return None
    print("⚙️ [TRANSFORM] Processando e Calculando Volume...")
    
    try:
        # TENTATIVA 1: Ler como CSV separado por TAB (Padrão SND mais comum)
        try:
            print("   -> Tentando ler como Texto/TAB...")
            # skiprows=2 para pular título e linha vazia, mantendo o cabeçalho
            df = pd.read_csv(file_path, sep='\t', encoding='latin-1', skiprows=2, on_bad_lines='skip')
            if len(df.columns) < 2:
                raise ValueError("Provável HTML")
        except:
            # TENTATIVA 2: Ler como HTML (Fallback)
            print("   -> Falhou TAB, tentando ler como HTML...")
            with open(file_path, 'rb') as f:
                html_content = f.read().decode('latin-1', errors='replace')
            dfs = pd.read_html(io.StringIO(html_content), decimal=',', thousands='.')
            if not dfs:
                return None
            df = dfs[0]

        # --- LIMPEZA E MAPEAMENTO DE COLUNAS ---
        print(f"   -> Colunas originais: {df.columns.tolist()}")
        mapa_colunas = {}
        for col in df.columns:
            c_clean = str(col).strip().lower().replace(' ', '_').replace('.', '').replace('/', '_')
            
            # Código do Ativo -> codigo (verificar "código" ou "ativo" mas não apenas "ativo" isolado)
            if 'código' in c_clean or 'codigo' in c_clean:
                if 'ativo' in c_clean or 'código' in c_clean or 'codigo' in c_clean:
                    mapa_colunas[col] = 'codigo'
            elif 'emissor' in c_clean:
                mapa_colunas[col] = 'emissor'
            elif 'mínimo' in c_clean or 'minimo' in c_clean:
                mapa_colunas[col] = 'pu_minimo'
            elif 'médio' in c_clean or 'medio' in c_clean:
                mapa_colunas[col] = 'pu_medio'
            elif 'máximo' in c_clean or 'maximo' in c_clean:
                mapa_colunas[col] = 'pu_maximo'
            elif 'quantidade' in c_clean:
                mapa_colunas[col] = 'quantidade'
            elif 'negócios' in c_clean or 'negocios' in c_clean:
                mapa_colunas[col] = 'numero_negocios'
        
        print(f"   -> Mapeamento: {mapa_colunas}")
        
        df = df.rename(columns=mapa_colunas)
        
        if 'codigo' in df.columns:
            df = df[df['codigo'].notna()]
            df = df[df['codigo'] != 'Código']

        # Adiciona Data de Referência (data_base)
        data_ref = get_d_minus_1().strftime('%Y-%m-%d')
        df['data_base'] = data_ref
        df['data_atualizacao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Conversão Numérica
        cols_num = ['pu_minimo', 'pu_medio', 'pu_maximo', 'quantidade', 'numero_negocios']
        for col in cols_num:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # CÁLCULO DE VOLUME (PU * QTD)
        print("🧮 Calculando Volume Total...")
        if 'pu_medio' in df.columns and 'quantidade' in df.columns:
            df['volume_total'] = df['pu_medio'] * df['quantidade']
        else:
            df['volume_total'] = 0.0

        # Colunas finais
        cols_finais = [
            'data_base', 'codigo', 'emissor', 
            'pu_minimo', 'pu_medio', 'pu_maximo', 
            'quantidade', 'numero_negocios', 'volume_total',
            'data_atualizacao'
        ]
        
        for c in cols_finais:
            if c not in df.columns:
                df[c] = None
            
        df_final = df[cols_finais].copy()
        
        # Normaliza o código (remove espaços, uppercase)
        if 'codigo' in df_final.columns:
            df_final['codigo'] = df_final['codigo'].astype(str).str.strip().str.upper()
            
            # Remove linhas com código inválido (vazio, nan, None, etc)
            total_antes = len(df_final)
            df_final = df_final[df_final['codigo'].notna()]
            df_final = df_final[df_final['codigo'] != '']
            df_final = df_final[df_final['codigo'] != 'NAN']
            df_final = df_final[df_final['codigo'] != 'NONE']
            df_final = df_final[~df_final['codigo'].str.contains('CÓDIGO', case=False, na=False)]
            
            registros_removidos = total_antes - len(df_final)
            if registros_removidos > 0:
                print(f"   🧹 Removidos {registros_removidos} registros com código inválido")
        
        print(f"📊 [DADOS] {len(df_final)} linhas válidas processadas.")
        return df_final

    except Exception as e:
        print(f"❌ [ERRO TRANSFORM]: {e}")
        import traceback
        traceback.print_exc()
        return None


def load_data(df, db_path=None):
    """
    Carrega dados de volume no banco SQLite
    Cria tabela negociacao_snd se não existir
    """
    if df is None or df.empty:
        print("⚠️ [AVISO] DataFrame vazio, nada a carregar.")
        return False
    
    if db_path is None:
        db_path = DB_PATH
        
    print(f"💾 [LOAD] Salvando no Banco: {db_path}")
    
    # Verifica se o diretório do banco existe
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"   📁 Diretório criado: {db_dir}")

    # ===== VALIDAÇÃO FINAL DOS DADOS =====
    print("   🔍 Validando dados antes de inserir...")
    
    # Garantir que colunas obrigatórias existam
    required_cols = ['data_base', 'codigo']
    for col in required_cols:
        if col not in df.columns:
            print(f"❌ [ERRO] Coluna obrigatória ausente: {col}")
            return False
    
    # Remover linhas onde codigo ou data_base são inválidos
    df_clean = df.copy()
    total_antes = len(df_clean)
    
    # Converter para string e limpar
    df_clean['codigo'] = df_clean['codigo'].astype(str).str.strip()
    df_clean['data_base'] = df_clean['data_base'].astype(str).str.strip()
    
    # Filtrar registros válidos
    df_clean = df_clean[
        (df_clean['codigo'].notna()) & 
        (df_clean['codigo'] != '') & 
        (df_clean['codigo'] != 'nan') &
        (df_clean['codigo'] != 'None') &
        (df_clean['data_base'].notna()) & 
        (df_clean['data_base'] != '')
    ]
    
    registros_invalidos = total_antes - len(df_clean)
    if registros_invalidos > 0:
        print(f"   ⚠️ {registros_invalidos} registros removidos por dados inválidos")
    
    if df_clean.empty:
        print("❌ [ERRO] Nenhum registro válido para inserir após validação!")
        return False
    
    print(f"   ✅ {len(df_clean)} registros válidos prontos para inserção")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Cria tabela com estrutura otimizada
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS negociacao_snd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_base TEXT NOT NULL,
            codigo TEXT NOT NULL,
            emissor TEXT,
            pu_minimo REAL,
            pu_medio REAL,
            pu_maximo REAL,
            quantidade INTEGER,
            numero_negocios INTEGER,
            volume_total REAL,
            data_atualizacao TEXT,
            UNIQUE(data_base, codigo)
        );
        """)
        
        # Cria índices para busca rápida
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_neg_codigo ON negociacao_snd(codigo);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_neg_data ON negociacao_snd(data_base);")
        
        # Remove dados da mesma data (evita duplicatas)
        data_ref = df_clean['data_base'].iloc[0]
        cursor.execute("DELETE FROM negociacao_snd WHERE data_base = ?", (data_ref,))
        deleted = cursor.rowcount
        if deleted > 0:
            print(f"   🗑️ Removidos {deleted} registros antigos de {data_ref}")
        
        # Insere novos dados
        df_clean.to_sql('negociacao_snd', conn, if_exists='append', index=False)
        conn.commit()
        
        # Estatísticas
        cursor.execute("SELECT COUNT(DISTINCT data_base) FROM negociacao_snd")
        total_dias = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM negociacao_snd")
        total_registros = cursor.fetchone()[0]
        
        print(f"✅ [FIM] Dados salvos com sucesso!")
        print(f"   📈 Total de dias no histórico: {total_dias}")
        print(f"   📊 Total de registros: {total_registros}")
        return True
        
    except Exception as e:
        print(f"❌ [ERRO SQL]: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()


def get_volume_summary(db_path=None):
    """
    Retorna resumo de volume para dashboard
    """
    if db_path is None:
        db_path = DB_PATH
        
    if not os.path.exists(db_path):
        return None
        
    conn = sqlite3.connect(db_path)
    
    try:
        # Volume total do dia mais recente
        query = """
        SELECT 
            data_base,
            SUM(volume_total) as volume_total_dia,
            COUNT(DISTINCT codigo) as qtd_ativos_negociados,
            SUM(numero_negocios) as total_negocios
        FROM negociacao_snd
        WHERE data_base = (SELECT MAX(data_base) FROM negociacao_snd)
        GROUP BY data_base
        """
        df = pd.read_sql_query(query, conn)
        return df
    except:
        return None
    finally:
        conn.close()


def get_top_volume(n=10, db_path=None):
    """
    Retorna os N ativos mais negociados (maior volume)
    """
    if db_path is None:
        db_path = DB_PATH
        
    if not os.path.exists(db_path):
        return None
        
    conn = sqlite3.connect(db_path)
    
    try:
        query = f"""
        SELECT 
            codigo,
            emissor,
            volume_total,
            quantidade,
            numero_negocios,
            pu_medio,
            data_base
        FROM negociacao_snd
        WHERE data_base = (SELECT MAX(data_base) FROM negociacao_snd)
        ORDER BY volume_total DESC
        LIMIT {n}
        """
        df = pd.read_sql_query(query, conn)
        return df
    except:
        return None
    finally:
        conn.close()


def executar_etl_completo(headless=True, use_system_chrome=True):
    """
    Executa o pipeline ETL completo
    Args:
        headless: Se False, abre janela do navegador visível (útil para debug)
        use_system_chrome: Se True, usa Chrome instalado no sistema (recomendado)
    """
    print("="*50)
    print("🚀 ETL PREÇOS SND - VOLUME NEGOCIADO")
    print("="*50)
    
    arquivo = extract_snd(headless=headless, use_system_chrome=use_system_chrome)
    if arquivo:
        df = transform_data(arquivo)
        if df is not None:
            success = load_data(df)
            # Limpa arquivo temporário
            try:
                os.remove(arquivo)
                print(f"🧹 Arquivo temporário removido: {arquivo}")
            except:
                pass
            return success
    return False


if __name__ == "__main__":
    import sys
    
    # Aceita argumento --visible para debug
    headless = "--visible" not in sys.argv
    
    executar_etl_completo(headless=headless)
