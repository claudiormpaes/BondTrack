import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright


def get_last_business_day(date):
    offset = 3 if date.weekday() == 0 else (2 if date.weekday() == 6 else 1)
    return date - timedelta(days=offset)


def get_ultimos_dias_uteis(n=3):
    """
    Retorna lista com os últimos N dias úteis (seg-sex)
    """
    dias = []
    hoje = datetime.now()
    d = hoje
    
    while len(dias) < n:
        d = d - timedelta(days=1)
        # Dias úteis: segunda (0) a sexta (4)
        if d.weekday() < 5:
            dias.append(d)
    
    return dias


def salvar_cadastro_com_upsert(df, db_path):
    """
    Salva cadastro no banco com UPSERT
    Chave única: codigo (ticker da debênture)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Criar tabela se não existir (com codigo como PRIMARY KEY)
    # Primeiro, verifica se a tabela existe
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cadastro_snd'")
    tabela_existe = cursor.fetchone() is not None
    
    if not tabela_existe:
        # Criar tabela com codigo como PRIMARY KEY
        colunas_def = []
        for col in df.columns:
            if col == 'codigo':
                colunas_def.append(f'"{col}" TEXT PRIMARY KEY')
            else:
                colunas_def.append(f'"{col}" TEXT')
        
        create_sql = f"CREATE TABLE cadastro_snd ({', '.join(colunas_def)})"
        cursor.execute(create_sql)
        print("   📁 Tabela cadastro_snd criada")
    
    # UPSERT: INSERT OR REPLACE para cada registro
    registros_inseridos = 0
    colunas = df.columns.tolist()
    placeholders = ', '.join(['?' for _ in colunas])
    colunas_quoted = ', '.join([f'"{c}"' for c in colunas])
    
    for _, row in df.iterrows():
        try:
            valores = [row[c] if pd.notna(row[c]) else None for c in colunas]
            cursor.execute(f"""
                INSERT OR REPLACE INTO cadastro_snd ({colunas_quoted})
                VALUES ({placeholders})
            """, valores)
            registros_inseridos += 1
        except Exception as e:
            # Em caso de erro de estrutura, tenta adicionar coluna
            pass
    
    conn.commit()
    
    # Estatísticas
    cursor.execute("SELECT COUNT(*) FROM cadastro_snd")
    total = cursor.fetchone()[0]
    
    conn.close()
    
    return registros_inseridos, total


def executar_automacao_snd(headless=True):
    """
    Extrai cadastro de debêntures do SND
    O cadastro é uma foto única do sistema, não precisa de múltiplos dias
    """
    with sync_playwright() as p:
        print("🛰️ SND: Extraindo Cadastro de Debêntures...")
        browser = None
        
        try:
            # Tenta usar Chrome do sistema primeiro
            try:
                print("   -> Tentando usar Chrome do sistema...")
                browser = p.chromium.launch(channel="chrome", headless=headless)
                print("   ✅ Chrome do sistema encontrado!")
            except:
                print("   -> Tentando usar Chromium do Playwright...")
                browser = p.chromium.launch(headless=headless)
                print("   ✅ Chromium do Playwright encontrado!")
            
            page = browser.new_page()
            page.goto("https://www.debentures.com.br/exploreosnd/consultaadados/emissoesdedebentures/caracteristicas_f.asp?tip_deb=publicas", timeout=60000)
            page.click("input[name='Submit']")

            with page.expect_download(timeout=60000) as d_info:
                page.click("a[href*='caracteristicas_e.asp']")
            download = d_info.value
            download.save_as("temp_snd.xls")

            # PADRONIZAÇÃO DE DATA BR
            data_br = get_last_business_day(datetime.now()).strftime('%d/%m/%Y')

            df = pd.read_csv("temp_snd.xls", sep='\t', encoding='latin-1', skiprows=4)
            df.columns = [str(c).strip() for c in df.columns]
            
            # Normalizar código
            if 'Codigo do Ativo' in df.columns:
                df['codigo'] = df['Codigo do Ativo'].astype(str).str.strip().str.upper()
            elif 'codigo' not in df.columns:
                # Procura coluna que contenha "codigo" ou "ativo"
                for col in df.columns:
                    if 'codigo' in col.lower() or 'ativo' in col.lower():
                        df['codigo'] = df[col].astype(str).str.strip().str.upper()
                        break
            
            df['data_referencia'] = data_br
            df['data_atualizacao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Remover registros sem código válido
            if 'codigo' in df.columns:
                df = df[df['codigo'].notna()]
                df = df[df['codigo'] != '']
                df = df[df['codigo'] != 'NAN']
            
            # Configurar banco
            db_dir = os.path.join(os.path.dirname(__file__), 'data')
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
            db_path = os.path.join(db_dir, 'debentures_anbima.db')
            
            # Salvar com UPSERT
            inseridos, total = salvar_cadastro_com_upsert(df, db_path)
            
            if os.path.exists("temp_snd.xls"): 
                os.remove("temp_snd.xls")
            
            print(f"✅ SND: {inseridos} registros salvos/atualizados")
            print(f"📊 Total no banco: {total} debêntures cadastradas")
            print(f"📅 Data de referência: {data_br}")
            
            return True

        except Exception as e:
            print(f"❌ Erro no extrator: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            if browser:
                browser.close()


if __name__ == "__main__":
    import sys
    
    headless = "--visible" not in sys.argv
    executar_automacao_snd(headless=headless)
