import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

def get_last_business_day(date):
    offset = 3 if date.weekday() == 0 else (2 if date.weekday() == 6 else 1)
    return date - timedelta(days=offset)

def executar_automacao_snd():
    with sync_playwright() as p:
        print("🛰️ SND: Usando Chrome local para evitar erro de certificado...")
        try:
            # channel="chrome" usa o seu navegador da máquina
            browser = p.chromium.launch(channel="chrome", headless=True)
            page = browser.new_page()
            page.goto("https://www.debentures.com.br/exploreosnd/consultaadados/emissoesdedebentures/caracteristicas_f.asp?tip_deb=publicas")
            page.click("input[name='Submit']")

            with page.expect_download() as d_info:
                page.click("a[href*='caracteristicas_e.asp']")
            download = d_info.value
            download.save_as("temp_snd.xls")

            # PADRONIZAÇÃO DE DATA BR PARA O JOIN
            data_br = get_last_business_day(datetime.now()).strftime('%d/%m/%Y')

            df = pd.read_csv("temp_snd.xls", sep='\t', encoding='latin-1', skiprows=4)
            df.columns = [str(c).strip() for c in df.columns]
            df['codigo'] = df['Codigo do Ativo'].astype(str).str.strip().str.upper()
            df['data_referencia'] = data_br
            
            db_dir = os.path.join(os.path.dirname(__file__), 'data')
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
            db_path = os.path.join(db_dir, 'debentures_anbima.db')
            conn = sqlite3.connect(db_path)
            df.to_sql('cadastro_snd', conn, if_exists='replace', index=False)
            conn.close()
            
            if os.path.exists("temp_snd.xls"): os.remove("temp_snd.xls")
            print(f"✅ SND: Cadastro sincronizado em {data_br}")

        except Exception as e:
            print(f"❌ Erro no extrator: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    executar_automacao_snd()