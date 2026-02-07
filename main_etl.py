import subprocess
import datetime
import sys
import sqlite3
import os
import time
import pandas as pd

# Configura o Pandas para mostrar todas as colunas no log
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 10)

# Caminhos Absolutos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# --- CONFIGURAÇÃO DOS SCRIPTS ---
# Mapeia qual script alimenta qual tabela para a auditoria
SCRIPTS_INFO = [
    {
        "script": "extrator_snd.py",
        "banco": "debentures_anbima.db",
        "tabela": "cadastro_snd",
        "col_data": "data_referencia" 
    },
    {
        "script": "etl_curvas_anbima.py",
        "banco": "curvas_anbima.db",
        "tabela": "curvas_anbima",
        "col_data": "data_referencia"
    },
    {
        "script": "etl_precos_snd.py",
        "banco": "debentures_anbima.db",
        "tabela": "negociacao_snd",
        "col_data": "data_base"
    }
]

def log(msg, tipo="INFO"):
    now = datetime.datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️"
    if tipo == "ERRO": icon = "❌"
    elif tipo == "SUCESSO": icon = "✅"
    elif tipo == "CMD": icon = "🚀"
    elif tipo == "DATA": icon = "📊"
    print(f"[{now}] {icon} {msg}")
    sys.stdout.flush()

def ler_amostra_tabela(nome_banco, nome_tabela, col_data):
    """Lê as 5 primeiras linhas de uma tabela específica"""
    db_path = os.path.join(DATA_DIR, nome_banco)
    
    if not os.path.exists(db_path):
        log(f"Banco não encontrado para leitura: {nome_banco}", "WARN")
        return

    try:
        conn = sqlite3.connect(db_path)
        
        # Verifica se a tabela existe
        check = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{nome_tabela}'", conn)
        if check.empty:
            log(f"Tabela '{nome_tabela}' ainda não existe no banco.", "WARN")
            conn.close()
            return

        # Busca os dados mais recentes
        query = f"SELECT * FROM {nome_tabela} ORDER BY {col_data} DESC LIMIT 5"
        try:
            df = pd.read_sql(query, conn)
        except:
            # Fallback se a coluna de data não existir
            df = pd.read_sql(f"SELECT * FROM {nome_tabela} LIMIT 5", conn)
        
        conn.close()

        if not df.empty:
            print(f"\n🔎 AMOSTRA DE DADOS ({nome_tabela}):")
            print("-" * 80)
            print(df.to_string(index=False))
            print("-" * 80 + "\n")
        else:
            log(f"A tabela '{nome_tabela}' existe mas está vazia.", "WARN")

    except Exception as e:
        log(f"Erro ao ler tabela {nome_tabela}: {e}", "ERRO")

def rodar_scripts():
    print("=" * 80)
    log(f"INICIANDO ROTINA ETL - {datetime.datetime.now().strftime('%d/%m/%Y')}", "CMD")
    print("=" * 80)
    
    falhas = 0

    for item in SCRIPTS_INFO:
        script = item["script"]
        script_path = os.path.join(BASE_DIR, script)

        # 1. Verifica se o script existe
        if not os.path.exists(script_path):
            log(f"Script não encontrado: {script}", "ERRO")
            continue

        print(f"\n⏳ Executando: {script}...")
        start_time = time.time()
        
        try:
            # 2. Roda o script
            process = subprocess.run(
                ["python", script],
                capture_output=True,
                text=True
            )
            
            # Mostra o output do script (prints internos)
            if process.stdout: 
                print("--- Log do Script ---")
                print(process.stdout.strip())
            
            if process.stderr: 
                print("--- Erros do Script ---")
                print(process.stderr.strip())

            if process.returncode == 0:
                log(f"{script} FINALIZADO COM SUCESSO.", "SUCESSO")
                
                # 3. MOSTRA OS DADOS IMEDIATAMENTE APÓS O SUCESSO
                ler_amostra_tabela(item["banco"], item["tabela"], item["col_data"])
                
            else:
                log(f"{script} FALHOU.", "ERRO")
                falhas += 1
                
        except Exception as e:
            log(f"Erro crítico ao tentar rodar {script}: {e}", "ERRO")
            falhas += 1

    print("\n" + "=" * 80)
    if falhas > 0:
        log(f"Rotina finalizada com {falhas} erros.", "ERRO")
        sys.exit(1)
    else:
        log("Todos os scripts rodaram e geraram dados.", "SUCESSO")

if __name__ == "__main__":
    rodar_scripts()
