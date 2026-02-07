import subprocess
import datetime
import sys
import sqlite3
import os
import time
import pandas as pd # Necessário para visualizar a tabela bonitinha

# Configuração de exibição do Pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# --- CONFIGURAÇÃO ---
SCRIPTS = [
    "extrator_snd.py",      # Opcional: Se existir
    "etl_curvas_anbima.py", # Curvas de Juros
    "etl_precos_snd.py"     # Preços de Negociação
]

# Caminhos Absolutos (Para evitar erro de pasta)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def log(msg, tipo="INFO"):
    now = datetime.datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️"
    if tipo == "ERRO": icon = "❌"
    elif tipo == "SUCESSO": icon = "✅"
    elif tipo == "CMD": icon = "🚀"
    elif tipo == "DATA": icon = "📊"
    print(f"[{now}] {icon} {msg}")
    sys.stdout.flush()

def auditoria_visual():
    """
    Lê os bancos de dados logo após a execução e imprime 5 linhas de cada.
    Isso garante que os dados foram gravados no disco do Runner.
    """
    print("\n" + "="*80)
    log("INICIANDO AUDITORIA VISUAL DOS DADOS GRAVADOS", "DATA")
    print("="*80)

    # Lista de Verificação: (Nome do Arquivo, Nome da Tabela, Coluna de Data)
    verificacoes = [
        ("debentures_anbima.db", "negociacao_snd", "data_base"),
        ("curvas_anbima.db", "curvas_anbima", "data_referencia")
    ]

    if not os.path.exists(DATA_DIR):
        log(f"Pasta DATA não encontrada: {DATA_DIR}", "ERRO")
        return

    for db_file, tabela, col_data in verificacoes:
        db_path = os.path.join(DATA_DIR, db_file)
        
        print(f"\n📂 Verificando Banco: {db_file}")
        
        if not os.path.exists(db_path):
            log(f"Arquivo .db não encontrado: {db_path}", "ERRO")
            continue

        try:
            conn = sqlite3.connect(db_path)
            
            # 1. Verifica se a tabela existe
            check_table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tabela}'", conn)
            if check_table.empty:
                log(f"Tabela '{tabela}' NÃO existe neste banco!", "ERRO")
                conn.close()
                continue

            # 2. Conta linhas totais
            count = pd.read_sql(f"SELECT COUNT(*) as total FROM {tabela}", conn).iloc[0]['total']
            
            # 3. Pega as 5 linhas mais recentes
            # Tenta ordenar pela data para ver o que acabou de entrar
            try:
                query = f"SELECT * FROM {tabela} ORDER BY {col_data} DESC LIMIT 5"
                df = pd.read_sql(query, conn)
            except:
                # Se der erro na ordenação, pega as 5 primeiras padrão
                query = f"SELECT * FROM {tabela} LIMIT 5"
                df = pd.read_sql(query, conn)

            log(f"Tabela: {tabela} | Total Linhas: {count}", "SUCESSO")
            
            if not df.empty:
                print("\n🔎 AMOSTRA (TOP 5 RECENTES):")
                print(df.to_string(index=False)) # Imprime a tabela formatada
            else:
                log("A tabela existe mas está VAZIA (0 registros).", "ERRO")

            conn.close()
            
        except Exception as e:
            log(f"Erro ao ler banco de dados: {e}", "ERRO")

    print("\n" + "="*80)

def rodar_scripts():
    print("=" * 60)
    log(f"INICIANDO ROTINA ETL - {datetime.datetime.now().strftime('%d/%m/%Y')}", "CMD")
    print("=" * 60)
    
    falhas = 0

    for script in SCRIPTS:
        # Pula script se arquivo não existir
        script_path = os.path.join(BASE_DIR, script)
        if not os.path.exists(script_path):
            log(f"Script não encontrado (pulando): {script}", "ERRO")
            continue

        print(f"\n⏳ Executando: {script}...")
        start_time = time.time()
        
        try:
            process = subprocess.run(
                ["python", script],
                capture_output=True,
                text=True
            )
            
            # Mostra o log do script filho
            if process.stdout: print(process.stdout)
            if process.stderr: print(process.stderr)
            
            if process.returncode == 0:
                log(f"{script} -> SUCESSO", "SUCESSO")
            else:
                log(f"{script} -> FALHA", "ERRO")
                falhas += 1
                
        except Exception as e:
            log(f"Erro crítico ao chamar {script}: {e}", "ERRO")
            falhas += 1

    # --- AQUI ESTÁ O QUE VOCÊ PEDIU ---
    # Roda a auditoria independente se houve falha ou não, para ver o que sobrou
    auditoria_visual()
    
    if falhas > 0:
        sys.exit(1) # Avisa o GitHub que houve erro

if __name__ == "__main__":
    rodar_scripts()
