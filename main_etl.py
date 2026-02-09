import subprocess
import datetime
import sys
import sqlite3
import os
import time
import pandas as pd

# --- CONFIGURAÇÃO VISUAL ---
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 50)

# --- CONFIGURAÇÃO DE CAMINHOS (DINÂMICO) ---
# Pega o diretório onde este script está rodando (seja Windows, Linux ou Mac)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Garante que a pasta data existe
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# --- MAPA DE EXECUÇÃO ---
PIPELINE = [
    {
        "nome": "1. CADASTRO DE DEBÊNTURES (SND)",
        "script": "extrator_snd.py",
        "banco": "debentures_anbima.db",
        "tabela": "cadastro_snd",
        "coluna_data": "data_referencia"
    },
    {
        "nome": "2. CURVAS DE JUROS (ANBIMA)",
        "script": "etl_curvas_anbima.py",
        "banco": "curvas_anbima.db",
        "tabela": "curvas_anbima",
        "coluna_data": "data_referencia"
    },
    {
        "nome": "3. TAXAS INDICATIVAS (ANBIMA)",
        "script": "etl_taxas_anbima.py",
        "banco": "debentures_anbima.db",
        "tabela": "taxas_indicativas_anbima",
        "coluna_data": "data_referencia"
    },
    {
        "nome": "4. PREÇOS E VOLUMES (SND)",
        "script": "etl_precos_snd.py",
        "banco": "debentures_anbima.db",
        "tabela": "negociacao_snd",
        "coluna_data": "data_referencia"
    }
]

def log(msg, tipo="INFO"):
    """Gera logs formatados para o console do GitHub Actions"""
    now = datetime.datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️"
    if tipo == "ERRO": icon = "❌"
    elif tipo == "SUCESSO": icon = "✅"
    elif tipo == "DB": icon = "🗄️"
    
    print(f"[{now}] {icon} {msg}")
    sys.stdout.flush()

def conferir_banco(nome_banco, nome_tabela, coluna_ordem):
    """Auditoria automática pós-execução"""
    db_path = os.path.join(DATA_DIR, nome_banco)
    
    if not os.path.exists(db_path):
        log(f"Arquivo de banco não encontrado: {db_path}", "ERRO")
        return

    try:
        conn = sqlite3.connect(db_path)
        
        # Verifica se tabela existe
        check = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{nome_tabela}'", conn)
        if check.empty:
            log(f"Banco conectado, mas tabela '{nome_tabela}' não existe.", "ERRO")
            conn.close()
            return

        # Busca últimas 5 linhas
        try:
            query = f"SELECT * FROM {nome_tabela} ORDER BY {coluna_ordem} DESC LIMIT 5"
            df = pd.read_sql(query, conn)
        except Exception:
            log(f"Coluna '{coluna_ordem}' não encontrada, listando 5 registros aleatórios.", "INFO")
            query = f"SELECT * FROM {nome_tabela} LIMIT 5"
            df = pd.read_sql(query, conn)
        
        conn.close()

        if not df.empty:
            print("\n" + "-"*80)
            log(f"AUDITORIA: ÚLTIMOS DADOS EM '{nome_tabela}'", "DB")
            print("-" * 80)
            print(df.to_string(index=False))
            print("-" * 80 + "\n")
        else:
            log(f"Tabela '{nome_tabela}' está vazia.", "ERRO")

    except Exception as e:
        log(f"Erro ao auditar banco: {e}", "ERRO")

def rodar_pipeline():
    print("=" * 80)
    log(f"PIPELINE GITHUB ACTIONS - {datetime.datetime.now().strftime('%d/%m/%Y')}", "SUCESSO")
    print("=" * 80)
    
    erros_totais = 0

    for tarefa in PIPELINE:
        script = tarefa["script"]
        caminho_script = os.path.join(BASE_DIR, script)

        print(f"\n🚀 ETAPA: {tarefa['nome']}")
        print("." * 40)

        if not os.path.exists(caminho_script):
            log(f"Script não encontrado no repo: {script}", "ERRO")
            erros_totais += 1
            continue

        start = time.time()
        
        try:
            # Executa o script filho no mesmo ambiente
            resultado = subprocess.run(
                ["python", caminho_script],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            
            # Imprime logs do script filho
            if resultado.stdout:
                print(f"📝 Output ({script}):")
                print(resultado.stdout.strip())
            
            if resultado.stderr:
                print(f"⚠️ Erros/Avisos ({script}):")
                print(resultado.stderr.strip())

            if resultado.returncode == 0:
                log(f"{script} finalizado com sucesso.", "SUCESSO")
                conferir_banco(tarefa["banco"], tarefa["tabela"], tarefa["coluna_data"])
            else:
                log(f"{script} falhou (Exit Code {resultado.returncode}).", "ERRO")
                erros_totais += 1

        except Exception as e:
            log(f"Erro de execução do Python: {e}", "ERRO")
            erros_totais += 1
            
        print(f"⏱️ Duração: {time.time() - start:.2f}s")

    print("\n" + "=" * 80)
    if erros_totais > 0:
        log(f"Pipeline finalizado com {erros_totais} erros.", "ERRO")
        sys.exit(1) # Faz o Action ficar Vermelho 🔴
    else:
        log("Pipeline finalizado com sucesso total.", "SUCESSO") # Faz o Action ficar Verde 🟢

if __name__ == "__main__":
    rodar_pipeline()
