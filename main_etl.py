import subprocess
import datetime
import sys

# Lista dos scripts na ordem correta de execução
# 1. Busca os dados brutos
# 2. Processa as curvas de juros (benchmark)
# 3. Processa preços e taxas para o Mercado Secundário
scripts = [
    "extrator_snd.py",
    "etl_curvas_anbima.py",
    "etl_precos_snd.py" 
]

def rodar_scripts():
    print(f"🚀 Iniciando Rotina de Dados - {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("-" * 50)
    
    resultados = {}

    for script in scripts:
        print(f"⏳ Executando: {script}...")
        try:
            # Executa o script e espera terminar
            # O check=True faz o Python levantar um erro se o script falhar
            resultado = subprocess.run(["python", script], capture_output=True, text=True, check=True)
            print(f"✅ {script} concluído com sucesso.")
            resultados[script] = "SUCESSO"
        except subprocess.CalledProcessError as e:
            print(f"❌ ERRO em {script}:")
            print(f"Saída de erro: {e.stderr}")
            resultados[script] = "FALHA"
            # Opcional: interromper a fila se um script essencial falhar
            # sys.exit(1) 

    print("\n" + "=" * 30)
    print("📋 RELATÓRIO FINAL DE EXECUÇÃO")
    print("=" * 30)
    for script, status in resultados.items():
        print(f"{'✅' if status == 'SUCESSO' else '❌'} {script}: {status}")
    print("=" * 30)

if __name__ == "__main__":
    rodar_scripts()

import sqlite3

def check_db_stats():
    conn = sqlite3.connect("data/debentures_anbima.db")
    cursor = conn.cursor()
    tabelas = ['cadastro_snd', 'mercado_secundario', 'negociacao_snd']
    print("\n📊 STATUS ATUAL DO BANCO:")
    for t in tabelas:
        try:
            count = cursor.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            last_date = cursor.execute(f"SELECT MAX(data_referencia) FROM {t}").fetchone()[0]
            print(f"- Tabela {t}: {count} registros (Última data: {last_date})")
        except:
            print(f"- Tabela {t}: Erro ao ler ou tabela inexistente.")
    conn.close()

# Chame check_db_stats() no final do seu main_etl.py
