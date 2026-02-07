import subprocess
import datetime
import sys
import sqlite3
import os
import time

# --- CONFIGURAÇÃO ---
# Lista dos scripts na ordem correta de execução
SCRIPTS = [
    "extrator_snd.py",      # 1. Atualiza Cadastro
    "etl_curvas_anbima.py", # 2. Atualiza Curvas (Juros)
    "etl_precos_snd.py"     # 3. Atualiza Preços (Volume)
]

def log(msg, tipo="INFO"):
    """Função para padronizar os LOGS"""
    now = datetime.datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️"
    if tipo == "ERRO": icon = "❌"
    elif tipo == "SUCESSO": icon = "✅"
    elif tipo == "WARN": icon = "⚠️"
    elif tipo == "CMD": icon = "🚀"
    
    print(f"[{now}] {icon} {msg}")
    sys.stdout.flush() # Força o print aparecer na hora

def check_db_stats():
    """Verifica se os dados foram salvos corretamente"""
    log("Iniciando verificação dos Bancos de Dados...", "CMD")
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    
    # Mapeamento: Arquivo -> Tabelas esperadas
    bancos = {
        "debentures_anbima.db": ["negociacao_snd"], # Tabelas que esperamos ver
        "curvas_anbima.db": ["curvas_anbima"]
    }

    if not os.path.exists(data_dir):
        log(f"Pasta 'data' não encontrada em: {data_dir}", "ERRO")
        return

    print("-" * 50)
    for db_file, tabelas in bancos.items():
        db_path = os.path.join(data_dir, db_file)
        
        if not os.path.exists(db_path):
            log(f"Banco NÃO encontrado: {db_file}", "WARN")
            continue
            
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            log(f"Conectado ao banco: {db_file}", "SUCESSO")
            
            for t in tabelas:
                try:
                    count = cursor.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    
                    # Tenta pegar a data mais recente
                    col_data = 'data_referencia' if 'curvas' in t else 'data_base'
                    try:
                        last_date = cursor.execute(f"SELECT MAX({col_data}) FROM {t}").fetchone()[0]
                    except:
                        last_date = "N/A"
                        
                    print(f"   📋 Tabela '{t}': {count} linhas | Última Data: {last_date}")
                except Exception as e:
                    print(f"   ❌ Tabela '{t}' erro: {e}")
            
            conn.close()
        except Exception as e:
            log(f"Erro ao ler banco {db_file}: {e}", "ERRO")
        print("-" * 50)

def rodar_scripts():
    print("=" * 60)
    log(f"INICIANDO ROTINA DE DADOS - {datetime.datetime.now().strftime('%d/%m/%Y')}", "CMD")
    print("=" * 60)
    
    sucessos = 0
    falhas = 0

    for script in SCRIPTS:
        print("\n" + "-" * 60)
        log(f"Executando script: {script}...", "CMD")
        print("-" * 60)
        
        start_time = time.time()
        
        try:
            # Executa o script e captura o log em tempo real
            process = subprocess.run(
                ["python", script],
                capture_output=True,
                text=True,
                check=False # Não para se der erro, queremos ver o log
            )
            
            # IMPRIME O LOG DO SCRIPT FILHO
            if process.stdout:
                print(process.stdout)
            
            if process.returncode == 0:
                log(f"Script {script} FINALIZADO COM SUCESSO.", "SUCESSO")
                sucessos += 1
            else:
                log(f"Script {script} FALHOU.", "ERRO")
                print("🔻 ERRO (STDERR):")
                print(process.stderr)
                falhas += 1
                
        except Exception as e:
            log(f"Erro crítico ao tentar rodar {script}: {e}", "ERRO")
            falhas += 1
            
        elapsed = time.time() - start_time
        print(f"⏱️ Tempo de execução: {elapsed:.2f} segundos")

    print("\n" + "=" * 60)
    log("RELATÓRIO FINAL", "CMD")
    print(f"✅ Sucessos: {sucessos}")
    print(f"❌ Falhas:   {falhas}")
    print("=" * 60)
    
    # Verifica o banco no final
    check_db_stats()
    
    if falhas > 0:
        log("A rotina terminou com erros. Verifique os logs acima.", "WARN")
        sys.exit(1) # Faz o GitHub Actions ficar vermelho
    else:
        log("Rotina concluída com sucesso total!", "SUCESSO")

if __name__ == "__main__":
    rodar_scripts()
