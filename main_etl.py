import subprocess
import datetime
import sys
import sqlite3
import os
import time
import pandas as pd

# --- CONFIGURAÇÃO VISUAL ---
# Ajusta o Pandas para mostrar todas as colunas no log do GitHub sem cortar
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 50)

# --- MAPA DE EXECUÇÃO ---
# Aqui definimos: Quem roda -> Onde salva -> O que conferir
PIPELINE = [
    {
        "nome": "1. CADASTRO DE DEBÊNTURES",
        "script": "extrator_snd.py",
        "banco": "debentures_anbima.db",
        "tabela": "cadastro_snd",
        "coluna_data": "data_referencia" # Ajuste se o nome for diferente
    },
    {
        "nome": "2. CURVAS DE JUROS (ANBIMA)",
        "script": "etl_curvas_anbima.py",
        "banco": "curvas_anbima.db",
        "tabela": "curvas_anbima",
        "coluna_data": "data_referencia"
    },
    {
        "nome": "3. PREÇOS E VOLUMES (SND)",
        "script": "etl_precos_snd.py",
        "banco": "debentures_anbima.db",
        "tabela": "negociacao_snd",
        "coluna_data": "data_base"
    }
]

# Caminhos Absolutos (Evita erro de "arquivo não encontrado")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def log(msg, tipo="INFO"):
    """Gera logs bonitos e visíveis"""
    now = datetime.datetime.now().strftime('%H:%M:%S')
    icon = "ℹ️"
    if tipo == "ERRO": icon = "❌"
    elif tipo == "SUCESSO": icon = "✅"
    elif tipo == "DB": icon = "🗄️"
    
    print(f"[{now}] {icon} {msg}")
    sys.stdout.flush()

def conferir_banco(nome_banco, nome_tabela, coluna_ordem):
    """
    Entra no banco de dados e imprime as 5 linhas mais recentes.
    """
    db_path = os.path.join(DATA_DIR, nome_banco)
    
    # 1. Verifica se o arquivo do banco existe
    if not os.path.exists(db_path):
        log(f"O arquivo do banco NÃO foi criado: {nome_banco}", "ERRO")
        return

    try:
        conn = sqlite3.connect(db_path)
        
        # 2. Verifica se a tabela existe
        check = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{nome_tabela}'", conn)
        if check.empty:
            log(f"Banco existe, mas a tabela '{nome_tabela}' NÃO foi criada.", "ERRO")
            conn.close()
            return

        # 3. Pega os 5 registros mais recentes
        # Tenta ordenar pela data para provar que o dado novo entrou
        try:
            query = f"SELECT * FROM {nome_tabela} ORDER BY {coluna_ordem} DESC LIMIT 5"
            df = pd.read_sql(query, conn)
        except Exception:
            # Se der erro na coluna de data, pega qualquer 5 linhas
            log(f"Coluna de data '{coluna_ordem}' não achada, pegando 5 linhas aleatórias.", "INFO")
            query = f"SELECT * FROM {nome_tabela} LIMIT 5"
            df = pd.read_sql(query, conn)
        
        conn.close()

        # 4. Exibe o Resultado
        if not df.empty:
            print("\n" + "-"*80)
            log(f"AUDITORIA: 5 LINHAS RECÉM-INSERIDAS EM '{nome_tabela}'", "DB")
            print("-" * 80)
            print(df.to_string(index=False))
            print("-" * 80 + "\n")
        else:
            log(f"A tabela '{nome_tabela}' existe mas está VAZIA (0 linhas).", "ERRO")

    except Exception as e:
        log(f"Erro ao tentar ler o banco {nome_banco}: {e}", "ERRO")

def rodar_pipeline():
    print("=" * 80)
    log(f"INICIANDO ROTINA COMPLETA - {datetime.datetime.now().strftime('%d/%m/%Y')}", "SUCESSO")
    print("=" * 80)
    
    erros_totais = 0

    for tarefa in PIPELINE:
        script = tarefa["script"]
        caminho_script = os.path.join(BASE_DIR, script)

        print(f"\n🚀 ETAPA: {tarefa['nome']}")
        print("." * 40)

        # 1. Checa se script existe
        if not os.path.exists(caminho_script):
            log(f"Script não encontrado: {script}", "ERRO")
            continue

        start = time.time()
        
        # 2. Executa o Script
        try:
            # capture_output=False faz o print do script filho aparecer em tempo real (se configurado)
            # Mas aqui usaremos True para capturar e organizar o log
            resultado = subprocess.run(
                ["python", script],
                capture_output=True,
                text=True
            )
            
            # Mostra o que o script falou (Logs internos)
            if resultado.stdout:
                print(f"📝 Log do {script}:")
                print(resultado.stdout.strip())
            
            if resultado.stderr:
                print(f"⚠️ Avisos/Erros do {script}:")
                print(resultado.stderr.strip())

            # 3. Analisa resultado e confere banco
            if resultado.returncode == 0:
                log(f"Execução de {script} finalizada com SUCESSO.", "SUCESSO")
                
                # --- AQUI ACONTECE A MÁGICA ---
                # Imediatamente após rodar, conferimos o banco
                conferir_banco(tarefa["banco"], tarefa["tabela"], tarefa["coluna_data"])
                # ------------------------------
                
            else:
                log(f"Falha na execução de {script}.", "ERRO")
                erros_totais += 1

        except Exception as e:
            log(f"Erro crítico ao tentar chamar o python: {e}", "ERRO")
            erros_totais += 1
            
        print(f"⏱️ Tempo da etapa: {time.time() - start:.2f}s")

    print("\n" + "=" * 80)
    if erros_totais > 0:
        log(f"Processo finalizado com {erros_totais} falhas. Verifique acima.", "ERRO")
        sys.exit(1) # Força erro no GitHub Actions para ficar vermelho
    else:
        log("Todos os scripts rodaram e os dados foram verificados no banco.", "SUCESSO")

if __name__ == "__main__":
    rodar_pipeline()
