import subprocess
import datetime
import sys
import sqlite3
import os

# Lista dos scripts na ordem correta de execução
scripts = [
    "extrator_snd.py",
    "etl_curvas_anbima.py",
    "etl_precos_snd.py" 
]

def check_db_stats():
    """
    Verifica o status das tabelas nos bancos de dados REAIS na pasta data.
    """
    
    # Mapeamento: Caminho do arquivo -> Tabelas esperadas nele
    # Ajustado para procurar dentro da pasta 'data/'
    bancos_esperados = {
        "data/debentures_anbima.db": ["negociacao_snd"],
        "data/curvas_anbima.db": ["curvas_anbima"]
    }
    
    print("\n📊 STATUS ATUAL DO BANCO DE DADOS:")
    print("-" * 50)
    
    # Diretório base onde o script está rodando
    base_dir = os.path.dirname(__file__)

    for db_name, tabelas in bancos_esperados.items():
        # Monta o caminho completo (ex: /home/runner/.../data/debentures_anbima.db)
        db_path = os.path.join(base_dir, db_name)
        
        if not os.path.exists(db_path):
            print(f"❌ Banco não encontrado: {db_name}")
            continue
            
        print(f"🗄️  BANCO: {db_name}")
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            for t in tabelas:
                try:
                    # Conta o total de registros
                    count = cursor.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    
                    # Tenta pegar a data mais recente
                    # O nome da coluna de data varia entre os bancos
                    col_data = 'data_referencia' if 'curvas' in t else 'data_base'
                    
                    try:
                        last_date = cursor.execute(f"SELECT MAX({col_data}) FROM {t}").fetchone()[0]
                    except:
                        last_date = "N/A"
                        
                    print(f"   ✅ Tabela '{t}': {count} registros (Última atualização: {last_date})")
                except Exception as e:
                    print(f"   ⚠️  Tabela '{t}': Erro ao ler ({e})")
            
            conn.close()
        except Exception as e:
            print(f"   ❌ Erro ao conectar no banco: {e}")
        print("-" * 30)

def rodar_scripts():
    print(f"🚀 Iniciando Rotina de Dados - {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 60)
    
    resultados = {}

    for script in scripts:
        print(f"\n⏳ Executando: {script}...")
        try:
            # capture_output=True guarda o print() dos scripts filhos
            # text=True garante que venha como string
            resultado = subprocess.run(["python", script], capture_output=True, text=True, check=True)
            
            print(f"✅ {script} concluído com sucesso.")
            
            # Imprime o LOG (o que o script printou internamente)
            if resultado.stdout:
                print(f"📝 LOG DE SAÍDA ({script}):")
                print("-" * 20)
                print(resultado.stdout.strip())
                print("-" * 20)
            
            resultados[script] = "SUCESSO"

        except subprocess.CalledProcessError as e:
            print(f"❌ ERRO CRÍTICO em {script}:")
            print("🔻 Saída de Erro (Traceback):")
            print(e.stderr)
            
            # Se houver stdout antes do erro, mostra também para ajudar no debug
            if e.stdout:
                print("🔻 Logs anteriores ao erro:")
                print(e.stdout)
                
            resultados[script] = "FALHA"
            # Continua para o próximo script mesmo com erro (opcional)

    print("\n" + "=" * 60)
    print("📋 RELATÓRIO FINAL DE EXECUÇÃO")
    print("=" * 60)
    for script, status in resultados.items():
        icon = '✅' if status == 'SUCESSO' else '❌'
        print(f"{icon} {script}: {status}")
    
    # Chama a verificação corrigida dos bancos
    check_db_stats()

if __name__ == "__main__":
    rodar_scripts()
