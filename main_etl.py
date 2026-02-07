import subprocess
import datetime
import sys
import sqlite3
import os

# CONFIGURAÇÃO
# Verifique se o nome do banco está correto com o que você usa no GitHub Actions (ex: meu_app.db)
NOME_BANCO = "meu_app.db" 

# Lista dos scripts na ordem correta de execução
scripts = [
    "extrator_snd.py",
    "etl_curvas_anbima.py",
    "etl_precos_snd.py" 
]

def check_db_stats():
    """Verifica o status das tabelas no banco de dados após a execução."""
    if not os.path.exists(NOME_BANCO):
        print(f"\n⚠️  Alerta: O banco de dados '{NOME_BANCO}' não foi encontrado.")
        return

    conn = sqlite3.connect(NOME_BANCO)
    cursor = conn.cursor()
    
    # Adicione ou remova tabelas conforme a estrutura do seu banco
    tabelas = ['cadastro_snd', 'mercado_secundario', 'negociacao_snd']
    
    print("\n📊 STATUS ATUAL DO BANCO DE DADOS:")
    print("-" * 50)
    
    for t in tabelas:
        try:
            # Tenta contar registros
            count = cursor.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            
            # Tenta pegar a última data (assume que a coluna data_referencia existe)
            # Se suas tabelas usam outro nome para data, ajuste aqui
            try:
                last_date = cursor.execute(f"SELECT MAX(data_referencia) FROM {t}").fetchone()[0]
            except sqlite3.OperationalError:
                last_date = "N/A (Coluna data_referencia não encontrada)"

            print(f"📂 Tabela '{t}':")
            print(f"   ↳ Registros: {count}")
            print(f"   ↳ Última atualização: {last_date}")
            
        except sqlite3.OperationalError:
            print(f"⚠️  Tabela '{t}': Não encontrada ou erro de leitura.")
            
    conn.close()
    print("-" * 50)

def rodar_scripts():
    print(f"🚀 Iniciando Rotina de Dados - {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 60)
    
    resultados = {}

    for script in scripts:
        print(f"\n⏳ Executando: {script}...")
        try:
            # capture_output=True guarda o print() dos scripts filhos
            # text=True garante que venha como string e não bytes
            resultado = subprocess.run(["python", script], capture_output=True, text=True, check=True)
            
            print(f"✅ {script} concluído com sucesso.")
            
            # --- AQUI ESTÁ A MUDANÇA PRINCIPAL ---
            # Imprime o LOG (o que o script printou internamente)
            if resultado.stdout:
                print(f"📝 LOG DE SAÍDA ({script}):")
                print("-" * 20)
                print(resultado.stdout.strip())
                print("-" * 20)
            else:
                print(f"ℹ️  O script {script} não retornou mensagens de texto.")
            # -------------------------------------

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
            
            # Opcional: Para tudo se um script falhar
            # sys.exit(1) 

    print("\n" + "=" * 60)
    print("📋 RELATÓRIO FINAL DE EXECUÇÃO")
    print("=" * 60)
    for script, status in resultados.items():
        icon = '✅' if status == 'SUCESSO' else '❌'
        print(f"{icon} {script}: {status}")
    
    # Chama a verificação do banco no final
    check_db_stats()

if __name__ == "__main__":
    rodar_scripts()
