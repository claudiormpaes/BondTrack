import subprocess
import datetime

# Lista dos scripts que você quer rodar
scripts = [
    "extrator_snd.py",
    "etl_curvas_anbima.py",
    # "seu_terceiro_script.py" 
]

def rodar_scripts():
    logs = []
    print(f"🚀 Iniciando Rotina de Dados - {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("-" * 50)

    for script in scripts:
        print(f"⏳ Executando: {script}...")
        try:
            # Executa o script e espera terminar
            resultado = subprocess.run(["python", script], capture_output=True, text=True, check=True)
            logs.append(f"✅ {script}: SUCESSO")
            print(f"✅ {script} concluído com sucesso.")
        except subprocess.CalledProcessError as e:
            logs.append(f"❌ {script}: ERRO")
            print(f"❌ Erro em {script}:")
            print(e.stderr) # Mostra o erro específico no terminal
        except Exception as e:
            logs.append(f"⚠️ {script}: FALHA CRÍTICA ({str(e)})")

    # Relatório Final no Terminal
    print("\n" + "="*30)
    print("📋 RELATÓRIO FINAL DE EXECUÇÃO")
    print("="*30)
    for log in logs:
        print(log)
    print("="*30)

if __name__ == "__main__":
    rodar_scripts()
