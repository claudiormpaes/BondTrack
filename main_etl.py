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
