import sqlite3
import pandas as pd
import os
import sys

# Ajuste de exibição do Pandas (para não cortar colunas no terminal)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def verificar_banco():
    print("="*60)
    print("🕵️  AUDITORIA DE BANCO DE DADOS - BOND TRACK")
    print("="*60)

    # Define onde procurar os dados
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")

    # Lista de Bancos esperados
    bancos = [
        "debentures_anbima.db",
        "curvas_anbima.db"
    ]

    if not os.path.exists(data_dir):
        print(f"❌ PASTA DE DADOS NÃO ENCONTRADA: {data_dir}")
        return

    total_arquivos = [f for f in os.listdir(data_dir) if f.endswith('.db')]
    print(f"📂 Arquivos encontrados na pasta data/: {total_arquivos}")
    print("-" * 60)

    for nome_banco in bancos:
        caminho_banco = os.path.join(data_dir, nome_banco)
        
        print(f"\n🗄️  ANALISANDO: {nome_banco}")
        
        if not os.path.exists(caminho_banco):
            print(f"   ❌ Arquivo não existe neste caminho: {caminho_banco}")
            continue

        try:
            conn = sqlite3.connect(caminho_banco)
            cursor = conn.cursor()
            
            # Pega todas as tabelas do banco
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tabelas = cursor.fetchall()

            if not tabelas:
                print("   ⚠️  O banco existe mas está SEM TABELAS.")
                conn.close()
                continue

            for tabela in tabelas:
                nome_tabela = tabela[0]
                if "sqlite" in nome_tabela or "metadata" in nome_tabela:
                    continue # Pula tabelas internas
                
                print(f"   📋 Tabela: '{nome_tabela}'")
                
                # 1. Contagem Total
                count = cursor.execute(f"SELECT COUNT(*) FROM {nome_tabela}").fetchone()[0]
                print(f"      ↳ Total de Linhas: {count}")

                if count == 0:
                    print("      ⚠️  TABELA VAZIA!")
                    continue

                # 2. Verifica Data mais recente
                # Tenta descobrir o nome da coluna de data
                colunas_info = cursor.execute(f"PRAGMA table_info({nome_tabela})").fetchall()
                colunas = [c[1] for c in colunas_info]
                
                col_data = next((c for c in colunas if 'data' in c and 'atualizacao' not in c), None)
                if not col_data: col_data = 'data_base' # Fallback

                if col_data in colunas:
                    last_date = cursor.execute(f"SELECT MAX({col_data}) FROM {nome_tabela}").fetchone()[0]
                    print(f"      ↳ Data mais recente ({col_data}): {last_date}")
                else:
                    print("      ↳ Coluna de data não identificada automaticamente.")

                # 3. Carrega Amostra com Pandas
                print("\n      🔎 AMOSTRA DOS ÚLTIMOS 5 REGISTROS:")
                query = f"SELECT * FROM {nome_tabela} ORDER BY {col_data} DESC LIMIT 5" if col_data in colunas else f"SELECT * FROM {nome_tabela} LIMIT 5"
                df = pd.read_sql_query(query, conn)
                print(df.to_string(index=False))
                
                # 4. Verificação de Qualidade (Específico para Preços)
                if 'volume_total' in df.columns:
                    zeros = cursor.execute(f"SELECT COUNT(*) FROM {nome_tabela} WHERE volume_total = 0").fetchone()[0]
                    print(f"\n      ⚠️  Registros com Volume Zerado: {zeros}")
                    
                    # Verifica se o PU está formatado como número
                    pu_medio = df['pu_medio'].iloc[0]
                    print(f"      🔢 Teste de Formato (PU Médio): {pu_medio} (Tipo: {type(pu_medio)})")
                    
                    if isinstance(pu_medio, str):
                        print("      ❌ ATENÇÃO: Os números estão salvos como TEXTO. O cálculo de volume pode estar errado.")
                    else:
                        print("      ✅ Formato numérico OK.")

            conn.close()

        except Exception as e:
            print(f"   ❌ Erro ao ler banco: {e}")
        
        print("-" * 60)

if __name__ == "__main__":
    verificar_banco()
