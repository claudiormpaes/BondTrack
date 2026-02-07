name: Robô ETL Anbima

on:
  schedule:
    # Roda de Terça a Sábado às 10:00 UTC (07:00 Horário de Brasília)
    - cron: '0 10 * * 2-6'
  workflow_dispatch:

jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    
    steps:
      - name: Baixar código
        uses: actions/checkout@v3

      - name: Instalar Python com Cache
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          cache: 'pip' # <--- Cache ativado: economiza tempo nas próximas execuções

      - name: Instalar bibliotecas Python
        run: pip install -r requirements.txt

      - name: Instalar Navegador (Playwright)
        # Instala apenas o Chromium e dependências essenciais (mais rápido que instalar todos)
        run: playwright install chromium --with-deps

      - name: RODAR O ETL
        env:
          GITHUB_ACTIONS: 'true' # Avisa o script Python que estamos na nuvem
        run: python main_etl.py

      - name: Salvar novo Banco de Dados
        run: |
          git config --global user.name 'Robo Claudio Paes'
          git config --global user.email 'robo@claudiopaes.com.br'
          
          # Tenta atualizar o repositório local antes de enviar (evita erro de rejeição)
          git pull --rebase origin main || echo "Rebase falhou, tentando merge normal"
          
          # Adiciona apenas os arquivos de banco de dados
          git add data/*.db
          
          # Comita se houver mudanças
          git commit -m "Auto-update DB: $(date +'%d/%m/%Y %H:%M')" || echo "Sem mudanças para salvar"
          
          # Envia para o GitHub
          git push
