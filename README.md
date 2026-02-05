# 📊 BondTrack - Plataforma Profissional de Análise de Debêntures

**BondTrack** é uma plataforma completa para análise do mercado brasileiro de debêntures, integrando dados do SND (Sistema Nacional de Debêntures) e ANBIMA.

## 🚀 Funcionalidades

### 🏠 Home Dashboard
- KPIs do mercado em tempo real
- Mapa Risco x Retorno interativo
- Destaques e principais indicadores
- Distribuição por categoria e fonte de dados

### 📡 Radar de Mercado
- **Top Movers:** Maiores taxas e durations
- **Heatmap:** Visualização de taxas por indexador e prazo
- **Curvas de Juros:** IPCA, CDI, Prefixado e outros
- **Distribuições:** Box plots por categoria

### 🔍 Screener Pro
- **Filtros Avançados:** Accordion com múltiplos critérios
  - Mercado: Categoria, Indexador, Emissor, Fonte
  - Crédito/Risco: Range de taxa e duration
  - Liquidez: Clusters de prazo
- **Scatter Plot:** Mapa interativo Risco x Retorno
  - Cores por categoria (IPCA Incentivado verde, CDI+ azul, etc.)
  - Símbolos por fonte de dados
  - Tamanho por PU
- **Export:** Download CSV dos resultados filtrados

### 📈 Análise de Ativo
- **Busca Inteligente:** Dropdown com código, emissor e indexador
- **Ficha Técnica Completa:** Todos os dados do ativo
- **Métricas Financeiras:** Taxa, PU, Duration, DV01, Spread vs ANBIMA
- **Cenários de Taxa:** Simulação com Duration e Convexidade
- **Ativos Similares:** Comparação com mesma categoria/indexador

### 🔎 Auditoria de Dados
- **Score de Qualidade:** Indicador 0-100 da qualidade dos dados
- **Análise de Completude:** Campos válidos vs inválidos
- **Detecção de Duplicatas:** Identificação de registros repetidos
- **Log de Inconsistências:** Taxas/durations negativas
- **Distribuição por Fonte:** Cobertura SND vs Anbima
- **Export:** Relatório completo em JSON

## 📦 Instalação

### Pré-requisitos
- Python 3.9 ou superior
- pip (gerenciador de pacotes Python)

### Passo a Passo

1. **Clone o repositório:**
```bash
git clone <url-do-repositorio>
cd bondtrack-app
```

2. **Crie um ambiente virtual (recomendado):**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. **Instale as dependências:**
```bash
pip install -r requirements.txt
```

4. **Instale o Playwright (para ETL):**
```bash
playwright install chromium
```

5. **Configure o banco de dados:**
   - Coloque o arquivo `debentures_anbima.db` na pasta `/data`
   - Ou execute o ETL para criar o banco:
```bash
python extrator_snd.py
```

6. **Execute a aplicação:**
```bash
streamlit run app.py
```

7. **Acesse no navegador:**
   - Local: `http://localhost:8501`

## 📁 Estrutura do Projeto

```
bondtrack-app/
├── app.py                    # Entry Point e Landing Page
├── requirements.txt          # Dependências Python
├── README.md                 # Este arquivo
├── .gitignore               # Arquivos ignorados pelo Git
├── extrator_snd.py          # ETL para coleta de dados SND
│
├── /src                     # Módulos Core
│   ├── __init__.py
│   ├── data_engine.py       # ETL, Merge SND+Anbima, Limpeza
│   ├── financial_math.py    # Cálculos: Duration, Convexidade, Spreads
│   └── visuals.py           # Templates Plotly (Dark Mode)
│
├── /pages                   # Páginas Streamlit
│   ├── 1_Radar_Mercado.py   # Top Movers, Heatmap, Curvas
│   ├── 2_Screener_Pro.py    # Filtros Avançados, Scatter Plot
│   ├── 3_Analise_Ativo.py   # Dossiê Completo do Ativo
│   └── 4_Auditoria.py       # Data Quality Center
│
└── /data                    # Banco de Dados
    └── debentures_anbima.db # SQLite com dados SND + ANBIMA
```

## 🗄️ Estrutura do Banco de Dados

### Tabela: `mercado_secundario` (ANBIMA)
- **codigo:** Ticker da debênture
- **data_referencia:** Data dos dados (DD/MM/YYYY)
- **taxa_indicativa:** Taxa do mercado secundário (%)
- **pu:** Preço Unitário
- **duration:** Duration em anos

### Tabela: `cadastro_snd` (SND)
- **codigo:** Ticker da debênture (chave de merge)
- **Empresa:** Nome do emissor
- **indice:** Indexador
- **deb_incent:** Debênture incentivada (S/N)
- **data_vencimento:** Vencimento
- **data_emissao:** Emissão
- ... outros campos cadastrais

### Chave Primária
**TICKER + DATA_REFERENCIA** para dados únicos por dia

## 🔄 ETL - Atualização de Dados

### Manual
```bash
python extrator_snd.py
```

### Automação (Futura)
- **Cron Job (Linux/Mac):**
```bash
0 19 * * 1-5 cd /path/to/bondtrack-app && python extrator_snd.py
```

- **Task Scheduler (Windows):**
  - Agendar execução diária de `extrator_snd.py` às 19h

## 🎨 Design System

### Paleta de Cores (Dark Mode)
- **Background:** `#0e1117`
- **Texto:** `#fafafa`
- **Verde Neon:** `#00CC96` (IPCA Incentivado, destaque positivo)
- **Roxo Neon:** `#AB63FA` (% CDI, secundário)
- **Azul:** `#636EFA` (CDI+)
- **Vermelho/Laranja:** `#EF553B` (IPCA Não Incentivado, alertas)
- **Laranja:** `#FFA15A` (Prefixado)

### Categorias de Indexador
| Categoria              | Cor       | Regra                                    |
|------------------------|-----------|------------------------------------------|
| IPCA Incentivado       | Verde     | IPCA + Incentivada = S                   |
| IPCA Não Incentivado   | Vermelho  | IPCA + Incentivada = N                   |
| CDI +                  | Azul      | CDI + Taxa < 30 (spread)                 |
| % CDI                  | Roxo      | CDI + Taxa > 30 (percentual)             |
| Prefixado              | Laranja   | PRÉ ou PREFIXADO                         |
| Outros                 | Verde Claro| Demais indexadores                      |

## 📊 Cálculos Financeiros

### Duration de Macaulay
```python
Duration = Σ(t × VP_t) / Σ(VP_t)
```

### Duration Modificada
```python
Duration_Mod = Duration_Macaulay / (1 + YTM/n)
```

### Convexidade
```python
Convexidade = Σ(t × (t+1) × VP_t) / (VP_total × (1+y)²)
```

### DV01
```python
DV01 = Preço × Duration_Mod × 0.0001
```

### Spread
```python
Spread (bps) = (Taxa_Ativo - Taxa_Benchmark) × 100
```

## 🚀 Deploy

### Streamlit Cloud (Recomendado)
1. Faça push do código para GitHub
2. Acesse [share.streamlit.io](https://share.streamlit.io)
3. Conecte seu repositório
4. Configure:
   - **Main file:** `app.py`
   - **Python version:** 3.9+
5. Deploy!

### Heroku
```bash
# Criar Procfile
echo "web: streamlit run app.py --server.port=$PORT" > Procfile

# Deploy
heroku create bondtrack-app
git push heroku main
```

### Docker
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
```

## 🛠️ Desenvolvimento

### Executar em modo de desenvolvimento
```bash
streamlit run app.py --server.runOnSave true
```

### Testes (Futuros)
```bash
pytest tests/
```

### Formatação de código
```bash
black .
```

## 🔮 Roadmap

### v1.1 (Próxima Versão)
- [ ] Integração com API B3 (volumes negociados)
- [ ] Ratings de crédito (Moody's, S&P, Fitch)
- [ ] Histórico de preços (séries temporais)
- [ ] Alertas personalizados
- [ ] Comparação com benchmarks (CDI, IPCA)

### v2.0 (Futuro)
- [ ] Machine Learning para precificação
- [ ] Portfolio tracking
- [ ] Backtesting de estratégias
- [ ] API REST para integração
- [ ] Mobile app

## 📝 Regras de Negócio

### Higienização de Indexadores
- `DI` → `CDI`
- `D.I.` → `CDI`
- `IGPM` → `IGP-M`
- `IPC-A` → `IPCA`
- `PRE` → `PRÉ`
- `PREFIXADO` → `PRÉ`

### Master Table
- **Merge:** SND (cadastro) + ANBIMA (preços)
- **Chave:** `codigo` (ticker)
- **Fonte:** Coluna indicando origem dos dados
  - `SND + Anbima`: Dados completos
  - `Anbima`: Apenas preços
  - `SND`: Apenas cadastro

### Data Mais Recente
Detecção automática via:
```sql
SELECT MAX(data_referencia) FROM mercado_secundario
```

## 🐛 Troubleshooting

### Erro: "Banco de dados não encontrado"
**Solução:** Certifique-se de que `debentures_anbima.db` está em `/data`

### Erro: "Nenhuma data disponível"
**Solução:** Execute o ETL para popular o banco:
```bash
python extrator_snd.py
```

### Erro: "Module not found"
**Solução:** Instale as dependências:
```bash
pip install -r requirements.txt
```

### Erro do Playwright: "Executable doesn't exist" ou "Browser closed"
**Solução:** O Playwright precisa baixar os binários do navegador antes do primeiro uso:
```bash
# Instalar apenas o Chromium (recomendado)
playwright install chromium

# OU instalar todos os navegadores
playwright install

# Se tiver problemas de permissão (Linux)
sudo playwright install-deps
playwright install chromium
```

**Nota para ETL de Volume (etl_precos_snd.py):**
Este ETL usa Playwright para acessar o site do SND. Certifique-se de:
1. Ter executado `playwright install chromium`
2. Ter conexão com a internet
3. O site do SND estar disponível

### Performance lenta
**Solução:** 
- Verifique o cache do Streamlit (`@st.cache_data`)
- Reduza o TTL do cache se necessário
- Otimize queries SQL

## 📄 Licença

Este projeto é de código aberto e está disponível sob a licença MIT.

## 🤝 Contribuindo

Contribuições são bem-vindas! Por favor:
1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📧 Contato

Para dúvidas ou sugestões, abra uma issue no GitHub.

---

**⚡ Powered by Streamlit | 📊 Dados: SND + ANBIMA | 🔄 Atualização Diária**
