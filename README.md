# Python + Rust: Análise de performance — Pandas vs Polars

Uma demonstração prática que compara três abordagens arquiteturais fundamentalmente diferentes para processamento de dados em Python.

## Objetivo

Este projeto fornece evidências empíricas de por que o **Polars (Rust)** é superior ao **Pandas (NumPy)** para grandes volumes de dados, isolando as contribuições de cada componente arquitetural:

- **Pandas (NumPy)**: Python + C, monothread (avaliação eager)
- **Pandas + PyArrow**: Python + memória columnar (avaliação eager)
- **Polars (Rust)**: Rust + multithreading + avaliação lazy + otimização de queries

## Metodologia

### 1. Dados sintéticos

Geramos datasets de tamanhos crescentes para revelar os pontos de inflexão arquitetónicos:

- **Small (1K linhas)**: mede overhead de inicialização
- **Medium (1M linhas)**: início da separação por velocidade pura (CPU/GIL)
- **Large (10M linhas)**: diferenças de memória tornam-se aparentes
- **XLarge (100M linhas)**: o ponto crítico onde lazy evaluation e multithreading dominam

#### Estrutura do dataset

Fato principal: `fact_content_performance`
- `content_id` (UUID): ~500k valores únicos (chave de JOIN)
- `event_date` (date): simulação de 1 ano
- `region_country` (categórica): 50 países (importante para teste de memória)
- `views` (integer): 1 a 1000
- `engagement_score` (float): 0 a 10
- `process_status` (categórica): 3 valores ("Completed", "Processing", "Failed")

Dimensão: `dim_content_metadata`
- `content_id` (UUID): chave de JOIN
- `content_category` (categórica): 10 categorias (Python, Rust, MLOps, APIs, Cloud, etc.)

Formato: **Apache Parquet** (nativo do Arrow, otimizado para leitura columnar)

### 2. Query de "batalha" (workload complexo)

A mesma operação é executada em todos os três engines para garantir comparação justa:

```python
# Leitura + JOIN + FILTRO + AGREGAÇÃO + ORDENAÇÃO
fact.join(dim, on="content_id")
    .filter(process_status != "Failed")
    .groupby("region_country")
    .agg({
        "views": "sum",
        "engagement_score": "mean",
    })
    .sort("views", descending=True)
```

Esta query testa:
- **I/O**: leitura de parquet
- **JOIN**: desempenho no casamento de chaves
- **Filtro**: teste de "predicate pushdown" (Polars otimiza; Pandas não)
- **Agregação**: GROUP BY + cálculos estatísticos
- **Ordenação**: sort final

### 3. Métricas capturadas

Para cada cenário e engine, medimos:
- **Tempo de execução (segundos)**: wall-clock via `time.perf_counter()`
- **Pico de memória (MB)**: máximo alocado via `tracemalloc`

### 4. Visualizações arquitetônicas

#### Gráfico 1: Tempo de execução (escala logarítmica)

Por que usar escala logarítmica? Com Polars ~31x mais rápido, uma escala linear esconderia as diferenças.

O que observamos:
- Small/Medium: diferenças pequenas (overhead de inicialização)
- Medium → Large: Polars começa a se destacar (multithreading vs GIL)
- Large → XLarge: Polars dispara (lazy evaluation + otimização de queries)

Mensagem: apenas Rust/multithreading escala dessa forma.

#### Gráfico 2: Uso de memória (cenários grandes: 10M & 100M)

Contraste visual extremo:
- Pandas: ~20 GB (pico)
- Pandas + PyArrow: ~13.6 GB
- Polars: praticamente insignificante (~30 KB)

Diferença observada: ~677.000x (20 GB vs 30 KB)

O que isso demonstra:
- Polars com predicate pushdown carrega apenas o necessário
- Pandas (avaliação eager) tende a carregar mais dados e gerar overhead
- PyArrow reduz parte do overhead, mas a limitação estrutural persiste

#### Gráfico 3: Fator de overhead de RAM (100M apenas)

Métrica: pico de RAM / tamanho do arquivo em disco

Para ~500 MB de dados:
- **Pandas**: ~40x o tamanho do arquivo
- **Pandas + PyArrow**: ~27x o tamanho do arquivo
- **Polars**: ~0x o tamanho do arquivo

Por que isso importa:
- Explica por que data centers sobreprovisionam hardware para workloads com Pandas
- Em clusters como Hadoop/Spark costuma-se precisar de muitas vezes o tamanho do dado em memória
- Polars reduz significativamente esse custo de infraestrutura

## Resultados esperados (@ 100M linhas)

| Métrica    | Pandas (NumPy)   | Pandas + PyArrow | Polars (Rust)          |
| ---------- | ---------------- | ---------------- | ---------------------- |
| Velocidade | Baseline         | 4.1x             | **31.1x**              |
| Memória    | ~20 GB           | ~13.6 GB         | **~30 KB**             |
| Overhead   | ~40x             | ~27x             | **~0x**                |
| Threading  | Monothread (GIL) | Monothread (GIL) | **Multithreaded**      |
| Execução   | Eager            | Eager            | **Lazy (otimizado)**   |
| Query Opt  | Nenhuma          | Nenhuma          | **Predicate Pushdown** |

## Como usar

### Instalação

```bash
uv sync
```

### Gerar dados sintéticos

```bash
# Todos os cenários
uv run demo generate

# Apenas um cenário
uv run demo generate --size large
uv run demo generate --size xlarge
```

Arquivos gerados em `data/`:
- `fact_content_performance_{small,medium,large,xlarge}.parquet`
- `dim_content_metadata_{small,medium,large,xlarge}.parquet`

### Executar benchmarks

```bash
# Todos os cenários e engines
uv run demo benchmark

# Cenário específico
uv run demo benchmark --scenario xlarge

# Engine específico
uv run demo benchmark --engine polars

# Combinado
uv run demo benchmark --scenario large --engine pandas
```

Resultados salvos em `results/`:
- `pandas.csv`
- `pandas-pyarrow.csv`
- `polars.csv`

### Visualizações interativas

```bash
uv run jupyter notebook analysis.ipynb
```

A notebook contém:
1. Gráfico de tempo (escala log): mostra o ganho do Polars
2. Gráfico de memória: contraste extremo entre engines
3. Fator de overhead: custos ocultos de infraestrutura
4. Tabela resumida: comparação arquitetural completa
5. Análise detalhada: speedup e overhead por cenário

### Limpeza

```bash
uv run demo cleanup
```

Remove `data/` e `results/`.

---

Demo para apresentação: "Python + Rust — uma comparação com dados reais".
