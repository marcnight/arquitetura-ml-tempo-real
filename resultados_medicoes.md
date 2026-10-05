# Resultados das medições — arquitetura de Big Data e MLOps em tempo real

Data das medições: 3 de outubro de 2026. Todos os números deste documento saíram de comandos executados nesta máquina; cada tabela indica o CSV de origem, dentro de `medicoes/`. Nada foi estimado. O que não pôde ser medido está listado na última seção, com o motivo.

Convenções: "dp" é o desvio-padrão amostral (n − 1); "N" é o número de repetições; vazão em transações por segundo (tx/s).

## Resumo dos resultados

| Item | Resultado principal |
|---|---|
| B | Processo único, base completa (284.807 tx): 2,262 s ± 0,182 s sem métricas (N = 30); 168,504 s ± 2,613 s com o registro de métricas ligado (N = 5) |
| C | 10.000 transações pelos dois caminhos: divergência máxima de probabilidade 3,33 × 10⁻¹⁶; 0 rótulos diferentes |
| D | O Spark sustentou todas as taxas testadas (até 4.919 tx/s com 1 worker). A saturação ponta a ponta ocorre entre 1.000 e 2.000 tx/s, na ponte de métricas (≈1.111 tx/s) |
| E | Com 1 partição, mais workers não aumentam a vazão (≈15 mil tx/s). Com 3 workers e 3 partições, 43,3 mil tx/s (2,8 vezes a de 1 worker) |
| G | Queda do worker: 0 perdidas, 0 duplicadas. Queda do driver: o fluxo só retoma com reinício automático; nesse caso, 0 perdidas e 265 a 363 duplicadas em 45.000 |
| I | Do zero (`down -v` e `up`): 31,6 s ± 1,2 s até todos os serviços saudáveis (N = 3) |

## Correções aplicadas antes das medições

O diff completo está em `medicoes/correcoes.diff` (gerado com `diff -ruN` entre a cópia original e o estado final).

| # | Correção pedida | O que foi feito | Arquivos |
|---|---|---|---|
| 1 | Conector Kafka do Spark na versão exata do PySpark | `KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"` (resolve para 4.1.2) | `src/spark_streaming.py` |
| 2 | Campo `timestamp` no esquema e nas predições; latência = agora − timestamp | `timestamp`, `tx_id` e `run_id` entram no esquema lido e saem nas predições; a ponte calcula `now - rec["timestamp"]` e grava cada observação em CSV | `src/spark_streaming.py`, `src/kafka_producer.py`, `src/kafka_metrics_bridge.py`, `src/metrics_exporter.py` |
| 3 | Alvo do serviço de inferência no Prometheus | O serviço (ponte de métricas) foi levado para dentro do compose; alvo `metrics-bridge:8000`. Os 5 alvos aparecem UP em `/targets` (captura no item H) | `docker-compose.yml`, `prometheus/prometheus.yml` |
| 4 | Spark apontando para o master do compose | `.master(cfg.spark_master)`, com padrão `spark://spark-master:7077`; o driver roda no serviço `spark-streaming` do compose | `src/config.py`, `src/spark_streaming.py`, `docker-compose.yml` |
| 5 | MLflow apontando para o servidor do compose | `mlflow_tracking_uri` lido de `MLFLOW_TRACKING_URI`, com padrão `http://localhost:5000` (servidor do compose) | `src/config.py`, `docker-compose.yml` |

Mudanças adicionais, necessárias para a pilha subir ou para medir:

- **Imagens:** `bitnami/spark:latest` e `bitnami/mlflow:latest` não existem mais no registro (erro em `medicoes/logs/pull_bitnami.log`). Foram substituídas por uma imagem própria `tcc-spark:4.1.2` (`docker/spark/Dockerfile`) e por `ghcr.io/mlflow/mlflow:v3.13.0`.
- **Kafka em modo KRaft:** a imagem `confluentinc/cp-kafka:8.2.1` não aceita mais ZooKeeper (erro "KAFKA_PROCESS_ROLES is not set"). O serviço `zookeeper` foi removido.
- **Mesma imagem para driver, workers e ponte:** o PySpark exige a mesma versão menor de Python no driver e nos executores (3.14 em todos).
- **Limite de CPU do worker:** cada worker tem `cpus: 2` e `--cores 2`, para que o número de workers corresponda a recursos distintos.
- **Sink de console desligado no compose** (`ENABLE_CONSOLE_SINK=0`): ele criava uma segunda consulta que refazia a inferência.
- **Histograma de latência:** buckets estendidos até 300 s em `src/metrics_exporter.py` (o maior era insuficiente para as latências sob saturação).
- **Ponte de métricas:** `fetch_max_bytes=512 KiB` e `max_partition_fetch_bytes=256 KiB`, após a falha descrita no item D.
- **Reinício automático do driver:** `restart: on-failure` no serviço `spark-streaming`, após a falha descrita no item G. Essa linha foi acrescentada depois das medições D, E e I, que rodaram sem ela.
- **Painel do Grafana:** `dashboard.json` regenerado como painel provisionado válido, com os mesmos painéis; a fonte de dados passou a `http://prometheus:9090`.

## A. Ambiente

Comando: `bash medicoes/scripts/A_ambiente.sh`. Saídas completas em `medicoes/A_ambiente/`.

| Item | Valor medido | Arquivo |
|---|---|---|
| Processador | AMD Ryzen 3 5300U; 1 soquete, 4 núcleos, 2 threads por núcleo, 8 CPUs lógicas | `lscpu.txt` |
| Memória | 7,1 GiB no total; 4,0 GiB de swap | `free.txt` |
| Sistema | Linux 7.2.2-arch1-1, x86_64 | `uname.txt` |
| Docker | 29.7.2 (build a7dcaa6fdb) | `docker_version.txt` |
| Docker Compose | 5.5.0 | `docker_compose_version.txt` |
| Python na imagem Spark | 3.14.8 | `python_java_imagem_spark.txt` |
| Java na imagem Spark | OpenJDK 21.0.12.1 | `python_java_imagem_spark.txt` |
| Bibliotecas (host) | `pip freeze` completo | `pip_freeze_host.txt` |
| Bibliotecas (imagem Spark) | pyspark 4.1.2, pandas 2.3.3, numpy 2.4.6, pyarrow 24.0.0, scikit-learn 1.9.0, imbalanced-learn 0.14.2, joblib 1.5.3, kafka-python 3.0.0, prometheus_client 0.25.0 | `pip_freeze_imagem_spark.txt` |

Imagens (identificadores sha256 e datas em `imagens.csv`):

| Serviço | Imagem |
|---|---|
| kafka, kafka-init | `confluentinc/cp-kafka:8.2.1` |
| kafka-exporter | `danielqsj/kafka-exporter:latest` |
| spark-master, spark-worker, spark-streaming, metrics-bridge | `tcc-spark:4.1.2` (construída localmente) |
| prometheus | `prom/prometheus:latest` |
| grafana | `grafana/grafana:latest` |
| mlflow | `ghcr.io/mlflow/mlflow:v3.13.0` |

Limitação do ambiente: tudo roda em uma única máquina. Produtor de carga, Kafka, Spark e monitoramento disputam as mesmas 8 CPUs lógicas e os mesmos 7,1 GiB.

## B. Inferência em processo único

Mede somente `pipeline.predict_batch(df)` sobre a base completa (284.807 transações); a leitura do CSV e a carga do modelo ficam fora do tempo. Modelo: `models/best_model.pkl` (Random Forest, 200 árvores).

Comandos:

```bash
venv/bin/python medicoes/scripts/B_inferencia_processo_unico.py 30 0 medicoes/B_inferencia/sem_metricas.csv
venv/bin/python medicoes/scripts/B_inferencia_processo_unico.py 5 1 medicoes/B_inferencia/com_metricas.csv
```

| Registro de métricas | N | Tempo médio (s) | dp (s) | Mín. (s) | Máx. (s) | Vazão média (tx/s) | dp (tx/s) | CSV |
|---|---|---|---|---|---|---|---|---|
| Desligado | 30 | 2,262 | 0,182 | 2,181 | 3,196 | 126.474 | 7.458 | `B_inferencia/sem_metricas.csv` |
| Ligado | 5 | 168,504 | 2,613 | 166,099 | 172,949 | 1.691 | 26 | `B_inferencia/com_metricas.csv` |

Fraudes preditas: 542 em todas as repetições, nos dois modos.

A primeira repetição sem métricas é a mais lenta (3,196 s; as demais ficam entre 2,18 s e 2,35 s). Com o registro ligado, o tempo é dominado pela atualização das métricas do Prometheus transação por transação em Python, e não pela inferência.

O modo com métricas teve 5 repetições em vez de 30, porque cada uma leva cerca de 3 minutos. As repetições com métricas rodaram com a pilha Docker parada.

## C. Equivalência entre os caminhos Pandas e Spark

As mesmas 10.000 transações (as primeiras da base) passaram pelo caminho Pandas (`predict_batch`) e pelo caminho Kafka → Spark → tópico `predictions`. A comparação é feita por `tx_id`.

Comando: `venv/bin/python medicoes/scripts/C_equivalencia.py`

| Medida | Valor |
|---|---|
| Transações enviadas / recebidas do Spark | 10.000 / 10.000 |
| Duplicadas | 0 |
| Divergência máxima de probabilidade | 3,3306690738754696 × 10⁻¹⁶ |
| Divergência média de probabilidade | 1,548 × 10⁻¹⁷ |
| Transações com probabilidade diferente (qualquer diferença) | 3.307 |
| Rótulos diferentes | 0 |
| Fraudes preditas (Pandas / Spark) | 37 / 37 |

Origem: `C_equivalencia/resumo.csv`; valores par a par em `C_equivalencia/comparacao_10000.csv`. N = 1 (uma única comparação).

As diferenças são da ordem do erro de arredondamento de ponto flutuante de 64 bits e não alteram nenhum rótulo.

## D. Ponta a ponta

Configuração: 1 worker Spark (2 núcleos), tópicos com 3 partições, 60 s de carga por execução, 5 repetições por taxa. A cada execução, tópicos e checkpoint são recriados e há um aquecimento de 3.000 mensagens.

Comandos:

```bash
venv/bin/python medicoes/scripts/D_ponta_a_ponta.py 100,500,1000 5 60
venv/bin/python medicoes/scripts/D_ponta_a_ponta.py 2000 5 60
venv/bin/python medicoes/scripts/D_ponta_a_ponta.py 5000 5 60 4   # 4 processos produtores
```

Como cada medida é obtida:

- **Vazão sustentada (Spark):** predições publicadas no tópico `predictions` durante a janela de carga, dividido pela duração.
- **Vazão da ponte:** observações registradas pela ponte de métricas na mesma janela.
- **Latência até a publicação:** carimbo do Kafka no registro da predição − `timestamp` do produtor.
- **Latência até a métrica:** instante em que a ponte registra a observação no Prometheus − `timestamp` do produtor. É a latência ponta a ponta pedida.
- **Quantis do Prometheus:** `histogram_quantile` sobre o histograma `inference_latency_seconds`, na janela da execução.
- **Lag:** offsets finais de `transactions` − offsets finais de `predictions`, amostrado a cada 1 s.
- **`inputRowsPerSecond` e `processedRowsPerSecond`:** médias dos microlotes com dados, lidas de `query.recentProgress`.

### Vazão e lag (média ± dp, N = 5)

| Taxa nominal (tx/s) | Taxa efetiva do produtor | Vazão sustentada (Spark) | Vazão da ponte | Lag máximo (mensagens) | `inputRowsPerSecond` | `processedRowsPerSecond` | Microlote (ms) |
|---|---|---|---|---|---|---|---|
| 100 | 100,00 ± 0,00 | 98,96 ± 0,38 | 98,44 ± 0,27 | 124 ± 21 | 102,2 ± 4,8 | 100,1 ± 0,1 | 692 ± 99 |
| 500 | 499,98 ± 0,04 | 497,10 ± 0,70 | 494,96 ± 0,41 | 386 ± 15 | 500,3 ± 0,1 | 502,4 ± 0,9 | 499 ± 8 |
| 1.000 | 999,90 ± 0,00 | 993,36 ± 2,01 | 988,54 ± 0,51 | 773 ± 51 | 1.001,4 ± 0,2 | 1.001,1 ± 2,7 | 518 ± 6 |
| 2.000 | 1.999,54 ± 0,05 | 1.978,84 ± 5,91 | 1.111,34 ± 34,95 | 3.528 ± 2.946 | 2.003,2 ± 1,6 | 2.019,2 ± 40,7 | 718 ± 111 |
| 5.000 | 4.992,44 ± 2,19 | 4.919,46 ± 33,40 | 868,44 ± 27,07 | 26.835 ± 6.859 | 4.997,5 ± 9,1 | 5.169,2 ± 158,3 | 1.218 ± 183 |

### Latência (segundos, média ± dp entre as 5 repetições)

| Taxa | Até a publicação p50 | p95 | p99 | Até a métrica p50 | p95 | p99 | Prometheus p50 | p95 | p99 |
|---|---|---|---|---|---|---|---|---|---|
| 100 | 0,79 ± 0,12 | 1,39 ± 0,18 | 1,67 ± 0,21 | 0,81 ± 0,12 | 1,41 ± 0,19 | 1,69 ± 0,21 | 0,85 ± 0,14 | 2,22 ± 0,14 | 2,61 ± 0,39 |
| 500 | 0,56 ± 0,01 | 0,89 ± 0,01 | 1,00 ± 0,03 | 0,62 ± 0,01 | 0,91 ± 0,02 | 1,02 ± 0,02 | 0,67 ± 0,01 | 0,98 ± 0,00 | 1,34 ± 0,36 |
| 1.000 | 0,58 ± 0,00 | 0,92 ± 0,01 | 1,02 ± 0,03 | 0,72 ± 0,01 | 1,05 ± 0,01 | 1,20 ± 0,04 | 0,74 ± 0,01 | 1,60 ± 0,13 | 2,32 ± 0,03 |
| 2.000 | 0,82 ± 0,13 | 1,40 ± 0,34 | 2,00 ± 1,19 | 24,00 ± 1,64 | 38,42 ± 1,89 | 39,92 ± 2,28 | 22,28 ± 1,99 | 54,84 ± 0,87 | 58,97 ± 0,17 |
| 5.000 | 1,45 ± 0,21 | 3,69 ± 0,54 | 5,69 ± 1,18 | 99,35 ± 0,85 | 167,68 ± 0,94 | 174,23 ± 0,77 | 99,39 ± 0,76 | 275,29 ± 0,35 | 295,06 ± 0,07 |

Em todas as 25 execuções: 0 erros de envio, 0 predições faltantes, 0 duplicadas e dreno completo.

Origem: `D_ponta_a_ponta/execucoes.csv` (uma linha por execução), `resumo.csv` (médias e dp), `lag_por_segundo.csv`, `progresso_microlotes.csv` e `brutos/*.csv.gz` (uma linha por predição).

### Ponto de saturação

- **Spark (1 worker, 2 núcleos):** não saturou em nenhuma taxa testada. A 5.000 tx/s nominais publicou 4.919 tx/s, com latência até a publicação de 1,45 s (p50) e 5,69 s (p99).
- **Ponta a ponta:** a saturação está entre 1.000 e 2.000 tx/s. A 1.000 tx/s a ponte acompanha (988,5 tx/s; p99 de 1,20 s). A 2.000 tx/s ela registra só 1.111 tx/s e a latência até a métrica sobe para 24,0 s (p50). O gargalo é a ponte de métricas, um processo Python de um único núcleo: o item F mostra que ela chega a ≈99 % de CPU nesse patamar.
- **Quantis do Prometheus sob saturação:** divergem dos quantis exatos (por exemplo, p99 de 295 s contra 174 s a 5.000 tx/s), porque são interpolados entre buckets largos (120 s e 300 s). Abaixo da saturação os dois concordam em décimos de segundo, com diferença maior no p95 e p99 pelo mesmo motivo.

### Falhas ocorridas no item D

| Falha | Evidência | Solução |
|---|---|---|
| A máquina suspendeu sozinha durante a primeira execução de 2.000 tx/s (07:14 a 10:50) | Linhas `D_t2000_r1_tentativa_falha` em `lag_por_segundo.csv` | Medições passaram a rodar sob `kde-inhibit --power --screenSaver systemd-inhibit --what=sleep:idle`; execução repetida |
| A ponte caiu a 2.000 tx/s com `InvalidReceiveError: Invalid frame length: 3145876`; em seguida a memória dela cresceu para ≈2,7 GiB e travou a máquina | `logs/falha_metrics_bridge_D_t2000_r1.log` | Limites de busca do consumidor reduzidos (ver correções); execução repetida |
| Um único processo produtor atingiu só 2.605,6 tx/s na taxa nominal de 5.000 | `D_ponta_a_ponta/falha_5000_produtor_unico.csv` | Patamar de 5.000 refeito com 4 processos produtores em paralelo |

As linhas com sufixo `_tentativa_falha`, `_produtor_unico` e `_interrompida` nos CSVs de lag e de recursos são dessas tentativas e não entram nas médias.

## E. Escalabilidade

Como o Spark não saturou nas taxas do item D, a capacidade foi medida pelo método do acúmulo: 300.000 mensagens são carregadas no tópico com o fluxo parado; o fluxo é então iniciado com `STARTING_OFFSETS=earliest` e mede-se a velocidade com que as predições são publicadas. O driver espera todos os executores registrarem antes do primeiro lote (`spark.cores.max` = 2 × workers e `spark.scheduler.minRegisteredResourcesRatio=1.0`).

Comando: `venv/bin/python medicoes/scripts/E_escalabilidade.py 300000 3`

A vazão da tabela é 300.000 dividido pelo tempo entre a primeira e a última predição publicada (amostragem dos offsets a cada 0,2 s). N = 3 por configuração.

| Workers | Partições | Executores com tarefas | Vazão média (tx/s) | dp (tx/s) | Relação com 1 worker, mesmas partições |
|---|---|---|---|---|---|
| 1 | 1 | 1 | 15.017 | 1.340 | 1,00 |
| 1 | 3 | 1 | 15.346 | 181 | 1,00 |
| 1 | 6 | 1 | 12.398 | 465 | 1,00 |
| 2 | 1 | 1 | 14.681 | 1.721 | 0,98 |
| 2 | 3 | 2 | 15.557 | 1.121 | 1,01 |
| 2 | 6 | 2 | 32.594 | 712 | 2,63 |
| 3 | 1 | 1 | 15.506 | 1.521 | 1,03 |
| 3 | 3 | 3 | 43.328 | 2.534 | 2,82 |
| 3 | 6 | — | não medido | — | — |

Origem: `E_escalabilidade/execucoes.csv`, `resumo.csv`, `serie_offsets.csv`, `falhas.csv`.

Leitura dos resultados:

- **Com 1 partição, acrescentar workers não muda a vazão.** Há uma única tarefa, e só um executor recebe trabalho (coluna "executores com tarefas").
- **2 workers com 3 partições não ganham sobre 1 worker.** Um dos executores fica com 2 das 3 tarefas e determina o tempo total.
- **O ganho aparece quando há partições suficientes para dividir a carga por igual:** 2,63 vezes com 2 workers e 6 partições; 2,82 vezes com 3 workers e 3 partições.
- **Com 1 worker, 6 partições rendem menos que 3** (12.398 contra 15.346 tx/s): 6 tarefas disputam os mesmos 2 núcleos.

Ressalvas sobre este item:

- Os 300.000 registros foram processados em um único microlote em todas as execuções. A vazão é a de escoamento de um acúmulo, e não a de regime com microlotes sucessivos.
- O tempo até a primeira predição (32 s a 40 s, coluna `tempo_ate_primeira_predicao_s`) inclui a subida do driver e a carga do modelo e não entra na vazão.
- O arquivo traz também `vazao_25_95_tx_s` (inclinação entre 25 % e 95 % das predições). Ela varia mais (dp de 13.534 tx/s em 3 workers e 3 partições) porque a publicação ocorre em rajadas; por isso a tabela usa o tempo entre a primeira e a última predição.
- A máquina tem 8 CPUs lógicas: com 3 workers (6 núcleos), sobram 2 para Kafka, driver e sistema.

### Falhas ocorridas no item E

| Falha | Evidência | Solução |
|---|---|---|
| Primeira passada inválida: o lote começava antes de todos os executores registrarem, e o número de workers usados variava | `E_escalabilidade/passada_1_descartada/` (com `LEIA-ME.txt`) | Espera pelo registro de todos os executores; passada refeita |
| A máquina suspendeu de novo (12:36 a 17:02), apesar do `systemd-inhibit` | `logs/E_execucao.log` | Acrescentado `kde-inhibit` |
| Um processo produtor ficou 1h20 bloqueado em `producer.flush()` (kafka-python 3.0.0), com 298.347 de 300.000 mensagens confirmadas; causa raiz não determinada | `logs/falha_produtor_travado_E_w1_p6_r3.log` | `flush(timeout=120)` e tempo-limite no script; execução repetida |
| Kafka caiu 3 vezes em 3 workers e 6 partições com `java.io.IOException: No space left on device` | `logs/kafka_queda_E_w3_p6_r1.log`, `_r2.log`, `_r3.log`; `E_escalabilidade/falhas.csv` | Não resolvido nesta rodada (ver "O que não foi medido") |
| A cadeia de medição foi encerrada às 19:20 por falta de memória no sistema (6,1 GiB de 7,1 GiB em uso e 3,1 GiB de swap com 3 workers) | Saída de `free -m` registrada na sessão | Workers reduzidos a 1 |

Causa do disco cheio: cada worker guarda no próprio contêiner os arquivos de cada aplicação Spark executada. O worker que ficou 7 horas ligado acumulou 2,37 GB (`docker ps --size`), e a partição raiz, onde o Docker grava, chegou a 96 % de uso.

O Kafka também caiu às 12:21, na primeira passada, igualmente com 3 workers. O log daquela queda não foi preservado, então a causa não está comprovada; o disco cheio é a hipótese provável.

## F. Recursos

`docker stats` amostrado a cada ≈2 s durante todas as execuções do item D (1 worker, 3 partições). CSV bruto: `F_recursos/docker_stats_D.csv` (14.216 linhas). Resumos: `resumo_por_taxa_somente_carga.csv` (só os 60 s de carga) e `resumo_por_taxa.csv` (carga mais dreno).

Comando do amostrador: `docker stats --no-stream --format '{{json .}}'`, chamado em laço pelo `harness.py` durante cada execução.

### CPU média durante a carga (% de um núcleo; ≈145 amostras por taxa)

| Contêiner | 100 tx/s | 500 | 1.000 | 2.000 | 5.000 |
|---|---|---|---|---|---|
| spark-worker (limite de 200 %) | 151,5 | 149,9 | 155,6 | 166,0 | 172,4 |
| spark-streaming (driver) | 59,3 | 43,4 | 41,7 | 47,4 | 35,4 |
| metrics-bridge | 16,6 | 43,9 | 87,0 | 99,1 | 91,7 |
| kafka | 15,4 | 13,5 | 13,1 | 12,0 | 30,4 |
| mlflow | 1,6 | 1,3 | 1,7 | 1,0 | 1,5 |
| spark-master | 1,2 | 1,1 | 1,2 | 1,1 | 0,8 |
| grafana | 1,1 | 1,0 | 1,0 | 1,0 | 1,0 |
| kafka-exporter | 1,1 | 0,7 | 0,7 | 0,8 | 1,0 |
| prometheus | 0,8 | 0,6 | 0,6 | 0,6 | 0,6 |

### Memória máxima durante a carga (MiB)

| Contêiner | 100 tx/s | 500 | 1.000 | 2.000 | 5.000 |
|---|---|---|---|---|---|
| spark-worker | 1.157 | 1.178 | 1.197 | 1.225 | 1.322 |
| spark-streaming (driver) | 1.073 | 1.103 | 1.109 | 1.138 | 1.098 |
| kafka | 575 | 584 | 590 | 581 | 653 |
| mlflow | 611 | 512 | 429 | 57 | 63 |
| metrics-bridge | 45 | 73 | 132 | 163 | 264 |
| grafana | 159 | 165 | 170 | 151 | 150 |
| spark-master | 149 | 166 | 176 | 119 | 149 |
| prometheus | 44 | 50 | 63 | 48 | 40 |
| kafka-exporter | 23 | 20 | 21 | 18 | 18 |

Observações:

- A ponte de métricas chega a ≈99 % de um núcleo a 2.000 tx/s, o que confirma que ela é o gargalo ponta a ponta do item D.
- O worker Spark usa ≈150 % a 172 % em todas as taxas, inclusive a 100 tx/s: o modelo paraleliza cada microlote nos 2 núcleos disponíveis, e o gatilho padrão dispara lotes continuamente.
- A memória do MLflow cai de ≈600 MiB para ≈60 MiB nos patamares de 2.000 e 5.000. Esses patamares foram medidos horas depois dos três primeiros, após a suspensão da máquina e o travamento por memória descritos no item D. A causa da queda não foi investigada; o MLflow não participa do fluxo de dados.

O item E registrou só uma amostra de `docker stats` ao final de cada execução (`E_escalabilidade/docker_stats_fim_execucao.csv`), e não uma série contínua.

## G. Recuperação

Carga de 500 tx/s por 90 s (45.000 mensagens). Aos 30 s o contêiner recebe SIGKILL e em seguida é iniciado de novo. O checkpoint do fluxo fica em volume e é preservado. Perdas e duplicatas são contadas por `tx_id` nas predições recebidas. N = 3 por cenário.

Comando: `venv/bin/python medicoes/scripts/G_recuperacao.py 500 3`

Falha injetada: `docker compose kill -s SIGKILL <serviço>` seguido de `docker compose start <serviço>`.

| Cenário | Perdidas (média) | Duplicadas (média ± dp; mín.–máx.) | Maior intervalo sem publicação (s) | Latência máxima (s) | Dreno completo |
|---|---|---|---|---|---|
| Worker (executor) | 0 | 0 ± 0 | 21,19 ± 0,28 | 30,62 ± 0,47 | 3 de 3 |
| Driver, com `restart: on-failure` | 0 | 313 ± 49 (265–363) | 33,95 ± 0,68 | 82,08 ± 1,44 | 3 de 3 |
| Driver, sem reinício automático | 32.549 ± 174 não processadas em 420 s | 160 ± 139 (0–249) | 35,04 ± 0,89 | — | 0 de 3 |

Origem: `G_recuperacao/execucoes.csv`, `execucoes_driver_sem_reinicio_automatico.csv`, `resumo.csv`, `intervalo_sem_publicacao.csv` e `brutos/*.csv.gz`.

Leitura dos resultados:

- **Queda do worker:** nenhuma mensagem perdida nem duplicada. As predições voltam a ser publicadas em ≈21 s.
- **Queda do driver com reinício automático:** nenhuma mensagem perdida. O lote que estava em andamento é reprocessado ao retomar do checkpoint, o que gera de 265 a 363 predições duplicadas (0,6 % a 0,8 % das 45.000). A garantia observada é de entrega "ao menos uma vez".
- **Queda do driver sem reinício automático:** o fluxo não retoma. As ≈32,5 mil mensagens não processadas continuam no tópico `transactions`; não foram apagadas, mas não receberam predição nos 420 s de espera.

### Falha encontrada: a primeira retomada do driver encerra a consulta

Ao ser reiniciado depois do SIGKILL, o driver reprocessa o lote pendente, publica as predições e então a consulta termina com:

```
java.lang.NullPointerException: Cannot invoke "scala.collection.IterableOps.map(scala.Function1)"
because the return value of "scala.Option.get()" is null
    at org.apache.spark.sql.kafka010.KafkaMicroBatchStream$.metrics(KafkaMicroBatchStream.scala:520)
    at org.apache.spark.sql.execution.streaming.runtime.ProgressContext.extractSourceProgress(ProgressReporter.scala:384)
```

O processo sai com código 1. Um segundo início retoma o fluxo normalmente. A exceção ocorre no código do próprio Spark 4.1.2 (relatório de progresso da fonte Kafka), e não no código do projeto. Log completo: `medicoes/logs/G_driver_falha_reinicio_diag.log`.

Por isso foi acrescentado `restart: on-failure` ao serviço `spark-streaming`: o Docker reinicia o driver após a saída com erro (`RestartCount` = 1 nas três execuções), e o fluxo se recupera sem intervenção. O custo é uma segunda subida do driver, que explica a latência máxima de ≈82 s contra ≈30 s na queda do worker.

Outras ocorrências no item G:

- A primeira tentativa falhou logo no início, porque o Kafka estava caído (item E). Log: `logs/G_execucao.log`.
- Em alguns reinícios do pipeline, a ponte de métricas entrou no grupo de consumo depois das primeiras predições do aquecimento e deixou de ler 18 de 3.000. Isso afeta só o aquecimento; o script passou a repeti-lo quando ocorre.

## H. Capturas de tela

Capturadas com o Chrome em modo sem janela (Playwright), com a pilha recebendo 300 tx/s.

Comandos:

```bash
venv/bin/python medicoes/scripts/H_registrar_mlflow.py
venv/bin/python medicoes/scripts/load_producer.py --rate 300 --duration 150 --run-id H_capturas2 &
venv/bin/python medicoes/scripts/H_capturas.py
```

| Arquivo (em `medicoes/H_capturas/`) | Conteúdo |
|---|---|
| `grafana_dashboard.png` | Painel com dados reais dos últimos 15 minutos: transações por segundo, latência, taxa de fraude, lag |
| `prometheus_targets.png` | Página `/targets` com os 5 alvos em estado UP |
| `mlflow_inicio.png` | Página inicial do MLflow 3.13.0 do compose |
| `mlflow_experimento.png` | Experimento `FraudDetectionCreditCard` com as 3 execuções registradas |
| `spark_master.png` | Interface do master Spark |

Observação sobre o painel do Grafana: a janela de 15 minutos inclui as execuções finais do item G, por isso aparecem os picos de latência (≈30 s) e de lag das quedas provocadas, seguidos do trecho estável a 300 tx/s. O título do painel de latência diz "(ms)", mas o eixo está em segundos.

### Execuções registradas no MLflow

O servidor MLflow do compose começou vazio; o histórico de treinamento anterior não foi migrado. Os três modelos já salvos em `models/` foram reavaliados no mesmo conjunto de teste, sem novo treinamento, e registrados. `models/best_model.pkl` não foi alterado.

| Modelo | ROC AUC | Precisão média (AP) | F1 | Precisão | Revocação |
|---|---|---|---|---|---|
| Regressão logística | 0,9660 | 0,7505 | 0,5133 | 0,3610 | 0,8878 |
| Árvore de decisão | 0,9272 | 0,6541 | 0,6125 | 0,4798 | 0,8469 |
| Random Forest | 0,9647 | 0,8594 | 0,8000 | 0,7664 | 0,8367 |

Origem: `H_capturas/mlflow_runs_registrados.csv`.

## I. Reprodutibilidade

Cada repetição executa `docker compose down -v`, sobe tudo do zero e cronometra a partir do início do `up`. N = 3.

Comando: `venv/bin/python medicoes/scripts/I_reprodutibilidade.py 3`

Sequência cronometrada: `docker compose down -v` e depois `docker compose up -d`.

| Etapa | Rep. 1 | Rep. 2 | Rep. 3 | Média (s) | dp (s) |
|---|---|---|---|---|---|
| `docker compose down -v` | 7,8 | 12,4 | 12,2 | 10,80 | 2,60 |
| Comando `up -d` retorna | 18,7 | 20,3 | 20,6 | 19,87 | 1,02 |
| Todos os serviços saudáveis | 30,2 | 32,2 | 32,4 | 31,60 | 1,22 |
| Todos os alvos do Prometheus UP | 32,4 | 33,7 | 33,9 | 33,33 | 0,81 |
| Consulta Spark ativa | 38,9 | 40,2 | 40,9 | 40,00 | 1,01 |
| Teste funcional concluído (1.000 mensagens) | 59,5 | 60,8 | 60,2 | 60,17 | 0,65 |

Origem: `I_reprodutibilidade/execucoes.csv`; saída do `up` em `up_rep1.log` a `up_rep3.log`.

Nas três repetições o `up` retornou código 0, todos os serviços com verificação de saúde ficaram saudáveis e o teste funcional recebeu as 1.000 predições.

Ressalvas: as imagens já estavam baixadas e construídas, então o tempo não inclui download nem `docker build`. O `kafka-exporter` não tem verificação de saúde; sua disponibilidade é confirmada pelo alvo do Prometheus.

## Registro geral de falhas

| # | Falha | Onde está o registro | Situação |
|---|---|---|---|
| 1 | Imagens `bitnami/spark` e `bitnami/mlflow` inexistentes | `logs/pull_bitnami.log` | Substituídas |
| 2 | `cp-kafka` 8.2.1 exige KRaft | Seção de correções | Compose convertido |
| 3 | Registro de progresso do Spark: `TypeError: Object of type UUID is not JSON serializable` | Seção de correções | Corrigido (uso de `p.json`) |
| 4 | Suspensão automática da máquina, duas vezes | Itens D e E | Contornada com `kde-inhibit` |
| 5 | Queda da ponte com `Invalid frame length` e estouro de memória | `logs/falha_metrics_bridge_D_t2000_r1.log` | Corrigida |
| 6 | Produtor único limitado a ≈2.606 tx/s | `D_ponta_a_ponta/falha_5000_produtor_unico.csv` | Contornada com 4 processos |
| 7 | Primeira passada do item E inválida | `E_escalabilidade/passada_1_descartada/` | Refeita |
| 8 | Produtor bloqueado em `flush()` | `logs/falha_produtor_travado_E_w1_p6_r3.log` | Contornada com tempo-limite; causa raiz desconhecida |
| 9 | Kafka sem espaço em disco com 3 workers e 6 partições | `logs/kafka_queda_E_w3_p6_r*.log` | Configuração não medida |
| 10 | Medição encerrada por falta de memória | Item E | Workers reduzidos |
| 11 | Driver Spark encerra na primeira retomada do checkpoint | `logs/G_driver_falha_reinicio_diag.log` | Contornada com `restart: on-failure` |
| 12 | Captura do MLflow e do Prometheus: tempo esgotado aguardando rede ociosa | Item H | Corrigida (espera por carregamento) |

## O que não foi medido, e por quê

1. **Escalabilidade com 3 workers e 6 partições (item E).** As três tentativas falharam porque o Kafka ficou sem espaço em disco, e a cadeia foi depois encerrada por falta de memória. A causa do disco cheio foi identificada (arquivos acumulados nos workers), mas a configuração não foi refeita: com 3 workers a máquina já operava com 6,1 GiB de 7,1 GiB em uso.
2. **Ponto de saturação do Spark sob carga contínua (item D).** O Spark acompanhou até a maior taxa testada (5.000 tx/s nominais). Taxas maiores não foram testadas porque o gerador de carga compete pelas mesmas CPUs. A capacidade foi medida de outra forma no item E (escoamento de acúmulo), que não equivale a uma taxa sustentada de regime.
3. **Variação de workers e partições no teste ponta a ponta (item D).** O item D rodou só com 1 worker e 3 partições; a variação ficou restrita ao item E.
4. **Uso de recursos em série contínua durante o item E.** Só há uma amostra de `docker stats` ao fim de cada execução.
5. **Inferência em processo único com métricas, 30 repetições (item B).** Foram feitas 5, por custarem ≈3 minutos cada.
6. **Equivalência com mais de uma amostra (item C).** Uma única comparação de 10.000 transações.
7. **Recuperação com reinício do master Spark ou do Kafka (item G).** Só foram testadas as quedas do driver e do worker.
8. **Tempo de reprodução incluindo download e construção das imagens (item I).** As imagens já estavam em cache local.
9. **Causa raiz de três falhas:** o bloqueio do produtor em `flush()`, a queda do Kafka às 12:21 na primeira passada do item E (log não preservado) e a exceção do Spark na retomada do driver (identificado o ponto no código do Spark, mas não a condição que a provoca).
10. **Comportamento em cluster real.** Todas as medições são de uma única máquina com 8 CPUs lógicas e 7,1 GiB; os números de escalabilidade refletem a divisão de núcleos dessa máquina, e não o acréscimo de máquinas.
11. **Histórico de treinamento no MLflow.** As execuções de treinamento anteriores não foram migradas para o servidor do compose; só as reavaliações dos modelos salvos.
