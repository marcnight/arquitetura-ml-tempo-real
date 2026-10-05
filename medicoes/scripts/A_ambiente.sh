#!/bin/bash
# Item A - coleta do ambiente. Uso: bash medicoes/scripts/A_ambiente.sh
cd "$(dirname "$0")/../.."
OUT=medicoes/A_ambiente; mkdir -p $OUT
lscpu > $OUT/lscpu.txt
free -h > $OUT/free.txt
uname -a > $OUT/uname.txt
docker --version > $OUT/docker_version.txt
docker compose version > $OUT/docker_compose_version.txt
venv/bin/python --version > $OUT/python_version.txt
venv/bin/pip freeze > $OUT/pip_freeze_host.txt
docker run --rm tcc-spark:4.1.2 pip freeze > $OUT/pip_freeze_imagem_spark.txt 2>&1
docker run --rm tcc-spark:4.1.2 bash -c 'python --version; java -version 2>&1' > $OUT/python_java_imagem_spark.txt 2>&1
echo "servico,imagem,id,criada" > $OUT/imagens.csv
docker compose config --format json | venv/bin/python -c '
import json,sys,subprocess
for n,s in sorted(json.load(sys.stdin)["services"].items()):
    i=s["image"]; r=subprocess.run(["docker","image","inspect",i,"--format","{{.Id}},{{.Created}}"],capture_output=True,text=True).stdout.strip()
    print(f"{n},{i},{r}")' >> $OUT/imagens.csv
cat $OUT/imagens.csv
