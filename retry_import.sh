#!/bin/bash

echo ""
echo "Script para reenviar registros de acesso para Matomo"
echo "===================================================="

if [[ -z "$PYTHONHASHSEED" ]] || [[ $PYTHONHASHSEED != 0 ]]; then
    echo ""
    echo "ERRO: É preciso setar a variável de ambiente PYTHONHASHSEED com o valor 0"
    echo "Executar 'export PYTHONHASHSEED=0'"
    exit 1
fi

if [[ -z "$MATOMO_IMPORTER_ROOT_DIR" ]]; then
    echo ""
    echo "ERRO: É preciso indicar na variável de ambiente MATOMO_IMPORTER_ROOT_DIR a raiz da aplicação matomo-log-analytics"
    echo "Por exemplo: executar 'export MATOMO_IMPORTER_ROOT_DIR=\$HOME/repos/matomo-log-analytics'"
    exit 1
fi

if [[ $# -lt 6 ]]; then
    echo "É preciso informar cinco parâmetros"
    echo "  1) Arquivo contendo caminho de logs a serem recarregados"
    echo "  2) Número da retentativa de carga"
    echo "  3) Identificador numérico do site"
    echo "  4) Número de recorders"
    echo "  5) Token-auth do Matomo"
    echo "  6) URL do site"
    exit 1
fi

# Arquivo que contém a lista de caminhos dos logs a serem recarregados (deve constar um caminho de log por linha)
logs_list_file=$1

# Indica o número da retentativa de carga
retry_number=$2

# Id do site
idsite=$3

# Número de threads de importação
recorders=$4

# Token do Matomo
token_auth=$5

# Url do Matomo
url=$6

# Verifica número de threads de importação
re="^[0-9]+$"
if ! [[ $recorders =~ $re ]]; then
   echo ""
   echo "ERRO: Número de recorders não é um número: $recorders"
   echo ""
   exit 1
fi

# Verifica se número de threads é razoável
threads=$(nproc --all)
if [[ $recorders -gt $threads ]]; then
    echo ""
    echo "WARNING: Número de recorders é maior que o de threads: $recorders > $threads"
    echo ""
fi

if [ -f "$logs_list_file" ]; then
    filelist=$(cat "$logs_list_file")
fi

for l in ${filelist}; do
    if [[ ! -f "$l" ]]; then
        echo "O caminho $l não existe. Verificar lista de caminhos de logs."
        exit 1
    fi
done

cd "$MATOMO_IMPORTER_ROOT_DIR" || exit

echo ""
echo ""
echo "* Configuração realizada"
echo "------------------------"
echo ""
echo "* Lista de logs:"
for fl in $filelist; do
  echo " $fl"
done
echo "* Retentativa de carga: $retry_number"
echo "* Identificador de site: $idsite"
echo "* Número de recorders: $recorders"
echo "* Url de site: $url"
echo ""

for f in ${filelist}; do
    echo "copying $f"
    cp "$f" .

    # extrai nome do arquivo a partir do caminho completo do arquivo
    log_file_gz="${f##*/}"

    echo "gunziping $log_file_gz"
    gunzip "$log_file_gz"

    # extrai nome do arquivo de log a partir do arquivo gzipped
    log_file="${log_file_gz%%.gz*}"

    # cria nome para guardar saida da carga de dados
    log_file_output="${log_file}"_retry"$retry_number"_loaded.txt

    echo "loading $log_file"
    trap '' 2
    python2 import_logs.py --url="$url" --idsite="$idsite" --recorders="$recorders" --log-format-name=ncsa_extended --token-auth="$token_auth" --output="$log_file_output" "$log_file"
    trap 2

    echo "removing $log_file"
    rm "$log_file"
done
