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

if [[ $# -lt 5 ]]; then
    echo "É preciso informar cinco parâmetros"
    echo "  1) Arquivo contendo caminho de logs a serem recarregados e respectiva linha de retomada de carga"
    echo "  2) Identificador numérico do site"
    echo "  3) Número de recorders"
    echo "  4) Token-auth do Matomo"
    echo "  5) URL do site"
    exit 1
fi

# Arquivo que contém a lista de caminhos dos logs a serem recarregados (deve constar um caminho de log por linha)
logs_list_file=$1

# Id do site
idsite=$2

# Número de threads de importação
recorders=$3

# Token do Matomo
token_auth=$4

# Url do Matomo
url=$5

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
    filelist=($(cat "$logs_list_file"))
fi

for (( index=0; index<${#filelist[@]}; index+=2 )); do
    echo "${filelist[index]}";
    if [[ ! -f "${filelist[index]}" ]]; then
        echo "O caminho ${filelist[index]} não existe. Verificar lista de caminhos de logs."
        exit 1;
    fi
done

cd "$MATOMO_IMPORTER_ROOT_DIR" || exit

echo ""
echo ""
echo "* Configuração realizada"
echo "------------------------"
echo ""
echo "* Lista de logs:"
for (( index=0; index<${#filelist[@]}; index+=2 )); do
    echo "${filelist[index]} a partir da linha ${filelist[index + 1]}";
done
echo "* Identificador de site: $idsite"
echo "* Número de recorders: $recorders"
echo "* Url de site: $url"
echo ""

for (( index=0; index<${#filelist[@]}; index+=2 )); do
    f=${filelist[index]}
    start_line=${filelist[index + 1]}

    if ! [[ $start_line =~ $re ]]; then
       echo ""
       echo "ERRO: arquivo $f, linha de início não é válida: $start_line"
       echo ""
       exit 1
    fi

    echo "copying $f"
    cp "$f" .

    # extrai nome do arquivo a partir do caminho completo do arquivo
    log_file_gz="${f##*/}"

    echo "gunziping $log_file_gz"
    gunzip "$log_file_gz"

    # extrai nome do arquivo de log a partir do arquivo gzipped
    log_file="${log_file_gz%%.gz*}"

    # obtem timestamp atual
    time=$(($(date +%s%N)/1000000))

    # cria nome para guardar saida da carga de dados
    log_file_output="${log_file}_${time}_loaded.txt"

    echo "loading $log_file"
    trap '' 2
    python2 import_logs.py --url="$url" --idsite="$idsite" --recorders="$recorders" --log-format-name=ncsa_extended --token-auth="$token_auth" --output="$log_file_output" --skip="$start_line" "$log_file"
    trap 2

    echo "removing $log_file"
    rm "$log_file"
done
