#!/bin/bash

echo ""
echo "Script para enviar registros de acesso para Matomo"
echo "=================================================="

if [[ -z "$PYTHONHASHSEED" ]] || [[ $PYTHONHASHSEED != 0 ]]; then
	echo ""
	echo "ERRO: É preciso setar a variável de ambiente PYTHONHASHSEED com o valor 0"
	echo "Executar 'export PYTHONHASHSEED=0'"
	exit 1
fi

if [[ $# != 5 ]]; then
	echo "É preciso informar cinco parâmetros"
	echo "  1) Pasta de logs"
	echo "  2) Identificador numérico do site"
	echo "  3) Número de recorders"
	echo "  4) Token-auth do Matomo"
	echo "  5) URL do site"
	exit 1
fi

logs_dir=$1
idsite=$2
recorders=$3
token_auth=$4
url=$5

if [ ! -d $logs_dir ]; then
	echo ""
	echo "ERRO: Diretório inexistente: $logs_dir"
	echo ""
	exit 1
fi

re="^[0-9]+$"
if ! [[ $recorders =~ $re ]]; then
   echo ""
   echo "ERRO: Número de recorders não é um número: $recorders"
   echo ""
   exit 1
fi

threads=$(nproc --all)
if [[ $recorders -gt $threads ]]; then
	echo ""
	echo "WARNING: Número de recorders é maior que o de threads: $recorders > $threads"
	echo ""
fi

echo ""
echo ""
echo "* Configuração realizada"
echo "------------------------"
echo ""
echo "* Pasta de logs: $logs_dir"
echo "* Identificador de site: $idsite"
echo "* Número de recorders: $recorders"
echo "* Url de site: $url"
echo ""

current_dir=$(pwd)
for i in $(ls $logs_dir); do
	if [ ${i: -3} == '.gz' ]; then
		echo ""
		echo "* Iniciando para arquivo $i"
		echo "------------------------"
		source="$logs_dir/$i"
		target="$current_dir/$i"
		echo "Copiando $source para $target"
		cp $source $target
		echo "Descompactando $target"
		gunzip $target;
		log_file=${target::-3}
		time=$(($(date +%s%N)/1000000))
		log_file_output=${log_file}_${time}_loaded.txt
		echo "Extraindo registros do arquido $log_file"
		echo "Registrando saída do importador em $log_file_output"
		if [[ -e "import_logs.py" ]]; then
			python2 import_logs.py --url=$url --idsite=$idsite --recorders=$recorders --token-auth=$token_auth --output=$log_file_output $log_file
		else
			echo ""
			echo "ERRO: arquivo import_logs.py ausente"
			exit 1
		fi
		echo "Removendo arquivo $log_file"
		rm $log_file;
		echo ""
	fi
done
