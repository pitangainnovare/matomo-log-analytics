import argparse
import logging
import os
import sys
sys.path.append('')

from libs import lib_database


LOGS_FILES_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/database')


def main():
    usage = """Cria tabela LogFile para armazenar dados de arquivos de log de acesso."""
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-u', '--database_uri',
        default=LOGS_FILES_DATABASE_STRING,
        dest='database_uri',
        help='String no formato mysql://username:password@host1:port/database'
    )

    parser.add_argument(
        '--logging_level',
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'],
        dest='logging_level',
        default='INFO'
    )

    params = parser.parse_args()

    logging.basicConfig(level=params.logging_level)

    logging.info('Criando tabelas LogFile e LogFileSummary')
    lib_database.create_tables(params.database_uri)


if __name__ == '__main__':
    main()
