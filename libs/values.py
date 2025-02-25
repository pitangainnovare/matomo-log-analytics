# coding=utf-8

# Logs SciELO Brasil (site clássico)
FILE_APACHE_NAME = 'apache'
FILE_NODE03_NAME = 'node03'
FILE_HIPERION_NAME = 'hiperion'
FILE_HIPERION_APACHE_NAME = 'hiperion-apache'
FILE_HIPERION_VARNISH_NAME = 'hiperion-varnish'
FILE_VARNISH_NAME = 'varnish'
FILE_SCL_2 = 'scielo.nbr.2.'
FILE_SCL_4 = 'scielo.nbr.4.'

# Logs SciELO Brasil (site novo)
FILE_SCL_2_NAME = 'scl2'
FILE_SCL_4_NAME = 'scl4'
FILE_NEW_BR_VARNISH02_NAME = 'newbrvarnish02'
FILE_NEW_BR_VARNISH03_NAME = 'newbrvarnish03'
FILE_NEW_BR_VARNISH05_NAME = 'newbrvarnish05'
FILE_NEW_BR_VARNISH06_NAME = 'newbrvarnish06'
FILE_NEW_BR_NAME_3 = 'new-br3'
FILE_NEW_BR_NAME_4 = 'new-br4'
FILE_NEW_BR_NAME_5 = 'new-br5'
FILE_NEW_BR_NAME_6 = 'new-br6'

# Logs SciELO Preprints
FILE_PREPRINTS_NAME = 'preprints'

# Logs SciELO Dataverse
FILE_DATAVERSE_NAME = 'dataverse'
FILE_DATAVERSE_DOT_NAME = 'data.scielo'
FILE_DATAVERSE_NAME_1 = 'data1'
FILE_DATAVERSE_NAME_2 = 'data2'

# Logs SciELO Venezuela
FILE_VENEZUELA_APACHE_NAME = 'apache'
FILE_VENEZUELA_HA_NAME = 'logs-ha'
FILE_VENEZUELA_CENTOS01_NAME = 'centos-2gb-nyc3-01'
FILE_VENEZUELA_CENTOS02_NAME = 'centos-2gb-nyc3-02'
FILE_VENEZUELA_CENTOS02_ORG_VE_NAME = 'scielo-org-ve'
FILE_VENEZUELA_CENTOS02_VARNISH_NAME = 'varnish-aws'
FILE_VENEZUELA_GENERIC_NAME_1 = 've-scielo-org-access'
FILE_VENEZUELA_GENERIC_NAME_2 = 've-scielo-org'
REGEX_VENEZUELA_ENDS_WITH_DATE = r'scielo-org-ve.log-\d{4}-\d{2}-\d{2}\.gz'
REGEX_VENEZUELA_ENDS_WITH_DATE_NO_HIPHEN = r'scielo-org-ve.log-\d{4}\d{2}\d{2}\.gz'
REGEX_VENEZUELA_STARTS_WITH_DATE = r'^\d{4}-\d{2}-\d{2}.*ve-scielo-org.*\.log\.gz'
FILE_VENEZUELA_NAME_1 = 'ven1'
FILE_VENEZUELA_NAME_2 = 'ven2'
FILE_VENEZUELA_NAME_3 = 'ven3'
FILE_VENEZUELA_NAME_4 = 'ven4'
FILE_VENEZUELA_NAME_5 = 'ven5'
FILE_VENEZUELA_NAME_6 = 'ven6'
FILE_VENEZUELA_NAME_7 = 'ven7'

# Logs SciELO de outras coleções
PARTIAL_DIR_TO_SERVER = {
    'scielo.cu': ('cub', ''),
}

PARTIAL_FILE_NAME_TO_SERVER = {
    'scielo.ar.': ('arg', ''),
    'scielo.bo.': ('bol', ''),
    'scielo.cl.': ('chl', ''),
    'scielo.co.': ('col', ''),
    'scielo.cr.': ('cri', ''),
    'scielo.ec.': ('ecu', ''),
    '01_scielo.es.': ('esp', '1'),
    '02_scielo.es.': ('esp', '2'),
    'scielo.mx.': ('mex', ''),
    'scielo.pt.': ('prt', ''),
    'scielo.py.': ('pry', ''),
    'scielo.za.': ('sza', ''),
    'scielo.uy.': ('ury', ''),
    'caribbean.scielo.org.1.': ('wid', '1'),
    'caribbean.scielo.org.2.': ('wid', '2'),
    'scielo.pepsic.': ('psi', ''),
    'scielo.revenf.': ('rve', ''),
    'scielo.sp.1.': ('ssp', '1'),
    'scielo.sp.2.': ('ssp', '2'),
    'scielo.ss.': ('sss', ''),
    'scielo.nbr.2.': ('nbr', '2'),
    'scielo.nbr.4.': ('nbr', '4'),
}

# Logs de coleção não detectada
FILE_INFO_UNDEFINED = ''

# Servidores invalidados
INVALID_SERVERS = {
    'ven1',
    'ven2',
    'ven3',
    'ven4',
    'ven5',
    'ven6'
}
