import os, sys
import subprocess as sp

list_hive = ['bln']
format_tsv2 = ['--outputformat=tsv2']
format_tsv = ['--outputformat=tsv']
format_csv2 = ['--outputformat=csv2']
format_csv = ['--outputformat=csv']
format_dsv = ['--outputformat=dsv']
format_table = ['--outputformat=table']


def bln_prepare_hiveql(formatting):
    if (formatting == 'csv') | (formatting == 'CSV'):
        return list_hive + format_csv
    elif (formatting == 'csv2') | (formatting == 'CSV2'):
        return list_hive + format_csv2
    elif (formatting == 'tsv') | (formatting == 'TSV'):
        return list_hive + format_tsv
    elif (formatting == 'tsv2') | (formatting == 'TSV2'):
        return list_hive + format_tsv2
    elif (formatting == 'dsv') | (formatting == 'DSV'):
        return list_hive + format_dsv
    else:
        return list_hive


def bln_e(cmd, outformat='tsv2', database=''):
    """create corresponding hive -e + command, add "use $database" if specified"""
    list_used = BL_prepare_hiveql(outformat) + ['-e']
    if database:
        cmd = 'use %s; ' + cmd
    list_used.append(cmd)
    sp.check_call(list_used)


def BL_f(hive_script, outformat='tsv2'):
    """create corresponding hive -f + hive_script_file"""
    list_used = BL_prepare_hiveql(outformat) + ['-f']
    list_used.append(hive_script)
    sp.check_call(list_used)


def BL_e_outcall(cmd, outputfile, outformat='tsv2', database=''):
    ''' create corresponding hive -e + command, add "use $database" if
    specified. take the output from the command. '''
    list_used = BL_prepare_hiveql(outformat) + ['-e']
    if database:
        cmd = 'use %s; ' + cmd
    list_used.append(cmd)
    file_handle = open(outputfile, 'w')
    sp.call(list_used, stdout=file_handle)
    file_handle.close()
