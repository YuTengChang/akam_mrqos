import os, sys
import subprocess as sp
import shlex

#list_hive = ['bln']
string_hive = 'beeline -u jdbc:hive2:// -n "" -p "" --silent=true --outputformat=tsv2 '
list_hive = ['/a/third-party/hive/bin/beeline','-u','jdbc:hive2://','-n','""','-p','""','--silent=true']

format_tsv2 = '--outputformat=tsv2 '
format_tsv = '--outputformat=tsv '
format_csv2 = '--outputformat=csv2 '
format_csv = '--outputformat=csv '
format_dsv = '--outputformat=dsv '
format_table = '--outputformat=table '


def bln_prepare_hiveql(formatting):
    """
    return the beeline executable with possible overwritten output format
    :param formatting: the parameter used to specify the hive output
    :return: return string of executable and parameters
    """

    if (formatting == 'csv') | (formatting == 'CSV'):
        return string_hive + format_csv
    elif (formatting == 'csv2') | (formatting == 'CSV2'):
        return string_hive + format_csv2
    elif (formatting == 'tsv') | (formatting == 'TSV'):
        return string_hive + format_tsv
    elif (formatting == 'tsv2') | (formatting == 'TSV2'):
        return string_hive + format_tsv2
    elif (formatting == 'dsv') | (formatting == 'DSV'):
        return string_hive + format_dsv
    elif (formatting == 'table') | (formatting == 'TABLE'):
        return string_hive + format_table
    else:
        return string_hive


def bln_e(cmd, outformat='tsv2', database=''):
    """
    create corresponding hive -e + command, add "use $database" if specified

    :rtype: None
    :param cmd: hive command
    :param outformat: output format of hive
    :param database: hive database
    :return: no return
    """

    if database:
        cmd = 'use %s; ' + cmd
    list_used = shlex.split(bln_prepare_hiveql(outformat) + '-e "%s"' % cmd)
    print "this is the list used: "
    print list_used
    sp.check_call(list_used)


def bln_f(hive_script, outformat='tsv2'):
    """
    create corresponding hive -f + hive_script_file

    :param hive_script: the hive script being run
    :param outformat: output format of hive
    :return: no return
    """

    list_used = shlex.split(bln_prepare_hiveql(outformat) + '-f %s' % hive_script)
    sp.check_call(list_used)

# not used function (not updated yet)
def bln_e_outcall(cmd, outputfile, outformat='tsv2', database=''):
    """
    create corresponding hive -e + command, add "use $database" if
    specified. take the output from the command.

    :param cmd: hive command
    :param outputfile: the output file takes the result
    :param outformat: output format of hive
    :param database: hive database
    :return: no return
    """

    list_used = bln_prepare_hiveql(outformat) + ['-e']
    if database:
        cmd = 'use %s; ' + cmd
    list_used.append(cmd)
    file_handle = open(outputfile, 'w')
    sp.call(list_used, stdout=file_handle)
    file_handle.close()
