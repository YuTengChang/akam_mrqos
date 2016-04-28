#!/opt/anaconda/bin/python
"""
Created on Thu April 27 14:18:15 2016

@author: ychang
"""
import sys, os
import shutil
import time
import calendar
import numpy
import pandas as pd
import subprocess as sp
import random
from sklearn import linear_model
from sklearn.cross_validation import train_test_split
import math
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config

from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
from plotly.offline import plot
from plotly.graph_objs import Scatter
from plotly.graph_objs import *

def main():
    ts = calendar.timegm(time.gmtime())
    print "###################"
    print "# Performing the LP solution model prediction"
    print "# starting processing time is " + str(ts) + " = " + time.strftime('GMT %Y-%m-%d %H:%M:%S', time.localtime(ts))
    print "###################"
    print ">> loading the LP solutions <<"

    file_folder = '/u4/ychang/Projects/18-MRQOS/Data'
    file_name = 'lp_out_and_performance.csv'
    figure_folder = '/var/www/Figures/lp_pred'
    output_file_name = 'lp_out_and_perf'
    file_source = os.path.join(file_folder, file_name)
    data = numpy.genfromtxt(file_source, delimiter='\t', skip_header=1, dtype='str')

    headers = ["maprule", "geoname", "netname", "load", "score_target", "in_country_target", "score",
           "in_country_pct", "deficit", "datestamp", "maprule2", "geoname2", "netname2", "sp99_t95",
           "sp99_t90", "sp99_t85", "sp99_t75", "sp99_t50", "sp95_t95", "sp95_t90", "sp95_t85",
           "sp95_t75", "sp95_t50", "sp90_t95", "sp90_t90", "sp90_t85", "sp90_t75", "sp90_t50",
           "sp75_t95", "sp75_t90", "sp75_t85", "sp75_t75", "sp75_t50", "star", "sp99d_area", "sp95d_area",
           "sp90d_area", "sp75d_area", "sp99d_max", "sp95d_max", "sp90d_max", "sp75d_max", "sp99d_freq",
           "sp95d_freq", "sp90d_freq", "sp75d_freq", "dp99_t95", "dp99_t90", "dp99_t85", "dp99_t75",
           "dp99_t50", "dp95_t95", "dp95_t90", "dp95_t85", "dp95_t75", "dp95_t50", "dp90_t95", "dp90_t90",
           "dp90_t85", "dp90_t75", "dp90_t50", "dp75_t95", "dp75_t90", "dp75_t85", "dp75_t75", "dp75_t50",
           "dtar", "dp99d_area", "dp95d_area", "dp90d_area", "dp75d_area", "dp99d_max", "dp95d_max",
           "dp90d_max", "dp75d_max", "dp99d_freq", "dp95d_freq", "dp90d_freq", "dp75d_freq", "icy_t95",
           "icy_t90", "icy_t85", "icy_t75", "icy_t50", "icy_tar", "icyd_area", "icyd_max", "icyd_freq",
           "ict_t95", "ict_t90", "ict_t85", "ict_t75", "ict_t50", "ict_tar", "ictd_area", "ictd_max",
           "ictd_freq", "total_mbps", "ping_mbps", "p2t_bps_pct_min", "zero_ping_count", "zp_ratio",
           "n_count", "avg_sp99d", "avg_sp95d", "avg_sp90d", "avg_sp75d", "avg_dp99d", "avg_dp95d",
           "avg_dp90d", "avg_dp75d", "avg_icyd", "avg_ictd", "datestamp2"];

    dff = pd.DataFrame(data, columns=headers)
    df = dff.convert_objects(convert_numeric=True)
    df = df.sort(['datestamp'], ascending=1)
    uniq_datestamp = sorted(list(set(df.datestamp)))
    uniq_datestamp = '.'.join([str(x) for x in uniq_datestamp])
    output_file_name = '%s.update_%s.%s.csv' % (output_file_name,
                                                time.strftime('%Y%m%d', time.localtime(ts)),
                                                uniq_datestamp)

    geo_list = sorted(list(set(df.geoname)))
    my_test_size_ratio = 0.20
    my_repetence = 10
    load_threshold = 20

    headers_out = ['geoname', 'samples', 'test_size_ratio', 'repetence', 'load',
                   'sp95_t95', 'score_t95', 'intercept_t95', 'coeff_t95',
                   'sp95_t90', 'score_t90', 'intercept_t90', 'coeff_t90',
                   'sp95_t85', 'score_t85', 'intercept_t85', 'coeff_t85',
                   'sp95_t75', 'score_t75', 'intercept_t75', 'coeff_t75',
                   'sp95_t50', 'score_t50', 'intercept_t50', 'coeff_t50']
    df_out = pd.DataFrame(columns=headers_out)
    df_out_count = 0

    for geo in geo_list:
        print " >> now calculating geo: %s <<" % geo
        this_row = my_reg_set(df, geo, test_size_ratio=my_test_size_ratio, repetence=my_repetence, load_threshold=load_threshold, figure_folder=figure_folder)
        if this_row[0] != -1:
            df_out.loc[df_out_count] = this_row
            df_out_count += 1

    df_out.to_csv(os.path.join(file_folder, output_file_name), index=False)
    sp.check_call('cp %s %s' % (os.path.join(file_folder, output_file_name),
                                os.path.join('/var/www/txt', output_file_name)), shell=True)
    return

def my_lp_scatter_generation(df, geoname, intercept, slope, figure_path, load_threshold):
    '''
    :param df: dataframe that contains the information
    :param geoname: geoname that applies
    :param intercept: regression parameter intercept
    :param slope: regression parameter slope(corf)
    :param figure_path: figure full path for storing
    :param load_threshold: only compute the case with load > load_threshold
    :return: nan
    '''
    # prepare the dataframe for plotting
    df2 = df.loc[:,['maprule','geoname','netname','load','score','score_target','sp95_t95','sp95_t75','datestamp']]
    [min_x, max_x] = [min(df2.score), max(df2.score)]
    netname_list = list(set(df2.netname))
    this_netname = netname_list[0]
    dft = df2[df2.netname==this_netname]
    dft['text'] = ['Country: %s</br>Netname: %s</br>Maprule: %s</br>Target score: %s</br>LP score: %s</br>s95_t95: %s </br>s95_t75: %s</br>Load: %s</br>Solution Date: %s' \
                       % (geoname,
                          netname,
                          maprule,
                          score_target,
                          score,
                          sp95_t95,
                          sp95_t75,
                          load,
                          datestamp) \
                       for (geoname, netname, maprule, score_target, score, sp95_t95, sp95_t75, load, datestamp) \
                       in zip(dft.geoname,
                              dft.netname,
                              dft.maprule,
                              dft.score_target,
                              dft.score,
                              dft.sp95_t95,
                              dft.sp95_t75,
                              dft.load,
                              dft.datestamp)]

    dft.columns = [this_netname+'_'+x for x in dft.columns]
    dfa = dft.copy()

    for this_netname in netname_list[1:]:
        dft = df2[df2.netname==this_netname]
        dft['text'] = ['Country: %s</br>Netname: %s</br>Maprule: %s</br>Target score: %s</br>LP score: %s</br>s95_t95: %s </br>s95_t75: %s</br>Load: %s</br>Solution Date: %s' \
                       % (geoname,
                          netname,
                          maprule,
                          score_target,
                          score,
                          sp95_t95,
                          sp95_t75,
                          load,
                          datestamp) \
                       for (geoname, netname, maprule, score_target, score, sp95_t95, sp95_t75, load, datestamp) \
                       in zip(dft.geoname,
                              dft.netname,
                              dft.maprule,
                              dft.score_target,
                              dft.score,
                              dft.sp95_t95,
                              dft.sp95_t75,
                              dft.load,
                              dft.datestamp)]
        dft.columns = [this_netname+'_'+x for x in dft.columns]
        dfa = pd.concat([dfa, dft], axis=1)

    # now generating the figure files
    marker_size_ref = df2.load.quantile(.1)/15
    # sizeref=marker_size_ref,
    scatter_netname = [Scatter(x=dfa[netname+'_score'],
                            y=dfa[netname+'_sp95_t95'],
                            text=dfa[netname+'_text'],
                            marker=Marker(size=dfa[netname+'_load'], sizemode='area', sizeref=marker_size_ref),
                            mode='markers',
                            name=netname) for netname in netname_list]

    scatter_line = Scatter(x=[min_x, max_x],
                           y=[min_x, max_x],
                           mode='line',
                           marker=Marker(color='black'),
                           name='one-to-one')

    scatter_line2 = Scatter(x=[min_x, max_x],
                            y=[intercept+slope*min_x, intercept+slope*max_x],
                            mode='line',
                            marker=Marker(color='blue'),
                            name='regression')

    data = Data(scatter_netname+[scatter_line]+[scatter_line2])

    layout = Layout(xaxis=XAxis(title='LP predicted score'),
                    yaxis=YAxis(title='score @ 95(pop):95(temporal)'),
                    title='%s LP prediction and real data with load > %s' % (geoname, str(load_threshold)))

    fig = Figure(data=data, layout=layout)
    plot(fig, filename=figure_path)



def my_reg_set(df, geo_set, test_size_ratio=0.20, repetence=1, load_threshold=100, figure_folder='/var/www/Figures/lp_pred'):
    '''
    :param df: dataframe that contains the information
    :param geo_set: geo_set that applies
    :param test_size_ratio: the split of train and test set size from original db(df)
    :param repetence: how many times we like to repeat the training/testing and then report the average
    :param load_threshold: only takes case with load greater than the load_threshold
    :return:
    '''
    # only take particular network/ISP, meaningful target, and load > 100 Mbps case
    df_interested = df[df.geoname.isin([geo_set]) & ~df.netname.isin(['ANY']) & ~df.score_target.isin([10000])]
    df_interested = df_interested[df_interested.load > load_threshold]
    df_length = len(df_interested)
    total_load = numpy.sum(df_interested.load)
    # if there is not enough data points, skip this case.
    if df_length < 20:
        return [-1]

    #fig = plt.figure(num=None, figsize=(18, 12), dpi=80, facecolor='w', edgecolor='k')
    regression_result = []

    y_list = ['sp95_t95', 'sp95_t90', 'sp95_t85', 'sp95_t75', 'sp95_t50']
    for this_index in range(len(y_list)):
        y_index = y_list[this_index]
        X_data = df_interested[['score']].values
        y_data = df_interested[y_index].values
        score = []
        intercept = []
        coeff = []

        for rep in range(repetence):
            # random split the training and testing data
            X_train, X_test, y_train, y_test = train_test_split(X_data, y_data, test_size=test_size_ratio)

            # Create linear regression object
            regr = linear_model.LinearRegression()

            # Train the model using the training sets
            regr.fit(X_train, y_train)

            # Test on the testing set
            score = score + [float(regr.score(X_test, y_test))]

            # Extracting the model learned
            intercept = intercept + [float(regr.intercept_)]
            coeff = coeff + [float(regr.coef_)]

        regression_result = regression_result + [y_index,
                                                 round(numpy.mean(score),2),
                                                 round(numpy.mean(intercept),2),
                                                 round(numpy.mean(coeff),3)]

    regression_result = [geo_set, df_length, test_size_ratio, repetence, total_load] + regression_result
    figure_path = os.path.join(figure_folder, '%s_lp_mrqos.html' % geo_set)
    my_lp_scatter_generation(df_interested, geo_set, regression_result[7], regression_result[8], figure_path, load_threshold)
    return regression_result


if __name__ == '__main__':
    sys.exit(main())