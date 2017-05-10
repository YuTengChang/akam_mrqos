#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 11 15:32:42 2014

@author: ychang
"""
# %% MODULE IMPORT
#==============================================================================
# this is importation of the modules
#==============================================================================
import pygal
import os, sys
import subprocess as sp
import numpy
import pandas as pd
import shutil
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config

# %% DATA PRE-PROCESS
#==============================================================================
# this is the proprocessing for the MRQOS data
#==============================================================================
root = os.path.join(config.local_mrqos_data,
                    'mrqos_summary_statistics')
source_path = os.path.join(root, 'summarized_table.csv')
dest_path = os.path.join(root, 'processed.csv')
cmd = "cat %s | sed s:NULL:0:g | sed 's/\t/,/g' | awk -F',' '{x=length($4); if(x==2){print $0;}}' | awk -F',' '{if($3>0){print $0;}}' > %s" % (source_path,
                                                                                                                                               dest_path)
sp.check_call(cmd, shell=True)


# the mixed-data-type
data = numpy.genfromtxt(dest_path, delimiter=',', dtype='str')
headers = ['startdate','enddate','maprule','geoname','netname',
           'sp99_t95','sp99_t90','sp99_t85','sp99_t75','sp99_t50',
           'sp95_t95','sp95_t90','sp95_t85','sp95_t75','sp95_t50',
           'sp90_t95','sp90_t90','sp90_t85','sp90_t75','sp90_t50',
           'sp75_t95','sp75_t90','sp75_t85','sp75_t75','sp75_t50',
           'star','sp99d_area','sp95d_area','sp90d_area','sp75d_area',
           'sp99d_max','sp95d_max','sp90d_max','sp75d_max','sp99d_freq',
           'sp95d_freq','sp90d_freq','sp75d_freq','dp99_t95','dp99_t90',
           'dp99_t85','dp99_t75','dp99_t50','dp95_t95','dp95_t90',
           'dp95_t85','dp95_t75','dp95_t50','dp90_t95','dp90_t90',
           'dp90_t85','dp90_t75','dp90_t50','dp75_t95','dp75_t90',
           'dp75_t85','dp75_t75','dp75_t50','dtar','dp99d_area',
           'dp95d_area','dp90d_area','dp75d_area','dp99d_max','dp95d_max'
           ,'dp90d_max','dp75d_max','dp99d_freq','dp95d_freq','dp90d_freq',
           'dp75d_freq','icy_t95','icy_t90','icy_t85','icy_t75','icy_t50',
           'icy_tar','icyd_area','icyd_max','icyd_freq','ict_t95','ict_t90',
           'ict_t85','ict_t75','ict_t50','ict_tar','ictd_area','ictd_max',
           'ictd_freq','total_mbps','ping_mbps','p2t_bps_pct_min',
           'sp99_cd','sp99_cw','sp95_cd','sp95_cw','sp90_cd','sp90_cw',
           'sp75_cd','sp75_cw','dp99_cd','dp99_cw','dp95_cd','dp95_cw',
           'dp90_cd','dp90_cw','dp75_cd','dp75_cw','icy_cd','icy_cw',
           'ict_cd','ict_cw','zero_ping_count','zp_ratio','n_count',
           'avg_sp99d','avg_sp95d','avg_sp90d','avg_sp75d','avg_dp99d',
           'avg_dp95d','avg_dp90d','avg_dp75d','avg_icyd','avg_ictd']

dff = pd.DataFrame(data, columns=headers)
df = dff.convert_objects(convert_numeric=True)
DateStart = df.iloc[0, 0]
DateEnd = df.iloc[0, 1]
DatePeriod = DateStart+" ~ "+DateEnd

# if data exist, then copy to /var/www/txt/ folder
shutil.copy2(dest_path, os.path.join(config.front_end_txt,
                                     'processed.csv'))
# reformatting the file to comma-separated files
file_list = ['processed_2wjoin.csv',
             'processed_2wjoin_full.csv',
             'processed_2wjoin_full_wloads.csv',
             'processed_3djoin_full_wloads.csv',
             'processed_2wjoin_full_wloads_wio.csv',
             'processed_3djoin_full_wloads_wio.csv',
             ]
for file_id in file_list:
    source_path = os.path.join(root, file_id)
    dest_path = os.path.join(config.front_end_txt, file_id)
    cmd = "cat {} | tail -n+2 | sed 's/\t/,/g' > {}".format(source_path, dest_path)
    sp.check_call(cmd, shell=True)
# cmd = "cat /u4/ychang/Projects/18-MRQOS/Data/processed_2wjoin.csv | tail -n+2 | sed 's/\t/,/g' > /var/www/txt/processed_2wjoin.csv"
# sp.check_call( cmd, shell=True )
# cmd = "cat /u4/ychang/Projects/18-MRQOS/Data/processed_2wjoin_full.csv | tail -n+2 | sed 's/\t/,/g' > /var/www/txt/processed_2wjoin_full.csv"
# sp.check_call( cmd, shell=True )
# cmd = "cat /u4/ychang/Projects/18-MRQOS/Data/processed_2wjoin_full_wloads.csv | tail -n+2 | sed 's/\t/,/g' > /var/www/txt/processed_2wjoin_full_wloads.csv"
# sp.check_call( cmd, shell=True )
# cmd = "cat /u4/ychang/Projects/18-MRQOS/Data/processed_3djoin_full_wloads.csv | tail -n+2 | sed 's/\t/,/g' > /var/www/txt/processed_3djoin_full_wloads.csv"
# sp.check_call( cmd, shell=True )
# cmd = "cat /u4/ychang/Projects/18-MRQOS/Data/processed_2wjoin_full_wloads_wio.csv | tail -n+2 | sed 's/\t/,/g' > /var/www/txt/processed_2wjoin_full_wloads_wio.csv"
# sp.check_call( cmd, shell=True )
# cmd = "cat /u4/ychang/Projects/18-MRQOS/Data/processed_3djoin_full_wloads_wio.csv | tail -n+2 | sed 's/\t/,/g' > /var/www/txt/processed_3djoin_full_wloads_wio.csv"
# sp.check_call( cmd, shell=True )

# run the report generating script
cmd = "/opt/anaconda/bin/python {}".format(os.path.join(config.local_mrqos_root,
                                                        'local_python',
                                                        'local_generate_Fred_reports.py'))
sp.check_call(cmd, shell=True)

# %% GEO and MR LIST DEFINITION
#==============================================================================
# this is definition
#==============================================================================
geo_list = numpy.unique(dff.geoname)
maprule_list = [1, 3, 121, 122, 153, 197, 198, 290, 332, 382,
                388, 436, 2903, 2905, 2911, 2916, 2923, 4114, 4992]


# %% E2-Figures
#==============================================================================
# Compute E2-Figures
#==============================================================================
## this is the procedure to create E2-figures: Geo-Map-Net XY plot for 3 different types to indicate percentiles:
## score
## distance
## in_continent x in_country
##
filedir = '/var/www/Figures/E2/';
#print ct+"::"+nt+"::MR_"+str(mrid)
plot_index = [['sp99_t95', 'sp99_t90', 'sp99_t85', 'sp99_t75', 'sp99_t50',
               'sp95_t95', 'sp95_t90', 'sp95_t85', 'sp95_t75', 'sp95_t50',
               'sp90_t95', 'sp90_t90', 'sp90_t85', 'sp90_t75', 'sp90_t50',
               'sp75_t95', 'sp75_t90', 'sp75_t85', 'sp75_t75', 'sp75_t50', 'star'],
              ['dp99_t95', 'dp99_t90', 'dp99_t85', 'dp99_t75', 'dp99_t50',
               'dp95_t95', 'dp95_t90', 'dp95_t85', 'dp95_t75', 'dp95_t50',
               'dp90_t95', 'dp90_t90', 'dp90_t85', 'dp90_t75', 'dp90_t50',
               'dp75_t95', 'dp75_t90', 'dp75_t85', 'dp75_t75', 'dp75_t50', 'dtar'],
              ['icy_t95', 'icy_t90', 'icy_t85', 'icy_t75', 'icy_t50', 'icy_tar',
               'ict_t95', 'ict_t90', 'ict_t85', 'ict_t75', 'ict_t50', 'ict_tar']];
plot_type = ['Score','Distance','Other']
x_pct = numpy.array([0.95, 0.90, 0.85, 0.75, 0.5])

for ct in geo_list:
    for mrid in maprule_list:
        df2 = df[(df['geoname']==ct)&(df['maprule']==mrid)]
        ntlist = numpy.unique(df2.netname);
        for nt in ntlist:
            for type_scan in range(len(plot_type)):
                ptype = plot_type[type_scan];
                Q_set = plot_index[type_scan];
                XY_chart = pygal.XY(transparency=1, dots_size=5)
                XY_chart.title = ptype+"::"+ct+"::"+nt+"::MR_"+str(mrid);
                str_for_name = ptype+"_"+ct+"_"+nt+"_"+str(mrid)+'.svg';
                tt = numpy.array(df2[(df2['netname']==nt)].loc[:,Q_set])[0,:];
                if type_scan<2: # distance and score plot
                    XY_chart.add('target', [(0.5,tt[20]),(0.95,tt[20])])
                    XY_chart.add('p99', [(x_pct[i], tt[i]) for i in range(0,5)])
                    XY_chart.add('p95', [(x_pct[i], tt[5+i]) for i in range(0,5)])
                    XY_chart.add('p90', [(x_pct[i], tt[10+i]) for i in range(0,5)])
                    XY_chart.add('p75', [(x_pct[i], tt[15+i]) for i in range(0,5)])
                else: # other plot
                    XY_chart.add('OCY_target', [(0.5,100-tt[5]),(0.95,100-tt[5])])
                    XY_chart.add('OCT_target', [(0.5,100-tt[11]),(0.95,100-tt[11])])
                    XY_chart.add('out cy', [(x_pct[i], 100-tt[i]) for i in range(0,5)])
                    XY_chart.add('out ct', [(x_pct[i], 100-tt[6+i]) for i in range(0,5)])
                XY_chart.render_to_file(filedir+str_for_name);

print "finished E2 Figures!"



# %% E-Figures
#==============================================================================
# Compute E-Figures
#==============================================================================
## this is the procedure to create E-figures: Geo-Map-Network polar maps for 5 measurements:
## score
## distance
## in_continent
## in_country
## io_ratio
## ping-based_to_all traffic
filedir = '/var/www/Figures/E/';
xfiledir = '/Figures/E2/';
ploygon_hover_index = ['Score','Distance','Out of Country','Out of Continent','non-P2T']

#print ct+"::"+nt+"::MR_"+str(mrid)
for ct in geo_list:
    for mrid in maprule_list:
        df2 = df[(df['geoname']==ct)&(df['maprule']==mrid)];
        ntlist = numpy.unique(df2.netname);
        for nt in ntlist:
            #print ct+"::"+nt+"::MR_"+str(mrid)
            radar_chart = pygal.Radar(dots_size=5, legend_font_size=15, x_label_font_size=15)
            radar_chart.title = ct+"::"+nt+"::MR_"+str(mrid);
            str_for_name = ct+"_"+nt+"_"+str(mrid)+'.svg';
            tt = numpy.array(df2[(df2['netname']==nt)].loc[:,['sp95_t95', 'sp95_t50', 'star',
                                                              'dp95_t95', 'dp95_t50', 'dtar',
                                                              'icy_t95', 'icy_t50', 'icy_tar',
                                                              'ict_t95', 'ict_t50', 'ict_tar',
                                                              'total_mbps','ping_mbps','p2t_bps_pct_min'
                                                              ]])[0,:];


            stats = numpy.reshape(numpy.concatenate((numpy.round(tt[0:2]/tt[2], decimals=2),
                                                     numpy.round(tt[3:5]/tt[5], decimals=2),
                                                     numpy.round((100-tt[6:8])/(100-tt[8]), decimals=2),
                                                     numpy.round((100-tt[9:11])/(100-tt[11]), decimals=2),
                                                     numpy.round(numpy.array([(1-(tt[13]+0.001)/(tt[12]+0.001))/0.05,
                                                                              tt[14]/9500]), decimals=2)
                                                    ), axis=1),[5,2]).T

            stats = numpy.minimum(10,stats)
            #print stats;
            temp_targets = numpy.array([str(tt[2]),str(tt[5]),str(100-tt[8]),str(100-tt[11]),'5'])
            temp_truevalue = numpy.array([str(tt[0]),
                                          str(tt[3]),
                                          str(100-tt[6]),
                                          str(100-tt[9]),
                                          str(numpy.round(100*(1-(tt[13]+0.001)/(tt[12]+0.001)), decimals=2))
                                         ])
            radar_chart.x_labels = ['Score('+str(tt[2])+')',
                                    'Distance('+str(tt[5])+')',
                                    'Out Country('+str(int(100-tt[8]))+'%)',
                                    'Out Continent('+str(int(100-tt[11]))+'%)',
                                    'non-P2T(5%)'];

            xlinks_child = [xfiledir+"Score_"+str_for_name,
                            xfiledir+"Distance_"+str_for_name,
                            xfiledir+"Other_"+str_for_name,
                            xfiledir+"Other_"+str_for_name,
                            xfiledir+"Score_"+str_for_name]

            radar_chart.add('target', [1,1,1,1,1])
            radar_chart.add('worst 5%', [{'value': stats[0,i],
                                          'label': ploygon_hover_index[i] + ': metric/target = ' + temp_truevalue[i]+'/'+temp_targets[i],
                                     'xlink': {'href': xlinks_child[i], 'target': '_self'}} for i in range(0,5)])

            radar_chart.x_labels_major = [0.5, 1.0, 1.5, 2.0];
            radar_chart.render_to_file(filedir+str_for_name);

print "finished E Figures!"


# %% J-Figures
#==============================================================================
# Compute J-Figures
#==============================================================================
## this is the procedure to create J-figures: Geo x Map bar plots with x-axis=network for 4 measurements,
## each of which is **** for a specific GEO and MAP ****:

## showing the average deficiency:
##'avg_sp99d', 'avg_sp95d', 'avg_sp90d', 'avg_sp75d',
##'avg_dp99d', 'avg_dp95d', 'avg_dp90d', 'avg_dp75d',
##'avg_icyd', 'avg_ictd'
## and overall_deficiency:
## avg_sp95d + avg_dp95d + avg_icyd + avg_ictd

xfiledir = '/Figures/E/';  # load from
xfiledir2 = '/Figures/E2/';  # load from
filedir = '/var/www/Figures/J/';   # save to
Q_set = ['avg_sp95d','avg_dp95d','avg_icyd','avg_ictd'];
T_set = ['star', 'dtar', 'icy_tar', 'ict_tar']
Legend = ['Score','Distance','Out of Country','Out of Continent'];
Types = ['Score','Distance','Other','Other'];

for ct in geo_list:
    for mrid in maprule_list:
        df2 = df[(df['geoname']==ct)&(df['maprule']==mrid)];
        nslist = numpy.unique(df2.netname);
        if len(nslist) == 0:
            # if the combination of GEO and MAP is missing
            break;
        data = [];
        lowerend = -0.1
        chart = pygal.Bar(x_label_rotation = 20, zero = lowerend);
        chart.x_labels = map(str,nslist);
        str_for_name = ct+"_"+str(mrid)+'.svg';
        count = 0

        for nt in nslist:
            ss = numpy.array(df2[df2.netname==nt].loc[:,Q_set])[0,:];
            if len(ss) == 0:
                ss = numpy.zero(4)
            if count == 0:
                tt = numpy.append(ss, sum(ss));
                target_title = numpy.array(df2[df2.netname==nt].loc[:,T_set])[0,:];
            else:
                tt = numpy.vstack((tt, numpy.append(ss, sum(ss))));
            count += 1;
        # now data has the format: deficiency_type x network
        data = tt.T

        chart.title = 'Deficiency: '+ct+' MR '+str(mrid)+'\ntarget=['+str(target_title[0])+', '+str(target_title[1])+', ' +str(100-target_title[2])+', ' +str(100-target_title[3])+'] ' ;

        if count > 1:
            # Deficiency First 4 Types:
            for type_scan in range(len(Q_set)):
                chart.add(Legend[type_scan], [{'value': data[type_scan, i],
                                              'xlink': {'href': xfiledir2+Types[type_scan]+"_"+ct+"_"+nslist[i]+"_"+str(mrid)+'.svg',
                                                        'target': '_self'}}
                                             for i in range(count)])
            # Deficiency Overall:
            chart.add('Sum(Deficiency)',[{'value': data[4,i],
                                  'xlink': {'href': xfiledir+ct+"_"+nslist[i]+"_"+str(mrid)+'.svg',
                                            'target': '_self'}} for i in range(count)])
        # there is only 'ANY' case:
        else:
            # Deficiency First 4 Types:
            for type_scan in range(len(Q_set)):
                chart.add(Legend[type_scan], [{'value': data[type_scan],
                                              'xlink': {'href': xfiledir2+Types[type_scan]+"_"+ct+"_"+nslist[0]+"_"+str(mrid)+'.svg',
                                                        'target': '_self'}}
                                            ])
            # Deficiency Overall:
            chart.add('Sum(Deficiency)',[{'value': data[4],
                                  'xlink': {'href': xfiledir+ct+"_"+nslist[0]+"_"+str(mrid)+'.svg',
                                            'target': '_self'}}
                                ])
        # save the file
        chart.render_to_file(filedir+str_for_name);

print "finished J Figures!" # < 15 sec


# %% A2-Figures (WorldMap)
#==============================================================================
# compute A2-Figures
#==============================================================================
## this is the procedure to create A-figures: Geo WorldMap for overall (cumulative) flags
## this is the procedure to create B-figures: Geo StackedBar Figure for each (individual) flags
xfiledir = '/Figures/J/';  # load from
filedir = '/var/www/Figures/A2/';   # save to
filedir2 = '/var/www/Figures/A4/';   # save to
Q_set = ['avg_sp95d','avg_dp95d','avg_icyd','avg_ictd','star', 'dtar', 'icy_tar', 'ict_tar'];
F_set = ['Score','Distance','OutGeo'];
Tscale = 100/numpy.log(2)

geo_list = numpy.unique(df.geoname);

geo_available = ['ad','ae','af','al','am','ao','aq','ar','at','au','az','ba','bd','be','bf',
                 'bg','bh','bi','bj','bn','bo','br','bt','bw','by','bz','ca','cd','cf','cg',
                 'ch','ci','cl','cm','cn','co','cr','cu','cv','cy','cz','de','dj','dk','do',
                 'dz','ec','ee','eg','eh','er','es','et','fi','fr','ga','gb','ge','gf','gh',
                 'gl','gm','gn','gq','gr','gt','gu','gw','gy','hk','hn','hr','ht','hu','id',
                 'ie','il','in','iq','ir','is','it','jm','jo','jp','ke','kg','kh','kp','kr',
                 'kw','kz','la','lb','li','lk','lr','ls','lt','lu','lv','ly','ma','mc','md',
                 'me','mg','ml','mm','mn','mo','mr','mt','mu','mv','mw','mx','my','mz',
                 'na','ne','ng','ni','nl','no','np','nz','om','pa','pe','pg','ph','pk','pl',
                 'pr','ps','pt','py','re','ro','rs','ru','rw','sa','sc','sd','se','sg','sh',
                 'si','sk','sl','sm','sn','so','sr','st','sv','sy','sz','td','tg','th','tj',
                 'tl','tm','tn','tr','tw','tz','ua','ug','us','uy','uz','va','ve','vn','ye',
                 'yt','za','zm','zw'];

# country that "in_continent" is more important than "in_country"
geo_go_by_continent = ['ad','al','am','az','at','ba','be','bg','by','ch',
                       'cy','cz','dk','ee','es','fi',
                       'ge','gr','hr','hu','ie','it','li','lt','lu',
                       'lv','mc','md','me','mt','nl','no','pl',
                       'pt','ro','rs','ru','se','si','sk','sm','ua']

# change to upper case to be comparable
for item in range(len(geo_available)):
    geo_available[item] = geo_available[item].upper();

# give the freedom to customize the country set we look at
geo_custom = geo_list;

geo_intersect = list(set(geo_available) & set(geo_list));
geo_intersect = list(set(geo_intersect) & set(geo_custom));
geo_intersect.sort();
#getting maximum violation with a upper limit 1000 (?)
#max_vio = min(float(max(numpy.array(df2.max())[1::])), 1000)
max_vio = 500

for mrid in maprule_list:
    df2 = df[(df['maprule']==mrid)&(df.netname=='ANY')].loc[:,numpy.append(['geoname'],Q_set)];
    geo_data = list(set(geo_intersect) & set(numpy.unique(df2.geoname)))

    # add 3 colors of "badness"
    color_set = "'#FF0000','#00FF00','#0000FF',"
    # add per geo/country colors
    for ct in geo_data:
        temp = numpy.array(df2[df2.geoname == ct])[0,1::]
        # replace 'in_country' by 'in_continent' if in the list 'geo_go_by_continent'
        if ct.lower() in geo_go_by_continent:
            temp[2] = temp[3];
            temp[6] = temp[7];

        hex_colors = '#%0.2x' % (255 * (1-numpy.exp(-1*temp[0]/Tscale))) + '%0.2x' % (255 * (1-numpy.exp(-1*temp[1]/Tscale))) + '%0.2x' % (255 * (1-numpy.exp(-1*temp[2]/Tscale)))
        color_set = color_set + "'%s'," % hex_colors

    cmd_str = "custom_style = Style( colors=(%s) )" % color_set[0:len(color_set)-1]
    from pygal.style import Style
    exec(cmd_str)
    worldmap = pygal.Worldmap(show_legend=False, style=custom_style);
    worldmap.title = 'Maprule '+str(mrid)

    # measurement type temp string:
    Score_str = '';
    Distance_str = '';
    OutGeo_str = '';
    # add the categorical data for later plots
    for ct in geo_data:
        temp = numpy.array(df2[df2.geoname == ct])[0,1::]
        # replace 'in_country' by 'in_continent' if in the list 'geo_go_by_continent'
        if ct.lower() in geo_go_by_continent:
            temp[2] = temp[3]
            temp[6] = temp[7]
        Score_str = Score_str + "{'value': ('" + ct.lower() + "', " + str(temp[0]) + ")},";
        Distance_str = Distance_str + "{'value': ('" + ct.lower() + "', " + str(temp[1]) + ")},";
        OutGeo_str = OutGeo_str + "{'value': ('" + ct.lower() + "', " + str(temp[2]) + ")},";

    # plot the categorical data
    for item in range(len(F_set)):
        exec('str_tmp='+F_set[item]+'_str;')
        str_tmp = "worldmap.add('"+F_set[item]+"', ["+str_tmp[:len(str_tmp)-1]+"])";
        exec(str_tmp);

    # plot per country(geo) data
    for ct in geo_data:
        temp = numpy.array(df2[df2.geoname == ct])[0,1::]
        if ct.lower() in geo_go_by_continent:
            target_str = 'target=['+str(temp[4])+','+str(temp[5])+','+str(100-temp[7])+']'
        else:
            target_str = 'target=['+str(temp[4])+','+str(temp[5])+','+str(100-temp[6])+']'

        labeling = 'score(%s)' % temp[0] + '\ndistance(%s)' % temp[1] + '\ngeo(%s)' % temp[2]
        #print labeling
        worldmap.add(target_str,[{'value':(ct.lower(),1),
                                  'label': labeling,
                                  'xlink':{'href': xfiledir+ct+'_'+str(mrid)+'.svg',
                                           'target': '_self'}
                         }])
    # save to the file
    str_file_name = 'worldmap_'+str(mrid)+'.svg'
    worldmap.render_to_file(filedir+str_file_name)

    worldmap.title = 'Maprule ' + str(mrid) + ': ' + DatePeriod
    str_file_name2 = 'worldmap_' + str(mrid) + '_' + DateEnd[0:4] + DateEnd[5:7] + DateEnd[8:10]+'.svg'
    worldmap.render_to_file(filedir2+str_file_name2)

print "finished A2 & A4 Figures!" # < 15 sec


# %% A3-Figures (WorldMap)
#==============================================================================
# compute A3-Figures
#==============================================================================
## this is the procedure to create A-figures: Geo WorldMap for overall (cumulative) flags
## this is the procedure to create B-figures: Geo StackedBar Figure for each (individual) flags
xfiledir = '/Figures/J/';  # load from
filedir = '/var/www/Figures/A3/';   # save to
F_set = ['Score','Distance','OutGeo'];

geo_list = numpy.unique(df.geoname);

geo_dictionary = {'ad': 'Andorra',
'ae': 'United Arab Emirates',
'af': 'Afghanistan',
'al': 'Albania',
'am': 'Armenia',
'ao': 'Angola',
'aq': 'Antarctica',
'ar': 'Argentina',
'at': 'Austria',
'au': 'Australia',
'az': 'Azerbaijan',
'ba': 'Bosnia and Herzegovina',
'bd': 'Bangladesh',
'be': 'Belgium',
'bf': 'Burkina Faso',
'bg': 'Bulgaria',
'bh': 'Bahrain',
'bi': 'Burundi',
'bj': 'Benin',
'bn': 'Brunei Darussalam',
'bo': 'Bolivia, Plurinational State of',
'br': 'Brazil',
'bt': 'Bhutan',
'bw': 'Botswana',
'by': 'Belarus',
'bz': 'Belize',
'ca': 'Canada',
'cd': 'Congo, the Democratic Republic of the',
'cf': 'Central African Republic',
'cg': 'Congo',
'ch': 'Switzerland',
'cl': 'Chile',
'cm': 'Cameroon',
'cn': 'China',
'co': 'Colombia',
'cr': 'Costa Rica',
'cu': 'Cuba',
'cv': 'Cape Verde',
'cy': 'Cyprus',
'cz': 'Czech Republic',
'de': 'Germany',
'dj': 'Djibouti',
'dk': 'Denmark',
'do': 'Dominican Republic',
'dz': 'Algeria',
'ec': 'Ecuador',
'ee': 'Estonia',
'eg': 'Egypt',
'eh': 'Western Sahara',
'er': 'Eritrea',
'es': 'Spain',
'et': 'Ethiopia',
'fi': 'Finland',
'fr': 'France',
'ga': 'Gabon',
'gb': 'United Kingdom',
'ge': 'Georgia',
'gf': 'French Guiana',
'gh': 'Ghana',
'gl': 'Greenland',
'gm': 'Gambia',
'gn': 'Guinea',
'gq': 'Equatorial Guinea',
'gr': 'Greece',
'gt': 'Guatemala',
'gu': 'Guam',
'gw': 'Guinea-Bissau',
'gy': 'Guyana',
'hk': 'Hong Kong',
'hn': 'Honduras',
'hr': 'Croatia',
'ht': 'Haiti',
'hu': 'Hungary',
'id': 'Indonesia',
'ie': 'Ireland',
'il': 'Israel',
'in': 'India',
'iq': 'Iraq',
'ir': 'Iran',
'is': 'Iceland',
'it': 'Italy',
'jm': 'Jamaica',
'jo': 'Jordan',
'jp': 'Japan',
'ke': 'Kenya',
'kg': 'Kyrgyzstan',
'kh': 'Cambodia',
'kr': 'South Korea',
'kw': 'Kuwait',
'kz': 'Kazakhstan',
'lb': 'Lebanon',
'li': 'Liechtenstein',
'lk': 'Sri Lanka',
'lr': 'Liberia',
'ls': 'Lesotho',
'lt': 'Lithuania',
'lu': 'Luxembourg',
'lv': 'Latvia',
'ly': 'Libyan Arab Jamahiriya',
'ma': 'Morocco',
'mc': 'Monaco',
'md': 'Moldova, Republic of',
'me': 'Montenegro',
'mg': 'Madagascar',
'ml': 'Mali',
'mm': 'Myanmar',
'mn': 'Mongolia',
'mo': 'Macao',
'mr': 'Mauritania',
'mt': 'Malta',
'mu': 'Mauritius',
'mv': 'Maldives',
'mw': 'Malawi',
'mx': 'Mexico',
'my': 'Malaysia',
'mz': 'Mozambique',
'na': 'Namibia',
'ne': 'Niger',
'ng': 'Nigeria',
'ni': 'Nicaragua',
'nl': 'Netherlands',
'no': 'Norway',
'np': 'Nepal',
'nz': 'New Zealand',
'om': 'Oman',
'pa': 'Panama',
'pe': 'Peru',
'pg': 'Papua New Guinea',
'ph': 'Philippines',
'pk': 'Pakistan',
'pl': 'Poland',
'pr': 'Puerto Rico',
'ps': 'Palestine, State of',
'pt': 'Portugal',
'py': 'Paraguay',
're': 'Reunion',
'ro': 'Romania',
'rs': 'Serbia',
'ru': 'Russian Federation',
'rw': 'Rwanda',
'sa': 'Saudi Arabia',
'sc': 'Seychelles',
'sd': 'Sudan',
'se': 'Sweden',
'sg': 'Singapore',
'si': 'Slovenia',
'sk': 'Slovakia',
'sl': 'Sierra Leone',
'sm': 'San Marino',
'sn': 'Senegal',
'so': 'Somalia',
'sr': 'Suriname',
'st': 'Sao Tome and Principe',
'sv': 'El Salvador',
'sy': 'Syrian Arab Republic',
'sz': 'Swaziland',
'td': 'Chad',
'tg': 'Togo',
'th': 'Thailand',
'tj': 'Tajikistan',
'tl': 'Timor-Leste',
'tm': 'Turkmenistan',
'tn': 'Tunisia',
'tr': 'Turkey',
'tw': 'Taiwan',
'tz': 'Tanzania, United Republic of',
'ua': 'Ukraine',
'ug': 'Uganda',
'us': 'United States',
'uy': 'Uruguay',
'uz': 'Uzbekistan',
'va': 'Holy See (Vatican City State)',
've': 'Venezuela, Bolivarian Republic of',
'vn': 'Viet Nam',
'ye': 'Yemen',
'yt': 'Mayotte',
'za': 'South Africa',
'zm': 'Zambia',
'zw': 'Zimbabwe'}

mr_dictionary = {'1': 'W.g+-+1',
                 '3': 'W.ms+-+3',
                 '121': 'W.da1+-+121',
                 '122': 'W.da2+-+122',
                 '153': 'S.b+-+153',
                 '197': 'W.gi3+-+197',
                 '198': 'W.di3+-+198',
                 '290': 'W.d+-+290',
                 '332': 'W.b+-+332',
                 '382': 'W.cha4+-+382',
                 '388': 'W.chc2+-+388',
                 '436': 'W.zch1+-+436',
                 '2903': 'W.w3+-+2903',
                 '2905': 'W.w5+-+2905',
                 '2911': 'W.w11+-+2911',
                 '2916': 'W.w16+-+2916',
                 '2923': 'W.w23+-+2923',
                 '4114': 'W.g4+-+4114',
                 '4992': 'S.ksd+-+4992'}

# change to upper case to be comparable
for item in range(len(geo_available)):
    geo_available[item] = geo_available[item].upper();

# give the freedom to customize the country set we look at
geo_custom = geo_list;

geo_intersect = list(set(geo_available) & set(geo_list));
geo_intersect = list(set(geo_intersect) & set(geo_custom));
geo_intersect.sort();
#getting maximum violation with a upper limit 1000 (?)
#max_vio = min(float(max(numpy.array(df2.max())[1::])), 1000)
max_vio = 500

for mrid in maprule_list:
    df2 = df[(df['maprule']==mrid)&(df.netname=='ANY')].loc[:,numpy.append(['geoname'],Q_set)];
    geo_data = list(set(geo_intersect) & set(numpy.unique(df2.geoname)))

    # add 3 colors of "badness"
    color_set = "'#FF0000','#00FF00','#0000FF',"
    # add per geo/country colors
    for ct in geo_data:
        temp = numpy.array(df2[df2.geoname == ct])[0,1::]
        # replace 'in_country' by 'in_continent' if in the list 'geo_go_by_continent'
        if ct.lower() in geo_go_by_continent:
            temp[2] = temp[3];
            temp[6] = temp[7];
        #hex_colors = '#%0.2x' % (min(int(float(temp[0])/max_vio*255), 255)) + '%0.2x' % (min(int(float(temp[1])/max_vio*255), 255)) + '%0.2x' % (min(int(float(temp[2])/max_vio*255), 255))
        hex_colors = '#%0.2x' % (255 * (1-numpy.exp(-1*temp[0]/Tscale))) + '%0.2x' % (255 * (1-numpy.exp(-1*temp[1]/Tscale))) + '%0.2x' % (255 * (1-numpy.exp(-1*temp[2]/Tscale)))
        color_set = color_set + "'%s'," % hex_colors

    cmd_str = "custom_style = Style( colors=(%s) )" % color_set[0:len(color_set)-1]
    from pygal.style import Style
    exec(cmd_str)
    worldmap = pygal.Worldmap(show_legend=False, style=custom_style);
    worldmap.title = 'Maprule '+str(mrid)

    # measurement type temp string:
    Score_str = '';
    Distance_str = '';
    OutGeo_str = '';
    # add the categorical data for later plots
    for ct in geo_data:
        temp = numpy.array(df2[df2.geoname == ct])[0,1::]
        # replace 'in_country' by 'in_continent' if in the list 'geo_go_by_continent'
        if ct.lower() in geo_go_by_continent:
            temp[2] = temp[3]
            temp[6] = temp[7]
        Score_str = Score_str + "{'value': ('" + ct.lower() + "', " + str(temp[0]) + ")},";
        Distance_str = Distance_str + "{'value': ('" + ct.lower() + "', " + str(temp[1]) + ")},";
        OutGeo_str = OutGeo_str + "{'value': ('" + ct.lower() + "', " + str(temp[2]) + ")},";

    # plot the categorical data
    for item in range(len(F_set)):
        exec('str_tmp='+F_set[item]+'_str;')
        str_tmp = "worldmap.add('"+F_set[item]+"', ["+str_tmp[:len(str_tmp)-1]+"])";
        exec(str_tmp);

    # plot per country(geo) data
    for ct in geo_data:
        xlinkptr = 'http://mapnocc.akamai.com/home/pages/mapruleQoS.php?width=1024&height=768&start=&layout=&ylowlimit=&ylimit=&test1=&test2=&test3=&test11=&test12=&test13=&logarithmic=&period=2w&timezone=Eastern&pagesize=large&maps%5B%5D='+mr_dictionary[str(mrid)]+'&continents%5B%5D=all&countries%5B%5D='+geo_dictionary[ct.lower()]+'&networks%5B%5D=all&attrs%5B%5D=LOAD&attrs%5B%5D=SCORE&attrs%5B%5D=DISTANCE&attrs%5B%5D=IN-CONTINENT&attrs%5B%5D=IN-COUNTRY&attrs%5B%5D=IN-OUT-RATIO&attrs%5B%5D=COGS&pctls%5B%5D=75-th&pctls%5B%5D=90-th&pctls%5B%5D=95-th&pctls%5B%5D=99-th&submitnow=submit+now'
        temp = numpy.array(df2[df2.geoname == ct])[0,1::]
        if ct.lower() in geo_go_by_continent:
            target_str = 'target=['+str(temp[4])+','+str(temp[5])+','+str(100-temp[7])+']'
        else:
            target_str = 'target=['+str(temp[4])+','+str(temp[5])+','+str(100-temp[6])+']'

        labeling = 'score(%s)' % temp[0] + '\ndistance(%s)' % temp[1] + '\ngeo(%s)' % temp[2]
        #print labeling
        worldmap.add(target_str,[{'value':(ct.lower(),1),
                                  'label': labeling,
                                  'xlink':{'href': xlinkptr,
                                           'target': '_blank'}
                         }])
    # save to the file
    str_file_name = 'worldmap_'+str(mrid)+'.svg'
    worldmap.render_to_file(filedir+str_file_name)

print "finished A3 Figures!" # < 15 sec
