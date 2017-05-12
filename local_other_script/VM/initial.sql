.separator ","
drop table if exists compound_metric;
create table compound_metric (
    startdate date,
    enddate date,
    maprule integer,
    geoname varchar,
    vsp95 float,
    bsp95 float,
    gsp95 float,
    dsp95 float,
    star integer,
    vdp95 float,
    bdp95 float,
    gdp95 float,
    ddp95 float,
    dtar integer,
    vicy float,
    bicy float,
    gicy float,
    dicy float,
    icytar integer,
    vict float,
    bict float,
    gict float,
    dict float,
    icttar integer,
    netname varchar
);
.import /var/www/txt/compound_metric_comma.csv compound_metric

drop table if exists mrqos_sum_14d;
create table mrqos_sum_14d (
    startdate date,
    enddate date,
    maprule integer,
    geoname varchar,
    netname varchar,
    sp99_t95 integer,
    sp99_t90 integer,
    sp99_t85 integer,
    sp99_t75 integer,
    sp99_t50 integer,
    sp95_t95 integer,
    sp95_t90 integer,
    sp95_t85 integer,
    sp95_t75 integer,
    sp95_t50 integer,
    sp90_t95 integer,
    sp90_t90 integer,
    sp90_t85 integer,
    sp90_t75 integer,
    sp90_t50 integer,
    sp75_t95 integer,
    sp75_t90 integer,
    sp75_t85 integer,
    sp75_t75 integer,
    sp75_t50 integer,
    star integer,
    sp99d_area integer,
    sp95d_area integer,
    sp90d_area integer,
    sp75d_area integer,
    sp99d_max integer,
    sp95d_max integer,
    sp90d_max integer,
    sp75d_max integer,
    sp99d_freq integer,
    sp95d_freq integer,
    sp90d_freq integer,
    sp75d_freq integer,
    dp99_t95 integer,
    dp99_t90 integer,
    dp99_t85 integer,
    dp99_t75 integer,
    dp99_t50 integer,
    dp95_t95 integer,
    dp95_t90 integer,
    dp95_t85 integer,
    dp95_t75 integer,
    dp95_t50 integer,
    dp90_t95 integer,
    dp90_t90 integer,
    dp90_t85 integer,
    dp90_t75 integer,
    dp90_t50 integer,
    dp75_t95 integer,
    dp75_t90 integer,
    dp75_t85 integer,
    dp75_t75 integer,
    dp75_t50 integer,
    dtar integer,
    dp99d_area integer,
    dp95d_area integer,
    dp90d_area integer,
    dp75d_area integer,
    dp99d_max integer,
    dp95d_max integer,
    dp90d_max integer,
    dp75d_max integer,
    dp99d_freq integer,
    dp95d_freq integer,
    dp90d_freq integer,
    dp75d_freq integer,
    icy_t95 integer,
    icy_t90 integer,
    icy_t85 integer,
    icy_t75 integer,
    icy_t50 integer,
    icy_tar integer,
    icyd_area integer,
    icyd_max integer,
    icyd_freq integer,
    ict_t95 integer,
    ict_t90 integer,
    ict_t85 integer,
    ict_t75 integer,
    ict_t50 integer,
    ict_tar integer,
    ictd_area integer,
    ictd_max integer,
    ictd_freq integer,
    peak95_kbps integer,
    ping_mbps integer,
    p2t_bps_pct_min integer,
    sp99_cd float,
    sp99_cw float,
    sp95_cd float,
    sp95_cw float,
    sp90_cd float,
    sp90_cw float,
    sp75_cd float,
    sp75_cw float,
    dp99_cd float,
    dp99_cw float,
    dp95_cd float,
    dp95_cw float,
    dp90_cd float,
    dp90_cw float,
    dp75_cd float,
    dp75_cw float,
    icy_cd float,
    icy_cw float,
    ict_cd float,
    ict_cw float,
    zero_ping_count integer,
    zp_ratio float,
    n_count integer,
    avg_sp99d float,
    avg_sp95d float,
    avg_sp90d float,
    avg_sp75d float,
    avg_dp99d float,
    avg_dp95d float,
    avg_dp90d float,
    avg_dp75d float,
    avg_icyd float,
    avg_ictd float
);

.import /var/www/txt/processed.csv mrqos_sum_14d

drop table if exists processed_2wjoin;
create table processed_2wjoin (
    startdate date,
    enddate date,
    maprule integer,
    geo varchar,
    netname varchar,
    sp95_t95 integer,
    sp95_t90 integer,
    sp75_t95 integer,
    sp75_t90 integer,
    dp95_t95 integer,
    dp95_t90 integer,
    dp75_t95 integer,
    dp75_t90 integer,
    ocy_t95 integer,
    ocy_t90 integer,
    oct_t95 integer,
    oct_t90 integer,
    load float,
    sp95_t95_2w integer,
    sp95_t90_2w integer,
    sp75_t95_2w integer,
    sp75_t90_2w integer,
    dp95_t95_2w integer,
    dp95_t90_2w integer,
    dp75_t95_2w integer,
    dp75_t90_2w integer,
    ocy_t95_2w integer,
    ocy_t90_2w integer,
    oct_t95_2w integer,
    oct_t90_2w integer
);
.import /var/www/txt/processed_2wjoin_full.csv processed_2wjoin

