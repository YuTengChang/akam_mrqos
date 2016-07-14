use mrqos;

SELECT
    region_info.*,
    mr_region_table.distribution
FROM
(
    SELECT region, collect_set(info) distribution
    FROM
    (
        SELECT
            a.region,
            round(100*a.case_region_load/b.caseload,2) case_load_perc,
            concat(a.casename,"(",round(100*a.case_region_load/b.caseload,2),"%:", a.case_region_load,":",b.caseload,")") info
        FROM
        (
            SELECT sum(ra_load) case_region_load,
                   concat("MR_",maprule,":GEO_",geoname,":",netname) casename,
                   region
            FROM mrqos_region
            WHERE datestamp=20160311 AND hour=05
            GROUP BY maprule, geoname, netname, region
        ) a
        INNER JOIN
        (
            SELECT a1.casename, sum(a1.ra_load) caseload FROM
            (
                SELECT *, concat("MR_",maprule,":GEO_",geoname,":",netname) casename
                FROM mrqos_region
                WHERE datestamp=20160311 AND hour=05
            ) a1
            GROUP BY a1.casename
        ) b
        ON a.casename=b.casename
    ) c
    GROUP BY c.region
) mr_region_table
INNER JOIN
(
    SELECT
        a1.region,
        a1.name,
        a1.ecor,
        a1.continent,
        a1.country,
        a1.city,
        round(a1.latitude,3) latitude,
        round(a1.longitude,3) longitude,
        a1.asnum,
        a1.provider,
        a1.region_capacity,
        a1.ecor_capacity,
        a1.prp,
        a1.numghosts
    FROM mapper.barebones a1, (select max(day) maxday from mapper.barebones) a2 where a1.day=a2.maxday
) region_info
ON mr_region_table.region=region_info.region
where region_info.region=15278;


select a1.casename, sum(a1.ra_load) caseload from
(select *, concat("MR_",maprule,":GEO_",geoname,":",netname) casename from mrqos_region) a1
group by a1.casename
limit 3;


SELECT ecor_info.*, ecor_load.distribution FROM
(
SELECT ecor, collect_set(continent)[0] continent, collect_set(country)[0] country, collect_set(city)[0] city,
    collect_set(round(latitude,3))[0] latitude, collect_set(round(longitude,3))[0] longitude, collect_set(asnum)[0] asnum,
    collect_set(provider) provider, collect_set(ecor_capacity) ecor_cap, collect_set(prp)[0] prp, sum(numghosts) numghosts
FROM
( SELECT * from mapper.barebones a1, (select max(day) maxday from mapper.barebones) a2 where a1.day=a2.maxday) a3
GROUP BY ecor
) ecor_info
INNER JOIN
(
SELECT ecor, collect_set(info) distribution from
(
    SELECT
        ecor, sum(case_region_load) case_ecor_load, casename, collect_set(caseload)[0] caseload,
        concat(casename,"(",round(100*sum(case_region_load)/collect_set(caseload)[0],2),"%:",sum(case_region_load),":",collect_set(caseload)[0],")") info
    FROM
    (
        SELECT
            a.region, c.ecor, a.case_region_load, a.casename, b.caseload
        FROM
        (
            SELECT sum(ra_load) case_region_load,
                   concat("MR_",maprule,":GEO_",geoname,":",netname) casename,
                   region
            FROM mrqos_region
            WHERE datestamp=20160311 AND hour=05
            GROUP BY maprule, geoname, netname, region
        ) a
        INNER JOIN
        (
            SELECT a1.casename, sum(a1.ra_load) caseload FROM
            (
                SELECT *, concat("MR_",maprule,":GEO_",geoname,":",netname) casename
                FROM mrqos_region
                WHERE datestamp=20160311 AND hour=05
            ) a1
            GROUP BY a1.casename
        ) b
        ON a.casename=b.casename
        INNER JOIN
        (SELECT region, ecor from mapper.barebones a1, (select max(day) maxday from mapper.barebones) a2 where a1.day=a2.maxday) c
        ON a.region=c.region
    ) c
    GROUP BY ecor, casename, caseload
) d
WHERE case_ecor_load>0
GROUP BY ecor
) ecor_load
on ecor_load.ecor=ecor_info.ecor
LIMIT 2;



========================================================================================================================

USE mrqos; SELECT maprule, geoname, netname, region, avg_region_score, score_target, hourly_region_nsd_demand, hourly_region_eu_demand, hourly_region_ra_load, case_ra_load, case_nsd_demand, case_eu_demand, case_uniq_region, name, ecor, continent, country, city, latitude, longitude, provider, region_capacity, ecor_capacity, prp, numghosts, datestamp, hour FROM mrqos_region_hour limit 2;

========================================================================================================================

SELECT c2.*, c1.score_target FROM
(SELECT maprule, geoname, netname, max(targetvalue) score_target FROM score GROUP BY maprule, geoname, netname) c1
 INNER JOIN
(
SELECT
    b.maprule,
    b.geoname,
    b.netname,
    b.region,
    b.avg_region_score,
    b.max_region_score,
    b.avg_region_nsd_demand,
    b.daily_region_nsd_demand,
    b.avg_region_eu_demand,
    b.daily_region_eu_demand,
    b.daily_region_ra_load,
    b.avg_case_percent_pre_norm_load,
    b.avg_case_percent_load,
    b.avg_recorded_load_percentage,
    b.case_percent_load,
    b.case_percent_nsd_demand,
    b.case_percent_eu_demand,
    b.case_ra_load,
    b.case_nsd_demand,
    b.case_eu_demand,
    b.case_sum_max_region_percent_load,
    b.case_uniq_region,
    a.name,
    a.ecor,
    a.continent,
    a.country,
    a.city,
    round(a.latitude,3) latitude,
    round(a.longitude,3) longitude,
    a.asnum,
    a.provider,
    a.region_capacity,
    a.ecor_capacity,
    a.prp,
    a.numghosts
FROM
( select * from mapper.barebones a1, (select max(day) maxday from mapper.barebones) a2 where a1.day=a2.maxday
) a
INNER JOIN
(
    SELECT b1.maprule maprule,
           b1.geoname geoname,
           b1.netname netname,
           b1.region region,
           round(b1.avg_region_score,2) avg_region_score,
           round(b1.max_region_score,2) max_region_score,
           round(b1.avg_region_nsd_demand,4) avg_region_nsd_demand,
           round(b1.daily_region_nsd_demand,4) daily_region_nsd_demand,
           round(b1.avg_region_eu_demand,4) avg_region_eu_demand,
           round(b1.daily_region_eu_demand,4) daily_region_eu_demand,
           b1.daily_region_ra_load daily_region_ra_load,
           round(b1.case_percent_pre_norm_load,4) avg_case_percent_pre_norm_load,
           round(b1.case_percent_load,4) avg_case_percent_load,
           round(b1.recorded_load_percentage,4) avg_recorded_load_percentage,
           CASE WHEN b2.case_ra_load>0 THEN round(100*b1.daily_region_ra_load/b2.case_ra_load,4) ELSE 0.0 END case_percent_load,
           CASE WHEN b2.case_nsd_demand>0 THEN round(100*b1.daily_region_nsd_demand/b2.case_nsd_demand,4) ELSE 0.0 END case_percent_nsd_demand,
           CASE WHEN b2.case_eu_demand>0 THEN round(100*b1.daily_region_eu_demand/b2.case_eu_demand,4) ELSE 0.0 END case_percent_eu_demand,
           b2.case_ra_load case_ra_load,
           b2.case_nsd_demand case_nsd_demand,
           b2.case_eu_demand case_eu_demand,
           b2.case_sum_max_region_percent_load case_sum_max_region_percent_load,
           b2.uniq_region case_uniq_region,
           b1.datestamp,
           b1.hour
    FROM
    (
        SELECT maprule,
               geoname,
               netname,
               region,
               avg(score) avg_region_score,
               max(score) max_region_score,
               avg(nsd_demand) avg_region_nsd_demand,
               sum(nsd_demand) daily_region_nsd_demand,
               avg(eu_demand) avg_region_eu_demand,
               sum(eu_demand) daily_region_eu_demand,
               sum(ra_load) daily_region_ra_load,
               avg(case_percent_pre_norm_load) case_percent_pre_norm_load,
               avg(case_percent_load) case_percent_load,
               avg(recorded_load_percentage) recorded_load_percentage,
               datestamp,
               hour
        FROM mrqos_region
        WHERE datestamp=20160315 and hour=10
        GROUP BY maprule, geoname, netname, region, datestamp, hour
    ) b1
    INNER JOIN
    (
        SELECT maprule,
               geoname,
               netname,
               sum(ra_load) case_ra_load,
               sum(nsd_demand) case_nsd_demand,
               sum(eu_demand) case_eu_demand,
               round(sum(max_case_percent_load),4) case_sum_max_region_percent_load,
               count(distinct(region)) uniq_region
        FROM
        (
            SELECT maprule,
                   geoname,
                   netname,
                   region,
                   sum(ra_load) ra_load,
                   sum(nsd_demand) nsd_demand,
                   sum(eu_demand) eu_demand,
                   max(CASE WHEN case_percent_load>0 THEN case_percent_load ELSE 0 END) max_case_percent_load
            FROM mrqos_region
            WHERE datestamp=20160315 and hour=10
            GROUP BY maprule, geoname, netname, region
        ) b3
        GROUP BY maprule, geoname, netname
    ) b2
    ON b1.maprule=b2.maprule AND b1.geoname=b2.geoname AND b1.netname=b2.netname
) b
ON a.region=b.region
WHERE b.case_percent_load >= 0.00005
) c2
on c1.maprule=c2.maprule and c1.geoname=c2.geoname and c1.netname=c2.netname;


========================================================================================================================

SELECT maprule, geoname, netname,
       case_score_target, case_ra_load, case_nsd_demand, case_eu_demand, case_uniq_region,
       collect_set(case_info) distribution
FROM
(
SELECT maprule, geoname, netname,
       score_target case_score_target, case_ra_load, case_nsd_demand, case_eu_demand, case_uniq_region,
       concat(region,"(",avg_region_score,
            ":NSD_",hourly_region_nsd_demand,"<",
                CASE WHEN case_nsd_demand>0 THEN round(100*hourly_region_nsd_demand/case_nsd_demand,2) ELSE 0 END,">",
            ":EU_",hourly_region_eu_demand,"<",
                CASE WHEN case_eu_demand>0 THEN round(100*hourly_region_eu_demand/case_eu_demand,2) ELSE 0 END,">",
            ":LOAD_",hourly_region_ra_load,"<",
                CASE WHEN case_ra_load>0 THEN round(100*hourly_region_ra_load/case_ra_load,2) ELSE 0 END,">",
       ":",name,":",ecor,":",continent,":",country,":",city,":",latitude,":",longitude,":",asnum,":",provider,
       ":",region_capacity,":",ecor_capacity,":",prp,":",numghosts,")") case_info
FROM mrqos_region_hour
WHERE datestamp=20160315 and hour=10
) a
GROUP BY maprule, geoname, netname, case_score_target, case_ra_load, case_nsd_demand, case_eu_demand, case_uniq_region
limit 1;

========================================================================================================================
TEST PROCEDURE OF MRQOS_REGION_SUMMARIZE_HOUR
========================================================================================================================

SELECT * FROM
(

SELECT c2.*, c1.score_target FROM
(SELECT maprule, geoname, netname, max(targetvalue) score_target FROM score GROUP BY maprule, geoname, netname) c1
 INNER JOIN
(
SELECT
    b.maprule,
    b.geoname,
    b.netname,
    b.region,
    b.avg_region_score,
    b.max_region_score,
    b.avg_region_nsd_demand,
    b.daily_region_nsd_demand,
    b.avg_region_eu_demand,
    b.daily_region_eu_demand,
    b.daily_region_ra_load,
    b.avg_case_percent_pre_norm_load,
    b.avg_case_percent_load,
    b.avg_recorded_load_percentage,
    b.case_percent_load,
    b.case_percent_nsd_demand,
    b.case_percent_eu_demand,
    b.case_ra_load,
    b.case_nsd_demand,
    b.case_eu_demand,
    b.case_sum_max_region_percent_load,
    b.case_uniq_region,
    a.name,
    a.ecor,
    a.continent,
    a.country,
    a.city,
    round(a.latitude,3) latitude,
    round(a.longitude,3) longitude,
    a.asnum,
    a.provider,
    a.region_capacity,
    a.ecor_capacity,
    a.prp,
    a.numghosts
FROM
( select * from mapper.barebones a1, (select max(day) maxday from mapper.barebones) a2 where a1.day=a2.maxday
) a
INNER JOIN
(
    SELECT b1.maprule maprule,
           b1.geoname geoname,
           b1.netname netname,
           b1.region region,
           round(b1.avg_region_score,2) avg_region_score,
           round(b1.max_region_score,2) max_region_score,
           round(b1.avg_region_nsd_demand,4) avg_region_nsd_demand,
           round(b1.daily_region_nsd_demand,4) daily_region_nsd_demand,
           round(b1.avg_region_eu_demand,4) avg_region_eu_demand,
           round(b1.daily_region_eu_demand,4) daily_region_eu_demand,
           b1.daily_region_ra_load daily_region_ra_load,
           round(b1.case_percent_pre_norm_load,4) avg_case_percent_pre_norm_load,
           round(b1.case_percent_load,4) avg_case_percent_load,
           round(b1.recorded_load_percentage,4) avg_recorded_load_percentage,
           CASE WHEN b2.case_ra_load>0 THEN round(100*b1.daily_region_ra_load/b2.case_ra_load,4) ELSE 0.0 END case_percent_load,
           CASE WHEN b2.case_nsd_demand>0 THEN round(100*b1.daily_region_nsd_demand/b2.case_nsd_demand,4) ELSE 0.0 END case_percent_nsd_demand,
           CASE WHEN b2.case_eu_demand>0 THEN round(100*b1.daily_region_eu_demand/b2.case_eu_demand,4) ELSE 0.0 END case_percent_eu_demand,
           b2.case_ra_load case_ra_load,
           b2.case_nsd_demand case_nsd_demand,
           b2.case_eu_demand case_eu_demand,
           b2.case_sum_max_region_percent_load case_sum_max_region_percent_load,
           b2.uniq_region case_uniq_region,
           b1.datestamp,
           b1.hour
    FROM
    (
        SELECT maprule,
               geoname,
               netname,
               region,
               avg(score) avg_region_score,
               max(score) max_region_score,
               avg(nsd_demand) avg_region_nsd_demand,
               sum(nsd_demand) daily_region_nsd_demand,
               avg(eu_demand) avg_region_eu_demand,
               sum(eu_demand) daily_region_eu_demand,
               sum(ra_load) daily_region_ra_load,
               avg(case_percent_pre_norm_load) case_percent_pre_norm_load,
               avg(case_percent_load) case_percent_load,
               avg(recorded_load_percentage) recorded_load_percentage,
               datestamp,
               hour
        FROM mrqos_region
        WHERE datestamp=20160407 and hour=20
        GROUP BY maprule, geoname, netname, region, datestamp, hour
    ) b1
    INNER JOIN
    (
        SELECT maprule,
               geoname,
               netname,
               sum(ra_load) case_ra_load,
               sum(nsd_demand) case_nsd_demand,
               sum(eu_demand) case_eu_demand,
               round(sum(max_case_percent_load),4) case_sum_max_region_percent_load,
               count(distinct(region)) uniq_region
        FROM
        (
            SELECT maprule,
                   geoname,
                   netname,
                   region,
                   sum(ra_load) ra_load,
                   sum(nsd_demand) nsd_demand,
                   sum(eu_demand) eu_demand,
                   max(CASE WHEN case_percent_load>0 THEN case_percent_load ELSE 0 END) max_case_percent_load
            FROM mrqos_region
            WHERE datestamp=20160407 and hour=20
            GROUP BY maprule, geoname, netname, region
        ) b3
        GROUP BY maprule, geoname, netname
    ) b2
    ON b1.maprule=b2.maprule AND b1.geoname=b2.geoname AND b1.netname=b2.netname
) b
ON a.region=b.region
WHERE b.case_percent_load >= 0.00005
) c2
on c1.maprule=c2.maprule and c1.geoname=c2.geoname and c1.netname=c2.netname

) everyrow LIMIT 3;

========================================================================================================================
TEST PROCEDURE OF MRQOS_SUMMARIZE WITH DIFFERENT WINDOW SIZE
========================================================================================================================

select
     startdate,
     enddate,
     maprule,
     geoname,
     netname,
     sp99_t95,
     sp99_t90,
     sp99_t85,
     sp99_t75,
     sp99_t50,
     sp95_t95,
     sp95_t90,
     sp95_t85,
     sp95_t75,
     sp95_t50,
     sp90_t95,
     sp90_t90,
     sp90_t85,
     sp90_t75,
     sp90_t50,
     sp75_t95,
     sp75_t90,
     sp75_t85,
     sp75_t75,
     sp75_t50,
     star,
     sp99d_area,
     sp95d_area,
     sp90d_area,
     sp75d_area,
     sp99d_max,
     sp95d_max,
     sp90d_max,
     sp75d_max,
     sp99d_freq,
     sp95d_freq,
     sp90d_freq,
     sp75d_freq,
     dp99_t95,
     dp99_t90,
     dp99_t85,
     dp99_t75,
     dp99_t50,
     dp95_t95,
     dp95_t90,
     dp95_t85,
     dp95_t75,
     dp95_t50,
     dp90_t95,
     dp90_t90,
     dp90_t85,
     dp90_t75,
     dp90_t50,
     dp75_t95,
     dp75_t90,
     dp75_t85,
     dp75_t75,
     dp75_t50,
     dtar,
     dp99d_area,
     dp95d_area,
     dp90d_area,
     dp75d_area,
     dp99d_max,
     dp95d_max,
     dp90d_max,
     dp75d_max,
     dp99d_freq,
     dp95d_freq,
     dp90d_freq,
     dp75d_freq,
     icy_t95,
     icy_t90,
     icy_t85,
     icy_t75,
     icy_t50,
     icy_tar,
     icyd_area,
     icyd_max,
     icyd_freq,
     ict_t95,
     ict_t90,
     ict_t85,
     ict_t75,
     ict_t50,
     ict_tar,
     ictd_area,
     ictd_max,
     ictd_freq,
     peak95_kbps,
     ping_mbps,
     p2t_bps_pct_min,
     zero_ping_count,
     round(case when n_count>0 then zero_ping_count/n_count*100 else 0 end, 2) zp_ratio,
     n_count,
     round(case when sp99d_freq>0 then sp99d_area/n_count/star*100 else 0 end, 2) avg_sp99d,
     round(case when sp95d_freq>0 then sp95d_area/n_count/star*100 else 0 end, 2) avg_sp95d,
     round(case when sp90d_freq>0 then sp90d_area/n_count/star*100 else 0 end, 2) avg_sp90d,
     round(case when sp75d_freq>0 then sp75d_area/n_count/star*100 else 0 end, 2) avg_sp75d,
     round(case when dp99d_freq>0 then dp99d_area/n_count/dtar*100 else 0 end, 2) avg_dp99d,
     round(case when dp95d_freq>0 then dp95d_area/n_count/dtar*100 else 0 end, 2) avg_dp95d,
     round(case when dp90d_freq>0 then dp90d_area/n_count/dtar*100 else 0 end, 2) avg_dp90d,
     round(case when dp75d_freq>0 then dp75d_area/n_count/dtar*100 else 0 end, 2) avg_dp75d,
     round(case when icyd_freq>0 then 100*icyd_area/(n_count*(100-icy_tar)) else 0 end, 2) avg_icyd,
     round(case when ictd_freq>0 then 100*ictd_area/(n_count*(100-ict_tar)) else 0 end, 2) avg_ictd
from (
    select
        to_date(from_unixtime((unix_timestamp()-(unix_timestamp()%86400))-1*86400)) STARTDATE,
        to_date(from_unixtime(unix_timestamp()-(unix_timestamp()%86400))) ENDDATE,
        maprule,
        geoname,
        netname,
        cast(percentile(sp99, 0.9584) as int) sp99_t95,
        cast(percentile(sp99, 0.9167) as int) sp99_t90,
        cast(percentile(sp99, 0.8572) as int) sp99_t85,
        cast(percentile(sp99, 0.75) as int) sp99_t75,
        cast(percentile(sp99, 0.5) as int) sp99_t50,
        cast(percentile(sp95, 0.9584) as int) sp95_t95,
        cast(percentile(sp95, 0.9167) as int) sp95_t90,
        cast(percentile(sp95, 0.8572) as int) sp95_t85,
        cast(percentile(sp95, 0.75) as int) sp95_t75,
        cast(percentile(sp95, 0.5) as int) sp95_t50,
        cast(percentile(sp90, 0.9584) as int) sp90_t95,
        cast(percentile(sp90, 0.9167) as int) sp90_t90,
        cast(percentile(sp90, 0.8572) as int) sp90_t85,
        cast(percentile(sp90, 0.75) as int) sp90_t75,
        cast(percentile(sp90, 0.5) as int) sp90_t50,
        cast(percentile(sp75, 0.9584) as int) sp75_t95,
        cast(percentile(sp75, 0.9167) as int) sp75_t90,
        cast(percentile(sp75, 0.8572) as int) sp75_t85,
        cast(percentile(sp75, 0.75) as int) sp75_t75,
        cast(percentile(sp75, 0.5) as int) sp75_t50,
        min(star) star,
        sum(sp99d) sp99d_area,
        sum(sp95d) sp95d_area,
        sum(sp90d) sp90d_area,
        sum(sp75d) sp75d_area,
        max(sp99d) sp99d_max,
        max(sp95d) sp95d_max,
        max(sp90d) sp90d_max,
        max(sp75d) sp75d_max,
        sum(case when sp99d>0 then 1 else 0 end) sp99d_freq,
        sum(case when sp95d>0 then 1 else 0 end) sp95d_freq,
        sum(case when sp90d>0 then 1 else 0 end) sp90d_freq,
        sum(case when sp75d>0 then 1 else 0 end) sp75d_freq,
        cast(percentile(dp99, 0.9584) as int) dp99_t95,
        cast(percentile(dp99, 0.9167) as int) dp99_t90,
        cast(percentile(dp99, 0.8572) as int) dp99_t85,
        cast(percentile(dp99, 0.75) as int) dp99_t75,
        cast(percentile(dp99, 0.5) as int) dp99_t50,
        cast(percentile(dp95, 0.9584) as int) dp95_t95,
        cast(percentile(dp95, 0.9167) as int) dp95_t90,
        cast(percentile(dp95, 0.8572) as int) dp95_t85,
        cast(percentile(dp95, 0.75) as int) dp95_t75,
        cast(percentile(dp95, 0.5) as int) dp95_t50,
        cast(percentile(dp90, 0.9584) as int) dp90_t95,
        cast(percentile(dp90, 0.9167) as int) dp90_t90,
        cast(percentile(dp90, 0.8572) as int) dp90_t85,
        cast(percentile(dp90, 0.75) as int) dp90_t75,
        cast(percentile(dp90, 0.5) as int) dp90_t50,
        cast(percentile(dp75, 0.9584) as int) dp75_t95,
        cast(percentile(dp75, 0.9167) as int) dp75_t90,
        cast(percentile(dp75, 0.8572) as int) dp75_t85,
        cast(percentile(dp75, 0.75) as int) dp75_t75,
        cast(percentile(dp75, 0.5) as int) dp75_t50,
        min(dtar) dtar,
        sum(dp99d) dp99d_area,
        sum(dp95d) dp95d_area,
        sum(dp90d) dp90d_area,
        sum(dp75d) dp75d_area,
        max(dp99d) dp99d_max,
        max(dp95d) dp95d_max,
        max(dp90d) dp90d_max,
        max(dp75d) dp75d_max,
        sum(case when dp99d>0 then 1 else 0 end) dp99d_freq,
        sum(case when dp95d>0 then 1 else 0 end) dp95d_freq,
        sum(case when dp90d>0 then 1 else 0 end) dp90d_freq,
        sum(case when dp75d>0 then 1 else 0 end) dp75d_freq,
        cast(percentile(icy_pct, 0.0416) as int) icy_t95,
        cast(percentile(icy_pct, 0.0832) as int) icy_t90,
        cast(percentile(icy_pct, 0.1428) as int) icy_t85,
        cast(percentile(icy_pct, 0.25) as int) icy_t75,
        cast(percentile(icy_pct, 0.5) as int) icy_t50,
        min(icy_tar) icy_tar,
        sum(icyd) icyd_area,
        max(icyd) icyd_max,
        sum(case when icyd>0 then 1 else 0 end) icyd_freq,
        cast(percentile(ict_pct, 0.0416) as int) ict_t95,
        cast(percentile(ict_pct, 0.0832) as int) ict_t90,
        cast(percentile(ict_pct, 0.1428) as int) ict_t85,
        cast(percentile(ict_pct, 0.25) as int) ict_t75,
        cast(percentile(ict_pct, 0.5) as int) ict_t50,
        min(ict_tar) ict_tar,
        sum(ictd) ictd_area,
        max(ictd) ictd_max,
        sum(case when ictd>0 then 1 else 0 end) ictd_freq,
        round(percentile(total_bps, 0.95), 4) peak95_kbps,
        round(sum(ping_bps/1024/1024), 4) ping_mbps,
        min(p2t_bps_pct) p2t_bps_pct_min,
        sum(case when ping_bps<1 then 1 else 0 end) zero_ping_count,
        count(*) n_count
    from
        mrqos_join
    where cast(ts as bigint) > (unix_timestamp()-(unix_timestamp()%86400))-1*86400
         and cast(ts as bigint) < (unix_timestamp()-(unix_timestamp()%86400))
    group by
    geoname, maprule, netname
    ) a LIMIT 1;

#=======================



USE mrqos;

INSERT OVERWRITE TABLE region_view_hour PARTITION (datestamp=20160620, hour=01)

SELECT
    region_info.*,
    mr_region_table.distribution
FROM
(
    SELECT
        a1.region,
        a1.name,
        a1.ecor,
        a1.continent,
        a1.country,
        a1.city,
        round(a1.latitude,3) latitude,
        round(a1.longitude,3) longitude,
        a1.asnum,
        a1.provider,
        a1.region_capacity,
        a1.ecor_capacity,
        a1.prp,
        a1.numghosts
    FROM mapper.barebones a1, (select max(day) maxday from mapper.barebones) a2 where a1.day=a2.maxday
) region_info
INNER JOIN
(
    SELECT region, collect_set(info) distribution
    FROM
    (
        SELECT
            a.region,
            round(100*a.case_region_load/b.caseload,2) case_load_perc,
            concat(a.casename,'(',round(100*a.case_region_load/b.caseload,2),':', a.case_region_load,':',b.caseload,')') info
        FROM
        (
            SELECT sum(ra_load) case_region_load,
                   concat('MR_',maprule,':GEO_',geoname,':',netname) casename,
                   region
            FROM mrqos_region
            WHERE datestamp=20160620 AND hour=01
            GROUP BY maprule, geoname, netname, region
        ) a
        INNER JOIN
        (
            SELECT a1.casename, sum(a1.ra_load) caseload FROM
            (
                SELECT *, concat('MR_',maprule,':GEO_',geoname,':',netname) casename
                FROM mrqos_region
                WHERE datestamp=20160620 AND hour=01
            ) a1
            GROUP BY a1.casename
        ) b
        ON a.casename=b.casename
    ) c
    GROUP BY c.region
) mr_region_table
ON mr_region_table.region=region_info.region;


==========================================
TESTING: IORATIO
==========================================

SELECT ab.maprule maprule,
       ab.geoname geoname,
       ab.netname netname,
       Sum(nsd_demand) nsd_demand,
       Sum(eu_demand) eu_demand,
       Sum(ra_load) ra_load,
       CASE
         WHEN Sum(bytes) > 0 THEN ROUND(100 * Sum(bytes_in) / Sum(bytes),3)
         ELSE 0.0
       END                                    in_out_ratio,
       Sum(hits) hits,
       Sum(connections) connections,
       CASE
         WHEN Sum(flytes) > 0 THEN ROUND(100 * Sum(bytes) / Sum(flytes),3)
         ELSE 0.0
       END                                    b2f_ratio,
       round(Sum(cpu)/1000000,3) Mcpu,
       Sum(disk) disk
FROM   (SELECT  maprule,
                geoname,
                netname,
                region,
                nsd_demand,
                eu_demand,
                ra_load
        FROM mrqos.mrqos_region
        WHERE datestamp=20160628
            AND hour=17
            AND ts=1467134102
        ) ab,
       (SELECT region,
               maprule_id,
               Sum(bytes)    bytes,
               Sum(bytes_in) bytes_in,
               Sum(hits)    hits,
               Sum(connections) connections,
               Sum(flytes)  flytes,
               Sum(cpu)     cpu,
               Sum(disk)    disk
        FROM   mrqos.maprule_info d,
               mrqos.mcm_machines e
        WHERE  d.ghostip = e.ghostip
               AND d.ts=e.ts
               AND d.ts=1467132722
        GROUP  BY region,
                  maprule_id) c
WHERE  ab.maprule = c.maprule_id
       AND ab.region = c.region
GROUP  BY ab.maprule, ab.geoname, ab.netname

==========================================
TESTING: IORATIO SUMMARY
==========================================
SELECT  maprule,
        geoname,
        netname,
        cast(percentile(nsd_demand, 0.9584) as int) nsd_demand_t95,
        cast(avg(nsd_demand) as int) nsd_demand_t50,
        cast(percentile(eu_demand, 0.9584) as int) eu_demand_t95,
        cast(avg(eu_demand) as int) eu_demand_t50,
        cast(percentile(ra_load, 0.9584) as int) ra_load_t95,
        cast(avg(ra_load) as int) ra_load_t50,
        round(cast(percentile(cast(in_out_ratio*1000 as int), 0.9584) as double)/1000, 3) in_out_ratio_t95,
        round(avg(in_out_ratio), 3) in_out_ratio_t50,
        cast(percentile(hits, 0.9584) as int) hits_t95,
        cast(avg(hits) as int) hits_t50,
        cast(percentile(connections, 0.9584) as int) connections_t95,
        cast(avg(connections) as int) connections_t50,
        round(cast(percentile(cast(b2f_ratio*1000 as int), 0.9584) as double)/1000, 3) b2f_ratio_t95,
        round(avg(b2f_ratio), 3) b2f_ratio_t50,
        round(cast(percentile(cast(m_cpu*1000 as int), 0.9584) as double)/1000, 3) m_cpu_t95,
        cast(avg(m_cpu) as int) m_cpu_t50,
        cast(percentile(disk, 0.9584) as int) disk_t95,
        cast(avg(disk) as int) disk_t50,
        count(*) case_count
FROM mrqos.mrqos_ioratio
WHERE datestamp > 20160710
GROUP BY maprule, geoname, netname
LIMIT 10;