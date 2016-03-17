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
       ":",name,":",ecor,":",continent,":",country,":",city,":",latitude,":",longitude,":",provider,
       ":",region_capacity,":",ecor_capacity,":",prp,":",numghosts,")") case_info
FROM mrqos_region_hour
WHERE datestamp=20160315 and hour=10
) a
GROUP BY maprule, geoname, netname, case_score_target, case_ra_load, case_nsd_demand, case_eu_demand, case_uniq_region
limit 1;