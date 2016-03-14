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
