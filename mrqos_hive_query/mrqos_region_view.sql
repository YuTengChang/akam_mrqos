use mrqos;

SELECT
    *
FROM
(
    SELECT region, collect_set(info) distribution
    FROM
    (
        SELECT
            a.region,
            round(100*a.case_region_load/b.caseload,2) case_load_perc,
            concat(a.casename," (",round(100*a.case_region_load/b.caseload,2),"%:", a.case_region_load,":",b.caseload,")") info
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
    GROUP BY region
) mr_region_table
INNER JOIN
(
    SELECT
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

where region=15278
;


select a1.casename, sum(a1.ra_load) caseload from
(select *, concat("MR_",maprule,":GEO_",geoname,":",netname) casename from mrqos_region) a1
group by a1.casename
limit 3;