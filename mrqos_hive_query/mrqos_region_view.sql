use mrqos;


select region, collect_set(info) distribution
from
(
    select a.*, b.region_ra_load, round(100*a.case_ra_load/b.region_ra_load,2) case_load_perc, concat(a.casename,"(",round(100*a.case_ra_load/b.region_ra_load,2),")") info from
    (
        select sum(ra_load) case_ra_load,
               concat("MR_",maprule,":GEO_",geoname,":",netname) casename,
               region
        from mrqos_region
        group by maprule, geoname, netname, region
    ) a
    inner join
    (
        select region, sum(ra_load) region_ra_load from mrqos_region group by region
    ) b
    on a.region=b.region
) allbox
group by region
limit 3;