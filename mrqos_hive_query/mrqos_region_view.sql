use mrqos;


select region, collect_set(info) distribution
from
(
    select a.region, round(100*a.case_region_load/b.caseload,2) case_load_perc, concat(a.casename,"(",round(100*a.case_region_load/b.caseload,2),"% of", b.caseload,")") info from
    (
        select sum(ra_load) case_region_load,
               concat("MR_",maprule,":GEO_",geoname,":",netname) casename,
               region
        from mrqos_region
        group by maprule, geoname, netname, region
    ) a
    inner join
    (
        select a1.casename, sum(a1.ra_load) caseload from
        (select *, concat("MR_",maprule,":GEO_",geoname,":",netname) casename from mrqos_region) a1
        group by a1.casename
    ) b
    on a.casename=b.casename
) allbox
group by region
limit 3;


select a1.casename, sum(a1.ra_load) caseload from
(select *, concat("MR_",maprule,":GEO_",geoname,":",netname) casename from mrqos_region) a1
group by a1.casename
limit 3;