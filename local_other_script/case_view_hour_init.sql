.separator ,
drop table if exists case_view_hour;
create table case_view_hour (
        maprule integer,
        geoname varchar,
        netname varchar,
        case_score_target float,
        case_ra_load bigint,
        case_nsd_demand float,
        case_eu_demand float,
        case_uniq_region integer,
        info varchar,
        date integer,
        hour varchar
);