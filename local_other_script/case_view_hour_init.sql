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
create index if not exists date_idx on case_view_hour (date ASC);
create index if not exists maprule_idx on case_view_hour (maprule ASC);
create index if not exists geoname_idx on case_view_hour (geoname ASC);
create index if not exists netname_idx on case_view_hour (netname ASC);
