.separator ,
drop table if exists pp_coverage;
create table pp_coverage (
    TOT_DEMANDS float,
    IN_COUNTRY_PCT float,
    IN_ASN_PCT float,
    HAS_PP_PCT float,
    AVG_DISTANCE float,
    DATESTAMP integer

 );
create index if not exists date_idx on pp_coverage (DATESTAMP ASC);



.separator ,
drop table if exists geo_pp_coverage;
create table geo_pp_coverage (
    COUNTRY varchar,
    ASNS int,
    TOT_DEMANDS float,
    IN_ASN_PCT float,
    IN_COUNTRY_PCT float,
    HAS_PP_PCT float,
    AVG_DISTANCE float,
    DATESTAMP integer

 );
create index if not exists date_idx on geo_pp_coverage (DATESTAMP ASC);
create index if not exists geo_idx on geo_pp_coverage (COUNTRY ASC);


.separator ,
drop table if exists geo_as_pp_coverage;
create table geo_as_pp_coverage (
    COUNTRY varchar,
    ASNUM int,
    SUM_DEMAND float,
    IN_ASN_PCT float,
    IN_PROVIDER_PCT float,
    IN_COUNTRY_PCT float,
    HAS_PP_PCT float,
    WEIGHTED_DISTANCE float,
    DATESTAMP integer

 );
create index if not exists date_idx on geo_as_pp_coverage (date ASC);
create index if not exists geo_idx on geo_as_pp_coverage (COUNTRY ASC);
create index if not exists asnum_idx on geo_as_pp_coverage (ASNUM ASC);