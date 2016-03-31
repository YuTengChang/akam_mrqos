.separator ,
drop table if exists region_view_hour;
create table region_view_hour (
    region integer,
    name varchar,
        ecor integer,
        continent varchar,
        country varchar,
        city varchar,
        latitude float,
        longitude float,
        asnum integer,
        provider varchar,
        region_capacity bigint,
        ecor_capacity bigint,
        prp varchar,
        numghost integer,
        info varchar,
        date integer,
        hour varchar
 );
create index if not exists date_idx on region_view_hour (date ASC);
create index if not exists region_idx on region_view_hour (region ASC);
