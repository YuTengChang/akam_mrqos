.separator ,
drop table if exists mrqos_aslist;
create table mrqos_aslist (
    netname varchar,
        as_count integer,
        as_list varchar
 );
.import /opt/web-data/SQLite3/mrqos_aslist.csv mrqos_aslist
