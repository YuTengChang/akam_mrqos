.separator ,
drop table if exists mrqos_aslist;
create table mrqos_aslist (
    netname varchar,
        as_count integer,
        as_list varchar
 );
.import /home/ychang/Documents/Projects/18-DDC/MRQOS_local_data/mrqos_aslist.csv mrqos_aslist
