.separator ,
drop table if exists reg_crit;
create table reg_crit (
    region integer,
    geo_list varchar,
    mpg_load float,
    mpg_num integer,
    Bratio float,
    Fratio float,
    Vratio float,
    dw_Bratio float,
    dw_Fratio float,
    dw_Vratio float,
    Gmpg_num integer,
    date integer
 );

create index if not exists date_idx on reg_crit (date ASC);

.import /var/www/txt/reg_importance.csv reg_crit
