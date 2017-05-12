SELECT  baseline.maprule,
        baseline.geoname,

        baseline.avg_sp95d vsp95d,
        standard.bsp95d,
        standard.gsp95d,
        standard.dsp95d,
        baseline.star,

        baseline.avg_dp95d vdp95d,
        standard.bdp95d,
        standard.gdp95d,
        standard.ddp95d,
        baseline.dtar,

        baseline.avg_icyd vicyd,
        standard.bicyd,
        standard.gicyd,
        standard.dicyd,
        baseline.icy_tar,

        baseline.avg_ictd victd,
        standard.bictd,
        standard.gictd,
        standard.dictd,
        baseline.ict_tar,

        baseline.netname
FROM
(
SELECT  gmap.geoname,
        gmap.netname,
        bmap.avg_sp95d bsp95d,
        gmap.avg_sp95d gsp95d,
        dmap.avg_sp95d dsp95d,
        bmap.avg_dp95d bdp95d,
        gmap.avg_dp95d gdp95d,
        dmap.avg_dp95d ddp95d,
        bmap.avg_icyd bicyd,
        gmap.avg_icyd gicyd,
        dmap.avg_icyd dicyd,
        gmap.avg_ictd gictd,
        dmap.avg_ictd dictd,
        bmap.avg_ictd bictd
FROM

  (
    select gb.geoname, gb.netname, gb.avg_sp95d, gb.avg_dp95d, gb.avg_icyd, gb.avg_ictd from
    (select regexp_replace(date_sub(to_date(from_unixtime(unix_timestamp())),1),"-","") tsng1 from mrqos_sum limit 1) ga
    JOIN
    (select * from mrqos_sum where maprule=1) gb
    ON (ga.tsng1=gb.ts)
  ) gmap
  JOIN
  (
    select db.geoname, db.netname, db.avg_sp95d, db.avg_dp95d, db.avg_icyd, db.avg_ictd from
    (select regexp_replace(date_sub(to_date(from_unixtime(unix_timestamp())),1),"-","") tsng1 from mrqos_sum limit 1) da
    JOIN
    (select * from mrqos_sum where maprule=290) db
    ON (da.tsng1=db.ts)
  ) dmap
  ON (gmap.geoname=dmap.geoname AND gmap.netname=dmap.netname)
  JOIN
  (
    select bb.geoname, bb.netname, bb.avg_sp95d, bb.avg_dp95d, bb.avg_icyd, bb.avg_ictd from
    (select regexp_replace(date_sub(to_date(from_unixtime(unix_timestamp())),1),"-","") tsng1 from mrqos_sum limit 1) ba
    JOIN
    (select * from mrqos_sum where maprule=332) bb
    ON (ba.tsng1=bb.ts)
  ) bmap
  ON (gmap.geoname=bmap.geoname AND gmap.netname=bmap.netname)
) standard
JOIN
(
  select b.maprule, b.geoname, b.netname, b.avg_sp95d, b.avg_dp95d, b.avg_icyd, b.avg_ictd, b.star, b.dtar, b.icy_tar, b.ict_tar from
  (select regexp_replace(date_sub(to_date(from_unixtime(unix_timestamp())),1),"-","") tsng1 from mrqos_sum limit 1) a
  JOIN
  (select * from mrqos_sum) b
  ON (a.tsng1=b.ts)
  WHERE
    ( b.avg_sp95d>0 OR b.avg_dp95d>0 OR b.avg_icyd>0 OR b.avg_ictd>0 )
) baseline
ON ( baseline.geoname=standard.geoname AND baseline.netname=standard.netname )
ORDER BY maprule, geoname, netname;
