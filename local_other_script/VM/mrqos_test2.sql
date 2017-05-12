select startdate,enddate,maprule,origin.geoname geoname,origin.netname netname,sp99_t95,sp99_t90,sp99_t85,sp99_t75,sp99_t50,sp95_t95,sp95_t90,sp95_t85,sp95_t75,sp95_t50,sp90_t95,sp90_t90,sp90_t85,sp90_t75,sp90_t50,sp75_t95,sp75_t90,sp75_t85,sp75_t75,sp75_t50,star,sp99d_area,sp95d_area,sp90d_area,sp75d_area,sp99d_max,sp95d_max,sp90d_max,sp75d_max,sp99d_freq,sp95d_freq,sp90d_freq,sp75d_freq,dp99_t95,dp99_t90,dp99_t85,dp99_t75,dp99_t50,dp95_t95,dp95_t90,dp95_t85,dp95_t75,dp95_t50,dp90_t95,dp90_t90,dp90_t85,dp90_t75,dp90_t50,dp75_t95,dp75_t90,dp75_t85,dp75_t75,dp75_t50,dtar,dp99d_area,dp95d_area,dp90d_area,dp75d_area,dp99d_max,dp95d_max,dp90d_max,dp75d_max,dp99d_freq,dp95d_freq,dp90d_freq,dp75d_freq,icy_t95,icy_t90,icy_t85,icy_t75,icy_t50,icy_tar,icyd_area,icyd_max,icyd_freq,ict_t95,ict_t90,ict_t85,ict_t75,ict_t50,ict_tar,ictd_area,ictd_max,ictd_freq,peak95_kbps,ping_mbps,p2t_bps_pct_min,sp99_cd,sp99_cw,sp95_cd,sp95_cw,sp90_cd,sp90_cw,sp75_cd,sp75_cw,dp99_cd,dp99_cw,dp95_cd,dp95_cw,dp90_cd,dp90_cw,dp75_cd,dp75_cw,icy_cd,icy_cw,ict_cd,ict_cw,zero_ping_count,zp_ratio,n_count,avg_sp99d,avg_sp95d,avg_sp90d,avg_sp75d,avg_dp99d,avg_dp95d,avg_dp90d,avg_dp75d,avg_icyd,avg_ictd,gsp95,dsp95,bsp95,gdp95,ddp95,bdp95,gicy,dicy,bicy,gict,dict,bict,'author','reason1','reason2'
from 
  (select * from mrqos_sum_14d where maprule=3 and geoname='BR' and netname='ANY' and enddate='2016-01-28') origin
left outer join 
(
select geoname, netname, 
       sum(gsp95) gsp95, sum(dsp95) dsp95, sum(bsp95) bsp95, sum(vsp95) vsp95, 
       sum(gdp95) gdp95, sum(ddp95) ddp95, sum(bdp95) bdp95, sum(vdp95) vdp95,
       sum(gicy) gicy, sum(dicy) dicy, sum(bicy) bicy, sum(vicy) vicy,
       sum(gict) gict, sum(dict) dict, sum(bict) bict, sum(vict) vict from
(
select 
	startdate,
	enddate,
	maprule,
	geoname,
	netname,
	case 	when maprule=1 then sp95d
		when maprule=290 then 0
		when maprule=332 then 0
		else 0 end gsp95,
        case    when maprule=1 then 0
                when maprule=290 then sp95d
                when maprule=332 then 0
                else 0 end dsp95,
        case    when maprule=1 then 0
                when maprule=290 then 0
                when maprule=332 then sp95d
                else 0 end bsp95,
        case    when maprule=1 then 0
                when maprule=290 then 0
                when maprule=332 then 0
                else sp95d end vsp95,
        case    when maprule=1 then dp95d
                when maprule=290 then 0
                when maprule=332 then 0
                else 0 end gdp95,
        case    when maprule=1 then 0
                when maprule=290 then dp95d
                when maprule=332 then 0
                else 0 end ddp95,
        case    when maprule=1 then 0
                when maprule=290 then 0
                when maprule=332 then dp95d
                else 0 end bdp95,
        case    when maprule=1 then 0
                when maprule=290 then 0
                when maprule=332 then 0
                else dp95d end vdp95,
        case    when maprule=1 then icyd
                when maprule=290 then 0
                when maprule=332 then 0
                else 0 end gicy,
        case    when maprule=1 then 0
                when maprule=290 then icyd
                when maprule=332 then 0
                else 0 end dicy,
        case    when maprule=1 then 0
                when maprule=290 then 0
                when maprule=332 then icyd
                else 0 end bicy,
        case    when maprule=1 then 0
                when maprule=290 then 0
                when maprule=332 then 0
                else icyd end vicy,
        case    when maprule=1 then ictd
                when maprule=290 then 0
                when maprule=332 then 0
                else 0 end gict,
        case    when maprule=1 then 0
                when maprule=290 then ictd
                when maprule=332 then 0
                else 0 end dict,
        case    when maprule=1 then 0
                when maprule=290 then 0
                when maprule=332 then ictd
                else 0 end bict,
        case    when maprule=1 then 0
                when maprule=290 then 0
                when maprule=332 then 0
                else ictd end vict
from
(
  select startdate, enddate, maprule, geoname, netname, avg(sp95d) sp95d, avg(dp95d) dp95d, avg(icyd) icyd, avg(ictd) ictd from 
  (
    select startdate, 
           enddate, 
           maprule, 
           geoname, 
           netname, 
           avg_sp95d sp95d, 
           avg_dp95d dp95d, 
           avg_icyd icyd, 
           avg_ictd ictd 
    from mrqos_sum_14d where maprule in (1,290,332) and geoname='BR' and netname='ANY' and enddate='2016-01-28' order by 1,3
  )
  group by startdate, enddate, maprule, geoname, netname
)
) group by startdate, enddate, netname, geoname
) metric 
on origin.geoname=metric.geoname and origin.netname=metric.netname
