select startdate, enddate, geoname, netname, 
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
from mrqos_sum_14d where maprule in (1,290,332) and geoname='BR' and netname='ANY' order by 1,3
)
group by startdate, enddate, maprule, geoname, netname
)
) group by startdate, enddate, netname, geoname
