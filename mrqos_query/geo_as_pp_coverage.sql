SELECT   day, country, asnum,
         sum_demand,
         in_country_pct,
         in_asn_pct,
         weighted_distance,
         has_pp_pct,
         in_provider_pct
FROM     ns_pp_inpct
WHERE    day =
         (SELECT max(day)
          FROM   ns_pp_inpct)
ORDER BY sum_demand DESC