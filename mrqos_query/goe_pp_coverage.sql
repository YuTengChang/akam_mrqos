SELECT
       a.country code,
       asns,
       tot_demands,
       CASE
         WHEN has_pp = 0 THEN NULL
         ELSE Round(100 * in_country / has_pp, 2)
       END in_country_pct,
       CASE
         WHEN has_pp = 0 THEN NULL
         ELSE Round(100 * in_asn / has_pp, 2)
       END in_ASN_pct,
       Round(100 * has_pp / tot_demands, 2) has_PP_pct,
       CASE
         WHEN has_pp = 0 THEN NULL
         ELSE Round(distance / has_pp, 2)
       END avg_distance
FROM   (SELECT country,
               Count(*) ASNs,
               Sum(sum_demand) tot_demands,
               Sum (sum_demand * in_country_pct * has_pp_pct) in_country,
               Sum(sum_demand * in_asn_pct * has_pp_pct) in_asn,
               Sum(sum_demand * has_pp_pct) has_pp,
               Sum(sum_demand * weighted_distance * has_pp_pct) distance
        FROM   ns_pp_inpct
        WHERE  day = (SELECT Max(day)
                      FROM   ns_pp_inpct)
        GROUP  BY country) a,
       country_code b
WHERE  a.country = b.code
ORDER  BY tot_demands DESC