SELECT clinic, SUM(records) AS records, SUM(review_count) AS reviews,
       ROUND(100.0 * SUM(review_count) / SUM(records), 2) AS review_pct
FROM YOUR_CATALOG.lab_medical.gold
GROUP BY clinic
ORDER BY reviews DESC, clinic;
