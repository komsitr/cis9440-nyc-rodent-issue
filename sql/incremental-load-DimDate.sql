WITH `maxDimDate` AS (
  SELECT IFNULL(MAX(PARSE_DATE('%d %B %Y', CONCAT(`DayOfMonth`, ' ', `Month`, ' ', `Year`))), DATE(1900, 1, 1)) AS `latestCreatedDate`
  FROM `avid-garage-399623.cis9440_project.DimDate` LIMIT 1
)

SELECT DISTINCT
  FORMAT_DATE('%Y-%m-%d', `created_date`) AS `DateID`,
  EXTRACT(YEAR FROM `created_date`) AS `Year`,
  FORMAT_DATE('%B', `created_date`) AS `Month`,
  EXTRACT(DAY FROM `created_date`) AS `DayOfMonth`,
  EXTRACT(WEEK FROM `created_date`) AS `Week`,
  FORMAT_DATE('%A', `created_date`) AS `DayOfWeek`,
  CASE WHEN EXTRACT(DAYOFWEEK FROM created_date) IN (1, 7) THEN 1 ELSE 0 END AS `IsWeekend`
FROM (
  SELECT DISTINCT CAST(`created_date` AS DATE) AS `created_date`
  FROM `avid-garage-399623.cis9440_project.nyc_311_service_request`
  WHERE CAST(`created_date` AS DATE) > (SELECT `latestCreatedDate` FROM `maxDimDate`)

  UNION ALL

  SELECT DISTINCT CAST(`inspection_date` AS DATE) AS `inspection_date`
  FROM `avid-garage-399623.cis9440_project.nyc_restaurant_inspection`
  WHERE CAST(`inspection_date` AS DATE) > (SELECT `latestCreatedDate` FROM `maxDimDate`)

) AS `TblCreatedDate`;
