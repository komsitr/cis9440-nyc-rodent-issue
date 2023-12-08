DELETE FROM `avid-garage-399623.cis9440_project.DimOpenRestaurant` WHERE 1=1;

INSERT INTO `avid-garage-399623.cis9440_project.DimOpenRestaurant`
  (`OpenRestaurantId`, OpenRestaurantType, IsQualifyAlcohol)
SELECT 
  GENERATE_UUID() AS `OpenRestaurantId`,
  `OpenRestaurantTypes`.OpenRestaurantType,
  `OpenRestaurantTypes`.IsQualifyAlcohol,
FROM (
  SELECT DISTINCT
    `seating_interest_sidewalk` AS `OpenRestaurantType`,
    CASE WHEN `qualify_alcohol` = 'yes' THEN 1 ELSE 0 END AS `IsQualifyAlcohol`
  FROM `avid-garage-399623.cis9440_project.nyc_open_restaurant_applications_historic`
  WHERE 
    `approved_for_roadway_seating` = 'yes' 
    OR `approved_for_sidewalk_seating` = 'yes'

  UNION ALL

  SELECT
    'none' AS `OpenRestaurantType`,
    0 AS `IsQualifyAlcohol`
) AS `OpenRestaurantTypes`
ORDER BY `OpenRestaurantType`, `IsQualifyAlcohol`