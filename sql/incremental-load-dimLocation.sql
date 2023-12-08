DECLARE `latestProcessed311RequestLocationDate` DEFAULT (
  SELECT IFNULL(MAX(`latest_record_date`), DATE(1900, 1, 1))
  FROM `avid-garage-399623.cis9440_project.stage_batch_execution`
  WHERE `job_name` = 'Load_DimLocation_311Requests'
  LIMIT 1
);

DECLARE `latestProcessedRestaurantInspectionLocationDate` DEFAULT (
  SELECT IFNULL(MAX(`latest_record_date`), DATE(1900, 1, 1))
  FROM `avid-garage-399623.cis9440_project.stage_batch_execution`
  WHERE `job_name` = 'Load_DimLocation_RestaurantInspection'
  LIMIT 1
);

DECLARE `latest311RequestLocationDate` DEFAULT (
  SELECT IFNULL(MAX(`latest_record_date`), DATE(1900, 1, 1))
  FROM `avid-garage-399623.cis9440_project.stage_batch_execution`
  WHERE `job_name` = 'batch_311_request_location'
  LIMIT 1
);

DECLARE `latestRestaurantInspectionLocationDate` DEFAULT (
  SELECT IFNULL(MAX(`latest_record_date`), DATE(1900, 1, 1))
  FROM `avid-garage-399623.cis9440_project.stage_batch_execution`
  WHERE `job_name` = 'batch_restaurant_inspection_location'
  LIMIT 1
);

DECLARE `executionTime` DEFAULT CURRENT_DATETIME('America/New_York');

-- Insert new records
INSERT INTO `avid-garage-399623.cis9440_project.DimLocation` 
  (`LocationID`, `BoroughCode`, `Borough`, `NeighborhoodCode`, `Neighborhood`, `ZipCode`, `Tract`, `Block`, `LocationType`, `PopulationDensity`)
SELECT
  GENERATE_UUID() AS `LocationID`,
  `out`.*
FROM (
  SELECT DISTINCT
    `merge_location`.`borough_code` AS `BoroughCode`,
    `merge_location`.`borough_name` AS `Borough`,
    `merge_location`.`neighborhood_code` AS `NeighborhoodCode`,
    `merge_location`.`neighborhood_name` AS `Neighborhood`,
    `merge_location`.`zipcode` AS `ZipCode`,
    `merge_location`.`tract_bct2020` AS `Tract`,
    `merge_location`.`block_bctcb2020` AS `Block`,
    `merge_location`.`locationType` AS `LocationType`,
    SAFE_DIVIDE(`pop`.`Pop1`, `pop`.`LandAcres`) AS `PopulationDensity`
  FROM (
    SELECT
      `borough_code`,
      `borough_name`,
      `neighborhood_code`,
      `neighborhood_name`,
      `zipcode`,
      `tract_bct2020`,
      `tract_geoid`,
      `block_bctcb2020`,
      `block_geoid`,
      `locationType`
    FROM `avid-garage-399623.cis9440_project.stage_nyc_311_request_location`
    WHERE `created_date` > `latestProcessed311RequestLocationDate`
      AND `borough_code` IS NOT NULL
      AND `borough_name` IS NOT NULL
      AND `neighborhood_code` IS NOT NULL
      AND `neighborhood_name` IS NOT NULL
      AND `zipcode` IS NOT NULL
      AND `tract_bct2020` IS NOT NULL
      AND `tract_geoid` IS NOT NULL
      AND `block_bctcb2020` IS NOT NULL
      AND `block_geoid` IS NOT NULL
      AND `locationType` IS NOT NULL

    UNION ALL

    SELECT
      `borough_code`,
      `borough_name`,
      `neighborhood_code`,
      `neighborhood_name`,
      `zipcode`,
      `tract_bct2020`,
      `tract_geoid`,
      `block_bctcb2020`,
      `block_geoid`,
      `locationType`
    FROM `avid-garage-399623.cis9440_project.stage_nyc_restaurant_inspection_location`
    WHERE `inspection_date` > `latestProcessedRestaurantInspectionLocationDate`
      AND `borough_code` IS NOT NULL
      AND `borough_name` IS NOT NULL
      AND `neighborhood_code` IS NOT NULL
      AND `neighborhood_name` IS NOT NULL
      AND `zipcode` IS NOT NULL
      AND `tract_bct2020` IS NOT NULL
      AND `tract_geoid` IS NOT NULL
      AND `block_bctcb2020` IS NOT NULL
      AND `block_geoid` IS NOT NULL
      AND `locationType` IS NOT NULL

    UNION ALL

    SELECT
      `borough_code`,
      `borough_name`,
      `neighborhood_code`,
      `neighborhood_name`,
      `zipcode`,
      `tract_bct2020`,
      `tract_geoid`,
      `block_bctcb2020`,
      `block_geoid`,
      `locationType`
    FROM `avid-garage-399623.cis9440_project.stage_nyc_open_restaurant_location`
    WHERE `borough_code` IS NOT NULL
      AND `borough_name` IS NOT NULL
      AND `neighborhood_code` IS NOT NULL
      AND `neighborhood_name` IS NOT NULL
      AND `zipcode` IS NOT NULL
      AND `tract_bct2020` IS NOT NULL
      AND `tract_geoid` IS NOT NULL
      AND `block_bctcb2020` IS NOT NULL
      AND `block_geoid` IS NOT NULL
      AND `locationType` IS NOT NULL
  ) AS `merge_location`
  INNER JOIN `avid-garage-399623.cis9440_project.nyc_2020_population_blocks` AS `pop` ON (
    `pop`.`GeoID` = `merge_location`.`block_bctcb2020`
  )
  LEFT JOIN `avid-garage-399623.cis9440_project.DimLocation` AS `dim` ON (
    `dim`.`BoroughCode` = `merge_location`.`borough_code`
    AND `dim`.`Borough` = `merge_location`.`borough_name`
    AND `dim`.`NeighborhoodCode` = `merge_location`.`neighborhood_code`
    AND `dim`.`Neighborhood` = `merge_location`.`neighborhood_name`
    AND `dim`.`ZipCode` = `merge_location`.`zipcode`
    AND `dim`.`Tract` = `merge_location`.`tract_bct2020`
    AND `dim`.`Block` = `merge_location`.`block_bctcb2020`
    AND `dim`.`LocationType` = `merge_location`.`locationType`
  )
  WHERE `dim`.`LocationID` IS NULL
) AS `out`;


-- -- Save the execution times for next incremental load
INSERT INTO `avid-garage-399623.cis9440_project.stage_batch_execution`
(`job_name`, `latest_record_date`, `execution_date`)
VALUES ('Load_DimLocation_311Requests', `latest311RequestLocationDate`, `executionTime`);

INSERT INTO `avid-garage-399623.cis9440_project.stage_batch_execution`
(`job_name`, `latest_record_date`, `execution_date`)
VALUES ('Load_DimLocation_RestaurantInspection', `latestRestaurantInspectionLocationDate`, `executionTime`);

