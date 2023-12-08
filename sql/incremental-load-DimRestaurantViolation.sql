DECLARE `latestProcessedInspectionDate` DEFAULT (
  SELECT IFNULL(MAX(`latest_record_date`), DATE(1900, 1, 1))
  FROM `avid-garage-399623.cis9440_project.stage_batch_execution`
  WHERE `job_name` = 'Load_DimRestaurantInspection'
  LIMIT 1
);

DECLARE `latestInspectionDate` DEFAULT (
  SELECT IFNULL(MAX(`inspection_date`), DATE(1900, 1, 1))
  FROM `avid-garage-399623.cis9440_project.nyc_restaurant_inspection`
  LIMIT 1
);

DECLARE `executionTime` DEFAULT CURRENT_DATETIME('America/New_York');

-- Update old records if category is changed
UPDATE `avid-garage-399623.cis9440_project.DimRestaurantViolation` 
SET
  `EndDate` = `executionTime`,
  `IsCurrent` = 0
WHERE `RestaurantViolationID` IN (
  SELECT `dim`.`RestaurantViolationID`
  FROM `avid-garage-399623.cis9440_project.nyc_restaurant_inspection` AS `src`
  INNER JOIN `avid-garage-399623.cis9440_project.focus_violation_code` AS `fvc` ON `fvc`.`violation_code` = `src`.`violation_code`
  LEFT JOIN `avid-garage-399623.cis9440_project.DimRestaurantViolation` AS `dim` ON (
    `dim`.`Cuisine` = `src`.`cuisine_description`
    AND `dim`.`ViolationCode` = `src`.`violation_code`
    AND `dim`.`Category` <> `fvc`.`category`
  )
  WHERE `src`.`inspection_date` > `latestProcessedInspectionDate`
    AND `dim`.`RestaurantViolationID` IS NOT NULL
    AND `dim`.`IsCurrent` = 1
);

-- Insert new records
INSERT INTO `avid-garage-399623.cis9440_project.DimRestaurantViolation` (
  `RestaurantViolationID`, `Cuisine`, `ViolationCode`, `Category`, `StartDate`, `EndDate`, `IsCurrent`
)
SELECT
  GENERATE_UUID() AS `RestaurantViolationID`,
  `out`.*
FROM (
  SELECT DISTINCT
    `src`.`cuisine_description` AS `Cuisine`,
    `src`.`violation_code` AS `ViolationCode`,
    `fvc`.`category` AS `Category`,
    `executionTime` AS `StartDate`,
    DATETIME('9999-12-31') AS `EndDate`,
    1 AS `IsCurrent`
  FROM `avid-garage-399623.cis9440_project.nyc_restaurant_inspection` AS `src`
  INNER JOIN `avid-garage-399623.cis9440_project.focus_violation_code` AS `fvc` ON `fvc`.`violation_code` = `src`.`violation_code`
  LEFT JOIN `avid-garage-399623.cis9440_project.DimRestaurantViolation` AS `dim` ON (
    `dim`.`Cuisine` = `src`.`cuisine_description`
    AND `dim`.`ViolationCode` = `src`.`violation_code`
    AND `dim`.`Category` = `fvc`.`category`
    AND `dim`.`IsCurrent` = 1
  )
  WHERE `src`.`inspection_date` > `latestProcessedInspectionDate`
    AND `dim`.`RestaurantViolationID` IS NULL
) AS `out`;

-- Save the execution time for next incremental load
INSERT INTO `avid-garage-399623.cis9440_project.stage_batch_execution`
(`job_name`, `latest_record_date`, `execution_date`)
VALUES ('Load_DimRestaurantInspection', `latestInspectionDate`, `executionTime`);

