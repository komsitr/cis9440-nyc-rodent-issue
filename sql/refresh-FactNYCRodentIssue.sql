DELETE FROM `avid-garage-399623.cis9440_project.FactNYCRodentIssue` WHERE 1=1;

INSERT INTO `avid-garage-399623.cis9440_project.FactNYCRodentIssue`
  (`RodentIssueID`, LocationID, RestaurantViolationID, OpenRestaurantID, DateID, TimeID, ComplaintID, `NumberOfComplaints`, `NumberOfViolations`, `AvgResolveTimeForComplaints`)
SELECT
  GENERATE_UUID() AS `RodentIssueID`,
  `dim_loc`.LocationID,
  `dim_vio`.RestaurantViolationID,
  `dim_opn`.OpenRestaurantID,
  `dim_date`.DateID,
  `dim_time`.TimeID,
  `dim_comp`.ComplaintID,
  COUNT(DISTINCT `src`.unique_key) AS `NumberOfComplaints`,
  COUNT(DISTINCT `ins`.camis) AS `NumberOfViolations`,
  IFNULL(AVG(TIMESTAMP_DIFF(`src`.closed_date, `src`.created_date, HOUR)), 0) AS `AvgResolveTimeForComplaints`,
FROM `avid-garage-399623.cis9440_project.nyc_311_service_request` AS `src`
INNER JOIN `avid-garage-399623.cis9440_project.stage_nyc_311_request_location` AS `src_loc` ON `src_loc`.unique_key = `src`.unique_key
INNER JOIN `avid-garage-399623.cis9440_project.DimLocation` AS `dim_loc` ON (
  `dim_loc`.Block = `src_loc`.block_bctcb2020
  AND `dim_loc`.LocationType = `src_loc`.locationType
)
FULL OUTER JOIN `avid-garage-399623.cis9440_project.stage_nyc_restaurant_inspection_location` AS `ins_loc` ON (
  `ins_loc`.block_bctcb2020 = `dim_loc`.Block
  AND `ins_loc`.locationType = `dim_loc`.LocationType
)
LEFT JOIN `avid-garage-399623.cis9440_project.nyc_restaurant_inspection` AS `ins` ON `ins`.camis = `ins_loc`.camis
FULL OUTER JOIN `avid-garage-399623.cis9440_project.stage_nyc_open_restaurant_location` AS `opn_loc` ON (
  `opn_loc`.block_bctcb2020 = `dim_loc`.Block
  AND `opn_loc`.locationType = `dim_loc`.LocationType
)
LEFT JOIN `avid-garage-399623.cis9440_project.nyc_open_restaurant_applications_historic` AS `opn` ON (
  `opn`.objectid = `opn_loc`.objectid
  AND (`opn`.approved_for_roadway_seating = 'yes' OR `opn`.approved_for_sidewalk_seating = 'yes')
)
INNER JOIN `avid-garage-399623.cis9440_project.DimComplaint` AS `dim_comp` ON (
  `dim_comp`.ComplaintType = IFNULL(`src`.complaint_type, 'Not Interested')
  AND `dim_comp`.ComplaintDescription = IFNULL(`src`.descriptor, 'N/A')
)
INNER JOIN `avid-garage-399623.cis9440_project.DimRestaurantViolation` AS `dim_vio` ON (
  `dim_vio`.ViolationCode = IFNULL(`ins`.violation_code, 'None')
  AND `dim_vio`.Cuisine = IFNULL(`ins`.cuisine_description, 'None')
)
INNER JOIN `avid-garage-399623.cis9440_project.DimOpenRestaurant` AS `dim_opn` ON (
  `dim_opn`.OpenRestaurantType = IFNULL(`opn`.seating_interest_sidewalk, 'none')
  AND `dim_opn`.IsQualifyAlcohol = CASE WHEN `opn`.qualify_alcohol = 'yes' THEN 1 ELSE 0 END
)
INNER JOIN `avid-garage-399623.cis9440_project.DimDate` AS `dim_date` ON (
  `dim_date`.DateID = CASE
    WHEN `src`.created_date IS NOT NULL THEN FORMAT_DATE('%Y-%m-%d', `src`.created_date)
    WHEN `ins`.inspection_date IS NOT NULL THEN FORMAT_DATE('%Y-%m-%d', `ins`.inspection_date)
    ELSE NULL
  END
)
INNER JOIN `avid-garage-399623.cis9440_project.DimTime` AS `dim_time` ON (
  `dim_time`.Hour = CASE
    WHEN `src`.created_date IS NOT NULL THEN EXTRACT(HOUR FROM `src`.created_date)
    WHEN `ins`.inspection_date IS NOT NULL THEN EXTRACT(HOUR FROM `ins`.inspection_date)
    ELSE NULL
  END
)
GROUP BY
  `dim_loc`.LocationID,
  `dim_vio`.RestaurantViolationID,
  `dim_opn`.OpenRestaurantID,
  `dim_date`.DateID,
  `dim_time`.TimeID,
  `dim_comp`.ComplaintID;