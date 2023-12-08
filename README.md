# CIS 9440 NYC Rodent Issue
This repository stores the Python code for the Fall 2023 class project of CIS 9440 Data Warehousing and Analytics at Baruch College.  
**Professor:** Professor Isaac Vaghefi  
**Team members:**
- Komsit Rattana
- Angela Lee
- Derek Strang
- Mariya Mithaiwala

The project analyzes NYC 311 Service Requests data to find insights from the rodent complaints with secondary data sources like:  
- NYC Geographical boundaries (NTA, Tract, Block, zip code)
  - https://data.cityofnewyork.us/City-Government/2020-Neighborhood-Tabulation-Areas-NTAs-Mapped/4hft-v355
  - https://data.cityofnewyork.us/City-Government/2020-Census-Tracts-Tabular/63ge-mke6
  - https://data.cityofnewyork.us/City-Government/2020-Census-Blocks-Mapped/sbvv-hpwv
- NYC Open Restaurant data
  https://data.cityofnewyork.us/Transportation/Open-Restaurant-Applications-Historic-/pitm-atqc
- Restaurant Inspection data
  https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j
- Population data by block from 2020 Census Data-census blocks  
  https://www.nyc.gov/site/planning/planning-level/nyc-population/2020-census.page

## Technology used
- Google Dataflow
- Google BigQuery
- Apache Beam
- Python

## Dimensional model
![CIS 9440 DW Project - Dimensional Model (1)](https://github.com/komsitr/cis9440-nyc-rodent-issue/assets/7727886/f8ab7af2-5366-4d6e-8f4d-ae7496978840)

## Project structure
- **location-transformation-pipeline**
  - Prepare staging data for location by extract geolocations from NYC 311 Service Request, Open Restaurant data, Restaurant Inspection data
  - Resolve to NTA, Tract, and Block
- **nyc-2020-census-block-dataload**
  - Extract Census Block to BigQuery
- **nyc-2020-neighborhood-dataload**
  - Extract NTA boundaries to BigQuery
- **nyc-311-request-extract-pipeline**
  - Extract NYC 311 Service Requests from API to BigQuery
  - Support snapshot and incremental load
- **nyc-open-restaurant-application-dataload**
  - Extract Open Restaurant data to BigQuery
- **nyc-restaurant-inspection-extract-pipeline**
  - Extract Restaurant Inspection data from API to BigQuery
  - Support snapshot and incremental load
- **sql**  
  SQL files for Schedule SQL Queries in BigQuery to refresh or incrementally load data into the tables
- **schemas**  
  Schemas for all BigQuery tables
- **data**  
  CSV for static and historical data
