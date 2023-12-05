import sys
import logging
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
from google.cloud import bigquery as googleBigQuery

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

STAGE_311_LOCATION_TABLE_SCHEMA = {
    'fields': [
        {
            'name': 'unique_key', 'type': 'STRING', 'mode': 'REQUIRED'
        }, 
        {
            'name': 'created_date', 'type': 'DATETIME', 'mode': 'REQUIRED'
        },
        {
            'name': 'borough_code', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'borough_name', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'neighborhood_code', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'neighborhood_name', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'zipcode', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'tract_bct2020', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'tract_geoid', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'block_bctcb2020', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'block_geoid', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'locationType', 'type': 'STRING', 'mode': 'NULLABLE'
        }
    ]
}

STAGE_RESTAURANT_INSPECTION_LOCATION_TABLE_SCHEMA = {
    'fields': [
        {
            'name': 'camis', 'type': 'STRING', 'mode': 'REQUIRED'
        }, 
        {
            'name': 'inspection_date', 'type': 'DATETIME', 'mode': 'REQUIRED'
        },
        {
            'name': 'borough_code', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'borough_name', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'neighborhood_code', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'neighborhood_name', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'zipcode', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'tract_bct2020', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'tract_geoid', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'block_bctcb2020', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'block_geoid', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'locationType', 'type': 'STRING', 'mode': 'NULLABLE'
        }
    ]
}

STAGE_OPEN_RESTAURANT_LOCATION_TABLE_SCHEMA = {
    'fields': [
        {
            'name': 'objectid', 'type': 'STRING', 'mode': 'REQUIRED'
        }, 
        {
            'name': 'borough_code', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'borough_name', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'neighborhood_code', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'neighborhood_name', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'zipcode', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'tract_bct2020', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'tract_geoid', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'block_bctcb2020', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'block_geoid', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'locationType', 'type': 'STRING', 'mode': 'NULLABLE'
        }
    ]
}

class Map311RequestResultToBigQueryRecord(beam.DoFn):
    def get_raw_value(self, recordGroup, key):
        for recordList in recordGroup[1]:
            for record in recordList:
                if key in record and record[key] is not None:
                    return record[key]
        return None

    def process(self, recordGroup, locationTypeCategoryList):

        locationType = self.get_raw_value(recordGroup, 'location_type')
        locationCategory = 'Unspecified'
        for locationTypeCategory in locationTypeCategoryList:
            if locationTypeCategory['location_type'] == locationType:
                locationCategory = locationTypeCategory['category']
                break

        out = {
            'unique_key': recordGroup[0],
            'created_date': self.get_raw_value(recordGroup, 'created_date'),
            'borough_code': self.get_raw_value(recordGroup, 'borough_code'),
            'borough_name': self.get_raw_value(recordGroup, 'borough_name'),
            'neighborhood_code': self.get_raw_value(recordGroup, 'neighborhood_code'),
            'neighborhood_name': self.get_raw_value(recordGroup, 'neighborhood_name'),
            'zipcode': self.get_raw_value(recordGroup, 'incident_zip'),
            'tract_bct2020': self.get_raw_value(recordGroup, 'tract_bct2020'),
            'tract_geoid': self.get_raw_value(recordGroup, 'tract_geoid'),
            'block_bctcb2020': self.get_raw_value(recordGroup, 'block_bctcb2020'),
            'block_geoid': self.get_raw_value(recordGroup, 'block_geoid'),
            'locationType': locationCategory
        }

        return [out]
    
class MapRestaurantInspectionResultToBigQueryRecord(beam.DoFn):
    def get_raw_value(self, recordGroup, key):
        for recordList in recordGroup[1]:
            for record in recordList:
                if key in record and record[key] is not None:
                    return record[key]
        return None

    def process(self, recordGroup):
        out = {
            'camis': recordGroup[0],
            'inspection_date': self.get_raw_value(recordGroup, 'inspection_date'),
            'borough_code': self.get_raw_value(recordGroup, 'borough_code'),
            'borough_name': self.get_raw_value(recordGroup, 'borough_name'),
            'neighborhood_code': self.get_raw_value(recordGroup, 'neighborhood_code'),
            'neighborhood_name': self.get_raw_value(recordGroup, 'neighborhood_name'),
            'zipcode': self.get_raw_value(recordGroup, 'zipcode'),
            'tract_bct2020': self.get_raw_value(recordGroup, 'tract_bct2020'),
            'tract_geoid': self.get_raw_value(recordGroup, 'tract_geoid'),
            'block_bctcb2020': self.get_raw_value(recordGroup, 'block_bctcb2020'),
            'block_geoid': self.get_raw_value(recordGroup, 'block_geoid'),
            'locationType': 'Commercial'
        }

        return [out]
    
class MapOpenRestaurantResultToBigQueryRecord(beam.DoFn):
    def get_raw_value(self, recordGroup, key):
        for recordList in recordGroup[1]:
            for record in recordList:
                if key in record and record[key] is not None:
                    return record[key]
        return None

    def process(self, recordGroup):
        out = {
            'objectid': recordGroup[0],
            'borough_code': self.get_raw_value(recordGroup, 'borough_code'),
            'borough_name': self.get_raw_value(recordGroup, 'borough_name'),
            'neighborhood_code': self.get_raw_value(recordGroup, 'neighborhood_code'),
            'neighborhood_name': self.get_raw_value(recordGroup, 'neighborhood_name'),
            'zipcode': self.get_raw_value(recordGroup, 'zip'),
            'tract_bct2020': self.get_raw_value(recordGroup, 'tract_bct2020'),
            'tract_geoid': self.get_raw_value(recordGroup, 'tract_geoid'),
            'block_bctcb2020': self.get_raw_value(recordGroup, 'block_bctcb2020'),
            'block_geoid': self.get_raw_value(recordGroup, 'block_geoid'),
            'locationType': 'Commercial'
        }

        return [out]
    
class ResolveNYCBlock(beam.DoFn):
    def process(self, record, blockMapDict, boroughColName):
        from shapely import Point
        import logging

        location = Point(record['longitude'], record['latitude'])
        boroughKey = ''
        if boroughColName in record and record[boroughColName] is not None:
            boroughKey = record[boroughColName].lower()
        
        if boroughKey in blockMapDict:
            for blockMap in blockMapDict[boroughKey]:
                if blockMap['the_geom'].contains(location):
                    if 'borough_code' not in record:
                        record['borough_code'] = blockMap['BoroCode']

                    if 'borough_name' not in record:
                        record['borough_name'] = blockMap['BoroName']

                    record['block_bctcb2020'] = blockMap['BCTCB2020']
                    record['block_geoid'] = blockMap['GEOID']

                    break
        else:
            logging.warning('not found borough %s', boroughKey)
        
        return [record]
    

class ResolveNYCTract(beam.DoFn):
    def process(self, record, tractMapDict, boroughColName):
        from shapely import Point
        import logging

        location = Point(record['longitude'], record['latitude'])
        boroughKey = ''
        if boroughColName in record and record[boroughColName] is not None:
            boroughKey = record[boroughColName].lower()

        if boroughKey in tractMapDict:
            for tractMap in tractMapDict[boroughKey]:
                if tractMap['the_geom'].contains(location):
                    if 'borough_code' not in record:
                        record['borough_code'] = tractMap['BoroCode']

                    if 'borough_name' not in record:
                        record['borough_name'] = tractMap['BoroName']

                    record['tract_bct2020'] = tractMap['BoroCT2020']
                    record['tract_geoid'] = tractMap['GEOID']
            
                    break
        else:
            logging.warning('not found borough %s', boroughKey)
        
        return [record]
    
class ResolveNYCNTA(beam.DoFn):
    def process(self, record, ntaMapDict, boroughColName):
        from shapely import Point
        import logging

        location = Point(record['longitude'], record['latitude'])
        boroughKey = ''
        if boroughColName in record and record[boroughColName] is not None:
            boroughKey = record[boroughColName].lower()

        if boroughKey in ntaMapDict:
            for tractMap in ntaMapDict[boroughKey]:
                if tractMap['the_geom'].contains(location):
                    if 'borough_code' not in record:
                        record['borough_code'] = tractMap['BoroCode']

                    if 'borough_name' not in record:
                        record['borough_name'] = tractMap['BoroName']

                    record['neighborhood_code'] = tractMap['NTA2020']
                    record['neighborhood_name'] = tractMap['NTAName']
            
                    break
        else:
            logging.warning('not found borough %s', boroughKey)
        
        return [record]

def mapWktToGeometry(element, col_name):
    from shapely import from_wkt
    element[col_name] = from_wkt(element[col_name])
    return element


class Program():

    GCLOUD_BIGQUERY_PROJECT_ID: str
    GCLOUD_BIGQUERY_DATASET: str

    data311RequestStartDate: str
    data311RequestEndDate: str

    dataRestaurantInspectionStartDate: str
    dataRestaurantInspectionEndDate: str

    destWriteDisposition: beam.io.BigQueryDisposition

    def get_batch_latest_date(self, jobName):
        bigquery_client = googleBigQuery.Client()
        query_job = bigquery_client.query('SELECT IFNULL(MAX(`latest_record_date`), DATE(1900, 1, 1)) AS `latest_record_date` \
            FROM `' + self.GCLOUD_BIGQUERY_PROJECT_ID + '.' + self.GCLOUD_BIGQUERY_DATASET + '.stage_batch_execution` \
            WHERE `job_name` = \'' + jobName + '\' \
            LIMIT 1')
        result = next(query_job.result())
        return result.latest_record_date.strftime(DATE_TIME_FORMAT)
    
    def save_batch_latest_date(self, jobName, dataTableName, recordDateColName):
        bigquery_client = googleBigQuery.Client()
        query_job = bigquery_client.query('INSERT INTO `' + self.GCLOUD_BIGQUERY_PROJECT_ID + '.' + self.GCLOUD_BIGQUERY_DATASET + '.stage_batch_execution` \
                                          (`job_name`, `latest_record_date`, `execution_date`) \
                                          SELECT \
                                            \'' + jobName + '\' AS `job_name`, \
                                            (SELECT MAX(`' + recordDateColName + '`) FROM `' + self.GCLOUD_BIGQUERY_PROJECT_ID + '.' + self.GCLOUD_BIGQUERY_DATASET + '.' + dataTableName + '`) AS `latest_record_date`, \
                                            CURRENT_DATETIME(\'America/New_York\') AS `execution_date`')
        query_job.result()


    def init_variables(self, mode):
        from datetime import datetime, timedelta
        from dateutil import tz

        utc_tz = tz.gettz('UTC')
        nyc_tz = tz.gettz('America/New_York')

        utc_now = datetime.utcnow().replace(tzinfo=utc_tz)

        one_day = timedelta(days=1)
        ny_today = utc_now.astimezone(nyc_tz).date()
        ny_yesterday = ny_today - one_day

        self.data311RequestEndDate = ny_today.strftime(DATE_TIME_FORMAT)
        self.dataRestaurantInspectionEndDate = ny_yesterday.strftime(DATE_TIME_FORMAT)
        
        if mode == 'snapshot':
            self.destWriteDisposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
            self.data311RequestStartDate = '2020-01-01 00:00:00'
            self.dataRestaurantInspectionStartDate = '2020-01-01 00:00:00'
            # testing value
            # self.data311RequestStartDate = '2023-12-03 00:00:00'
            # self.dataRestaurantInspectionStartDate = '2023-12-01 00:00:00'
        else:
            self.destWriteDisposition = beam.io.BigQueryDisposition.WRITE_APPEND
            self.data311RequestStartDate = self.get_batch_latest_date('batch_311_request_location')
            self.dataRestaurantInspectionStartDate = self.get_batch_latest_date('batch_restaurant_inspection_location')

    def main(self, argv=None):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--project',
            dest='project',
            required=True,
            help='Google BigQuery project ID')
        parser.add_argument(
            '--dataset',
            dest='dataset',
            required=True,
            help='Google BigQuery destination dataset name')
        parser.add_argument(
            '--mode',
            dest='mode',
            required=True,
            help='snapshot = refresh all the data / increment = daily increment load')
        
        known_args, pipeline_args = parser.parse_known_args(argv)
        
        self.GCLOUD_BIGQUERY_PROJECT_ID = known_args.project
        self.GCLOUD_BIGQUERY_DATASET = known_args.dataset

        self.init_variables(known_args.mode)

        beam_options = PipelineOptions(sys.argv[1:])
        
        logging.info("Extract location from 311 Request Data with created_date >= %s && < %s", self.data311RequestStartDate, self.data311RequestEndDate)
        logging.info("Extract location from Restaurant Inspection with inspection_date >= %s && < %s", self.dataRestaurantInspectionStartDate, self.dataRestaurantInspectionEndDate)

        stage_nyc_311_request_location_table_spec = bigquery.TableReference(
            projectId=self.GCLOUD_BIGQUERY_PROJECT_ID,
            datasetId=self.GCLOUD_BIGQUERY_DATASET,
            tableId='stage_nyc_311_request_location')
        
        stage_nyc_restaurant_inspection_location_table_spec = bigquery.TableReference(
            projectId=self.GCLOUD_BIGQUERY_PROJECT_ID,
            datasetId=self.GCLOUD_BIGQUERY_DATASET,
            tableId='stage_nyc_restaurant_inspection_location')
        
        stage_nyc_open_restaurant_location_table_spec = bigquery.TableReference(
            projectId=self.GCLOUD_BIGQUERY_PROJECT_ID,
            datasetId=self.GCLOUD_BIGQUERY_DATASET,
            tableId='stage_nyc_open_restaurant_location')

        with beam.Pipeline(options=beam_options) as p:
            ntaMap = (p
            | 'Read NTA boundaries' >> beam.io.ReadFromBigQuery(
                table=f'{self.GCLOUD_BIGQUERY_PROJECT_ID}:{self.GCLOUD_BIGQUERY_DATASET}.nyc_2020_neighborhood',
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            | 'Map NTA MKT to Geometry' >> beam.Map(mapWktToGeometry, col_name='the_geom')
            | 'Group NTA by borough' >> beam.GroupBy(lambda record: record['BoroName'].lower())
            )

            tractMap = (p
            | 'Read Tract boundaries' >> beam.io.ReadFromBigQuery(
                table=f'{self.GCLOUD_BIGQUERY_PROJECT_ID}:{self.GCLOUD_BIGQUERY_DATASET}.nyc_2020_census_tracts',
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            | 'Map Tract MKT to Geometry' >> beam.Map(mapWktToGeometry, col_name='the_geom')
            | 'Group Tracts by borough' >> beam.GroupBy(lambda record: record['BoroName'].lower())
            )

            blockMap = (p
            | 'Read Block boundaries' >> beam.io.ReadFromBigQuery(
                table=f'{self.GCLOUD_BIGQUERY_PROJECT_ID}:{self.GCLOUD_BIGQUERY_DATASET}.nyc_2020_census_blocks',
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            | 'Map Block MKT to Geometry' >> beam.Map(mapWktToGeometry, col_name='the_geom')
            | 'Group Blocks by borough' >> beam.GroupBy(lambda record: record['BoroName'].lower())
            )

            locationTypeCategory = (p
            | 'Read location types' >> beam.io.ReadFromBigQuery(
                table=f'{self.GCLOUD_BIGQUERY_PROJECT_ID}:{self.GCLOUD_BIGQUERY_DATASET}.nyc_service_request_location_category',
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            )

            # -------------------------------------------------------------
            # Branch 1: 311 Requests
            # -------------------------------------------------------------
            nyc311_requests_main = (p
            | 'Read 311 Requests' >> beam.io.ReadFromBigQuery(
                query="SELECT * FROM " + self.GCLOUD_BIGQUERY_DATASET + ".nyc_311_service_request WHERE created_date > CAST('{startDate}' AS DATETIME) AND created_date < CAST('{endDate}' AS DATETIME)".format(startDate=self.data311RequestStartDate, endDate=self.data311RequestEndDate),
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            )

            block_nyc311_pipeline = (nyc311_requests_main
            | 'Resolve 311 NYC Block' >> beam.ParDo(ResolveNYCBlock(), blockMapDict=beam.pvalue.AsDict(blockMap), boroughColName='borough')
            | 'Build 311 Block tuple' >> beam.Map(lambda record: (record['unique_key'], record))
            )

            tract_nyc311_pipeline = (nyc311_requests_main
            | 'Resolve 311 NYC Tract' >> beam.ParDo(ResolveNYCTract(), tractMapDict=beam.pvalue.AsDict(tractMap), boroughColName='borough')
            | 'Build 311 Tract tuple' >> beam.Map(lambda record: (record['unique_key'], record))
            )

            nta_nyc311_pipeline = (nyc311_requests_main
            | 'Resolve 311 NYC NTA' >> beam.ParDo(ResolveNYCNTA(), ntaMapDict=beam.pvalue.AsDict(ntaMap), boroughColName='borough')
            | 'Build 311 NTA tuple' >> beam.Map(lambda record: (record['unique_key'], record))
            )

            ((block_nyc311_pipeline, tract_nyc311_pipeline, nta_nyc311_pipeline)
             | 'Merge 311 location results' >> beam.CoGroupByKey()
             | 'Map 311 result to entity' >> beam.ParDo(Map311RequestResultToBigQueryRecord(), locationTypeCategoryList=beam.pvalue.AsList(locationTypeCategory))
             | 'Save to 311 location staging table' >> beam.io.WriteToBigQuery(
                stage_nyc_311_request_location_table_spec,
                schema=STAGE_311_LOCATION_TABLE_SCHEMA,
                write_disposition=self.destWriteDisposition,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            )

            # -------------------------------------------------------------
            # Branch 2: Restaurant Inspection
            # -------------------------------------------------------------
            # restaurant_inspection_main = (p
            # | 'Read Restaurant Inspection' >> beam.io.ReadFromBigQuery(
            #     query="SELECT * FROM " + self.GCLOUD_BIGQUERY_DATASET + ".nyc_restaurant_inspection WHERE inspection_date > CAST('{startDate}' AS DATETIME) AND inspection_date < CAST('{endDate}' AS DATETIME)".format(startDate=self.dataRestaurantInspectionStartDate, endDate=self.dataRestaurantInspectionEndDate),
            #     method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            # )

            # block_inspect_pipeline = (restaurant_inspection_main
            # | 'Resolve ResIns NYC Block' >> beam.ParDo(ResolveNYCBlock(), blockMapDict=beam.pvalue.AsDict(blockMap), boroughColName='boro')
            # | 'Build ResIns Block tuple' >> beam.Map(lambda record: (record['camis'], record))
            # )

            # tract_inspect_pipeline = (restaurant_inspection_main
            # | 'Resolve ResIns NYC Tract' >> beam.ParDo(ResolveNYCTract(), tractMapDict=beam.pvalue.AsDict(tractMap), boroughColName='boro')
            # | 'Build ResIns Tract tuple' >> beam.Map(lambda record: (record['camis'], record))
            # )

            # nta_inspect_pipeline = (restaurant_inspection_main
            # | 'Resolve ResIns NYC NTA' >> beam.ParDo(ResolveNYCNTA(), ntaMapDict=beam.pvalue.AsDict(ntaMap), boroughColName='boro')
            # | 'Build ResIns NTA tuple' >> beam.Map(lambda record: (record['camis'], record))
            # )

            # ((block_inspect_pipeline, tract_inspect_pipeline, nta_inspect_pipeline)
            #  | 'Merge restaurant inspection location results' >> beam.CoGroupByKey()
            #  | 'Map restaurant inspection result to entity' >> beam.ParDo(MapRestaurantInspectionResultToBigQueryRecord())
            #  | 'Save to restaurant inspection location staging table' >> beam.io.WriteToBigQuery(
            #     stage_nyc_restaurant_inspection_location_table_spec,
            #     schema=STAGE_RESTAURANT_INSPECTION_LOCATION_TABLE_SCHEMA,
            #     write_disposition=self.destWriteDisposition,
            #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            # )

            # # -------------------------------------------------------------
            # # Branch 3: Open Restaurant - Only snapshot mode
            # # -------------------------------------------------------------
            # if known_args.mode == 'snapshot':
            #     open_restaurant_main = (p
            #     | 'Read Open Restaurant' >> beam.io.ReadFromBigQuery(
            #         table=f'{self.GCLOUD_BIGQUERY_PROJECT_ID}:{self.GCLOUD_BIGQUERY_DATASET}.nyc_open_restaurant_applications_historic',
            #         method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            #     | 'Filter only record with geolocation' >> beam.Filter(lambda record: (record['latitude'] is not None) and (record['longitude'] is not None))
            #     )

            #     block_openres_pipeline = (open_restaurant_main
            #     | 'Resolve OpenRes NYC Block' >> beam.ParDo(ResolveNYCBlock(), blockMapDict=beam.pvalue.AsDict(blockMap), boroughColName='borough')
            #     | 'Build OpenRes Block tuple' >> beam.Map(lambda record: (record['objectid'], record))
            #     )

            #     tract_openres_pipeline = (open_restaurant_main
            #     | 'Resolve OpenRes NYC Tract' >> beam.ParDo(ResolveNYCTract(), tractMapDict=beam.pvalue.AsDict(tractMap), boroughColName='borough')
            #     | 'Build OpenRes Tract tuple' >> beam.Map(lambda record: (record['objectid'], record))
            #     )

            #     nta_openres_pipeline = (open_restaurant_main
            #     | 'Resolve OpenRes NYC NTA' >> beam.ParDo(ResolveNYCNTA(), ntaMapDict=beam.pvalue.AsDict(ntaMap), boroughColName='borough')
            #     | 'Build OpenRes NTA tuple' >> beam.Map(lambda record: (record['objectid'], record))
            #     )

            #     ((block_openres_pipeline, tract_openres_pipeline, nta_openres_pipeline)
            #     | 'Merge open restaurant location results' >> beam.CoGroupByKey()
            #     | 'Map open restaurant result to entity' >> beam.ParDo(MapOpenRestaurantResultToBigQueryRecord())
            #     | 'Save to open restaurant location staging table' >> beam.io.WriteToBigQuery(
            #         stage_nyc_open_restaurant_location_table_spec,
            #         schema=STAGE_OPEN_RESTAURANT_LOCATION_TABLE_SCHEMA,
            #         write_disposition=self.destWriteDisposition,
            #         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            #     )


        # Save batch execution log
        self.save_batch_latest_date('batch_311_request_location', 'nyc_311_service_request', 'created_date')
        self.save_batch_latest_date('batch_restaurant_inspection_location', 'nyc_restaurant_inspection', 'inspection_date')



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    program = Program()
    program.main()