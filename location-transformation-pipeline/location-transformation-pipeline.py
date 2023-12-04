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

class MapResultToBigQueryRecord(beam.DoFn):
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
    
class ResolveNYCBlock(beam.DoFn):
    def process(self, record, blockMapDict):
        from shapely import Point
        import logging

        location = Point(record['longitude'], record['latitude'])
        boroughKey = record['borough'].lower()
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
    def process(self, record, tractMapDict):
        from shapely import Point
        import logging

        location = Point(record['longitude'], record['latitude'])
        boroughKey = record['borough'].lower()
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
    def process(self, record, ntaMapDict):
        from shapely import Point
        import logging

        location = Point(record['longitude'], record['latitude'])
        boroughKey = record['borough'].lower()
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

    dataEndDate: str
    destWriteDisposition: beam.io.BigQueryDisposition

    def init_variables(self, mode):
        from datetime import datetime, timedelta
        from dateutil import tz

        utc_tz = tz.gettz('UTC')
        nyc_tz = tz.gettz('America/New_York')

        utc_now = datetime.utcnow().replace(tzinfo=utc_tz)

        ny_today = utc_now.astimezone(nyc_tz).date()

        self.dataEndDate = ny_today.strftime(DATE_TIME_FORMAT)
        
        if mode == 'snapshot':
            self.destWriteDisposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
            #self.data311RequestStartDate = '1900-01-01T00:00:00'
            self.data311RequestStartDate = '2023-12-03T00:00:00'
        else:
            self.destWriteDisposition = beam.io.BigQueryDisposition.WRITE_APPEND
            bigquery_client = googleBigQuery.Client()
            query_job = bigquery_client.query('SELECT IFNULL(MAX(`latest_record_date`), DATE(1900, 1, 1)) AS `latest_record_date` \
                FROM `' + self.GCLOUD_BIGQUERY_PROJECT_ID + '.' + self.GCLOUD_BIGQUERY_DATASET + '.stage_batch_execution` \
                WHERE `job_name` = \'batch_311_request_location\' \
                LIMIT 1')
            result = next(query_job.result())
            self.data311RequestStartDate = result.latest_record_date.strftime(DATE_TIME_FORMAT)

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
        
        logging.info("Extract location from 311 Request Data with created_date >= %s && < %s", self.data311RequestStartDate, self.dataEndDate)

        stage_nyc_311_request_location_table_spec = bigquery.TableReference(
            projectId=self.GCLOUD_BIGQUERY_PROJECT_ID,
            datasetId=self.GCLOUD_BIGQUERY_DATASET,
            tableId='stage_nyc_311_request_location')

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

            main = (p
            | 'Set 311 Request Date Range' >> beam.io.ReadFromBigQuery(
                query="SELECT * FROM " + self.GCLOUD_BIGQUERY_DATASET + ".nyc_311_service_request WHERE created_date > CAST('{startDate}' AS DATETIME) AND created_date < CAST('{endDate}' AS DATETIME)".format(startDate=self.data311RequestStartDate, endDate=self.dataEndDate),
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            )

            block_pipeline = (main
            | 'Resolve NYC Block' >> beam.ParDo(ResolveNYCBlock(), blockMapDict=beam.pvalue.AsDict(blockMap))
            | 'Build Block tuple' >> beam.Map(lambda record: (record['unique_key'], record))
            )

            tract_pipeline = (main
            | 'Resolve NYC Tract' >> beam.ParDo(ResolveNYCTract(), tractMapDict=beam.pvalue.AsDict(tractMap))
            | 'Build Tract tuple' >> beam.Map(lambda record: (record['unique_key'], record))
            )

            nta_pipeline = (main
            | 'Resolve NYC NTA' >> beam.ParDo(ResolveNYCNTA(), ntaMapDict=beam.pvalue.AsDict(ntaMap))
            | 'Build NTA tuple' >> beam.Map(lambda record: (record['unique_key'], record))
            )

            ((block_pipeline, tract_pipeline, nta_pipeline)
             | 'Merge 311 location results' >> beam.CoGroupByKey()
             | 'Map 311 result to entity' >> beam.ParDo(MapResultToBigQueryRecord(), locationTypeCategoryList=beam.pvalue.AsList(locationTypeCategory))
             | 'Save to 311 location staging table' >> beam.io.WriteToBigQuery(
                stage_nyc_311_request_location_table_spec,
                schema=STAGE_311_LOCATION_TABLE_SCHEMA,
                write_disposition=self.destWriteDisposition,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            )

            # -------------------------------------------------------------
            # Branch 2: Restaurant Inspection
            # -------------------------------------------------------------




            # -------------------------------------------------------------
            # Branch 3: Open Restaurant - only snapshot mode
            # -------------------------------------------------------------
            if known_args.mode == 'snapshot':
                pass


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    program = Program()
    program.main()