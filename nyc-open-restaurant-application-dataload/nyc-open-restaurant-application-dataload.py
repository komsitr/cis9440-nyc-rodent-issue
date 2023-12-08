import csv
from google.cloud import bigquery

batchSize = 100

def main():

    bigquery_client = bigquery.Client()

    table_id = "avid-garage-399623.cis9440_project.nyc_open_restaurant_applications_historic"

    # Delete the table
    bigquery_client.delete_table(table_id, not_found_ok=True)

    # recreate the able
    schema = [
        bigquery.SchemaField('objectid', 'STRING'),
        bigquery.SchemaField('globalid', 'STRING'),
        bigquery.SchemaField('seating_interest_sidewalk', 'STRING'),
        bigquery.SchemaField('restaurant_name', 'STRING'),
        bigquery.SchemaField('legal_business_name', 'STRING'),
        bigquery.SchemaField('doing_business_as_dba', 'STRING'),
        bigquery.SchemaField('bulding_number', 'STRING'),
        bigquery.SchemaField('street', 'STRING'),
        bigquery.SchemaField('borough', 'STRING'),
        bigquery.SchemaField('zip', 'STRING'),
        bigquery.SchemaField('business_address', 'STRING'),
        bigquery.SchemaField('food_service_establishment', 'STRING'),
        bigquery.SchemaField('sidewalk_dimensions_length', 'INTEGER'),
        bigquery.SchemaField('sidewalk_dimensions_width', 'INTEGER'),
        bigquery.SchemaField('sidewalk_dimensions_area', 'INTEGER'),
        bigquery.SchemaField('roadway_dimensions_length', 'INTEGER'),
        bigquery.SchemaField('roadway_dimensions_width', 'INTEGER'),
        bigquery.SchemaField('roadway_dimensions_area', 'INTEGER'),
        bigquery.SchemaField('approved_for_sidewalk_seating', 'STRING'),
        bigquery.SchemaField('approved_for_roadway_seating', 'STRING'),
        bigquery.SchemaField('qualify_alcohol', 'STRING'),
        bigquery.SchemaField('sla_serial_number', 'STRING'),
        bigquery.SchemaField('sla_license_type', 'STRING'),
        bigquery.SchemaField('landmark_district_or_building', 'STRING'),
        bigquery.SchemaField('landmarkdistrict_terms', 'STRING'),
        bigquery.SchemaField('healthcompliance_terms', 'STRING'),
        bigquery.SchemaField('time_of_submission', 'DATETIME'),
        bigquery.SchemaField('latitude', 'FLOAT'),
        bigquery.SchemaField('longitude', 'FLOAT'),
        bigquery.SchemaField('community_board', 'STRING'),
        bigquery.SchemaField('council_district', 'STRING'),
        bigquery.SchemaField('census_tract', 'STRING'),
        bigquery.SchemaField('bin', 'STRING'),
        bigquery.SchemaField('bbl', 'STRING'),
        bigquery.SchemaField('nta', 'STRING')
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = bigquery_client.create_table(table)

    rows = []
    with open('Open_Restaurant_Applications__Historic__20231204.csv') as f:
        csv_reader = csv.DictReader(f, delimiter=',')

        i = 1
        batchIndex = 0
        for row in csv_reader:
            rows.append({
                'objectid': get_string_value(row, 'objectid'),
                'globalid': get_string_value(row, 'globalid'),
                'seating_interest_sidewalk': get_string_value(row, 'seating_interest_sidewalk'),
                'restaurant_name': get_string_value(row, 'restaurant_name'),
                'legal_business_name': get_string_value(row, 'legal_business_name'),
                'doing_business_as_dba': get_string_value(row, 'doing_business_as_dba'),
                'bulding_number': get_string_value(row, 'bulding_number'),
                'street': get_string_value(row, 'street'),
                'borough': get_string_value(row, 'borough'),
                'zip': get_string_value(row, 'zip'),
                'business_address': get_string_value(row, 'business_address'),
                'food_service_establishment': get_string_value(row, 'food_service_establishment'),
                'sidewalk_dimensions_length': get_int_value(row, 'sidewalk_dimensions_length'),
                'sidewalk_dimensions_width': get_int_value(row, 'sidewalk_dimensions_width'),
                'sidewalk_dimensions_area': get_int_value(row, 'sidewalk_dimensions_area'),
                'roadway_dimensions_length': get_int_value(row, 'roadway_dimensions_length'),
                'roadway_dimensions_width': get_int_value(row, 'roadway_dimensions_width'),
                'roadway_dimensions_area': get_int_value(row, 'roadway_dimensions_area'),
                'approved_for_sidewalk_seating': get_string_value(row, 'approved_for_sidewalk_seating'),
                'approved_for_roadway_seating': get_string_value(row, 'approved_for_roadway_seating'),
                'qualify_alcohol': get_string_value(row, 'qualify_alcohol'),
                'sla_serial_number': get_string_value(row, 'sla_serial_number'),
                'sla_license_type': get_string_value(row, 'sla_license_type'),
                'landmark_district_or_building': get_string_value(row, 'landmark_district_or_building'),
                'landmarkdistrict_terms': get_string_value(row, 'landmarkdistrict_terms'),
                'healthcompliance_terms': get_string_value(row, 'healthcompliance_terms'),
                'time_of_submission': get_date_value(row, 'time_of_submission'),
                'latitude': get_float_value(row, 'latitude'),
                'longitude': get_float_value(row, 'longitude'),
                'community_board': get_string_value(row, 'community_board'),
                'council_district': get_string_value(row, 'council_district'),
                'census_tract': get_string_value(row, 'census_tract'),
                'bin': get_string_value(row, 'bin'),
                'bbl': get_string_value(row, 'bbl'),
                'nta': get_string_value(row, 'nta'),
            })

            if i % batchSize == 0:
                batchIndex = batchIndex + 1
                writeToBigQuery(bigquery_client, table_id, rows, batchIndex)
                rows = []

            i = i + 1

        # Write the remaining items if any
        batchIndex = batchIndex + 1
        writeToBigQuery(bigquery_client, table_id, rows, batchIndex)
        rows = []

def get_string_value(record, key):
    if key in record and record[key] is not None:
        return record[key]
    else:
        return None
    
def get_date_value(record, key):
    from datetime import datetime

    if key in record and record[key] is not None:
        #08/02/2023 03:40:00 PM
        return datetime.strptime(record[key], '%m/%d/%Y %I:%M:%S %p').isoformat()
    else:
        return None
    
def get_float_value(record, key):
    if key in record and record[key] is not None:
        try:
            return float(record[key])
        except:
            return None
    else:
        return None

def get_int_value(record, key):
    if key in record and record[key] is not None:
        try:
            return int(record[key])
        except:
            return None
    else:
        return None

def writeToBigQuery(bigquery_client: bigquery.Client, table_id, rows, batchIndex):
    if len(rows) <= 0:
        return

    errors = bigquery_client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"insert batch index {batchIndex} failed: {errors}")
    else:
        print(f"Batch index {batchIndex} success")

# ---------------------------------------------

main()