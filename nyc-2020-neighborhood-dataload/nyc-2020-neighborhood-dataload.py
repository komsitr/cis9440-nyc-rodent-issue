import geojson
from geojson import MultiPolygon
from google.cloud import bigquery
from shapely.geometry import shape

batchSize = 100

def main():

    bigquery_client = bigquery.Client()

    # This example uses a table containing a column named "geo" with the
    # GEOGRAPHY data type.
    table_id = "avid-garage-399623.cis9440_project.nyc_2020_neighborhood"

    # Delete the table
    bigquery_client.delete_table(table_id, not_found_ok=True)

    # recreate the able
    schema = [
        bigquery.SchemaField('the_geom', 'STRING'),
        bigquery.SchemaField('BoroCode', 'STRING'),
        bigquery.SchemaField('BoroName', 'STRING'),
        bigquery.SchemaField('CountyFIPS', 'STRING'),
        bigquery.SchemaField('NTA2020', 'STRING'),
        bigquery.SchemaField('NTAName', 'STRING'),
        bigquery.SchemaField('NTAAbbrev', 'STRING'),
        bigquery.SchemaField('NTAType', 'STRING'),
        bigquery.SchemaField('CDTA2020', 'STRING'),
        bigquery.SchemaField('CDTAName', 'STRING'),
        bigquery.SchemaField('Shape_Leng', 'FLOAT'),
        bigquery.SchemaField('Shape_Area', 'FLOAT')
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = bigquery_client.create_table(table)

    rows = []
    with open('2020-nyc-nta.geojson') as f:
        ntaData = geojson.load(f)
        print("Found features:", len(ntaData['features']))

    i = 1
    batchIndex = 0
    for feature in ntaData['features']:
        # Use the python-geojson library to generate GeoJSON of a line from LAX to
        # JFK airports. Alternatively, you may define GeoJSON data directly, but it
        # must be converted to a string before loading it into BigQuery.

        geo = shape(feature["geometry"])
        # format geometry coordinates as WKT
        wkt = geo.wkt

        rows.append({
            'the_geom': wkt,
            'BoroCode': feature['properties']['borocode'],
            'BoroName': feature['properties']['boroname'],
            'CountyFIPS': feature['properties']['countyfips'],
            'NTA2020': feature['properties']['nta2020'],
            'NTAName': feature['properties']['ntaname'],
            'NTAAbbrev': feature['properties']['ntaabbrev'],
            'NTAType': feature['properties']['ntatype'],
            'CDTA2020': feature['properties']['cdta2020'],
            'CDTAName': feature['properties']['cdtaname'],
            'Shape_Leng': float(feature['properties']['shape_leng']),
            'Shape_Area': float(feature['properties']['shape_area'])
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