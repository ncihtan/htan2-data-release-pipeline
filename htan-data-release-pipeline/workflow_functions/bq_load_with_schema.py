from google.cloud import bigquery

def load_bq(client, project, dataset, table, data, schema):
    '''
    Load table and schema to BigQuery
    '''

    print( 'Loading %s.%s.%s to BigQuery' 
        % (project, dataset, table))

    table_bq = '%s.%s.%s' % (project, dataset, table)
    job_config = bigquery.LoadJobConfig(
        schema=schema, 
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        source_format=bigquery.SourceFormat.CSV
    )

    job = client.load_table_from_dataframe(
        data, table_bq, job_config=job_config
    )


def get_description(attribute, schema, add_descriptions):

    try:
        dsc = schema[schema['Attribute'] == attribute]['Description'].values[0]
        description = (dsc[:1024]) if len(dsc) > 1024 else dsc

    except:
        try:
            dsc = add_descriptions[attribute]
            description = (dsc[:1024]) if len(dsc) > 1024 else dsc
        except:
            description = 'Description unavailable. Contact DCC for more information'
            print(
                '{} attribute not found in HTAN schema'.format(
                    attribute)
            )

    return description