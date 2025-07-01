from google.cloud import bigquery

def load_bq(client, project, dataset, table, data):
    '''
    Load dataframe to BigQuery
    '''
    print('Loading: '+dataset+'.'+table)
    
    table_bq = '%s.%s.%s' % (project, dataset, table)

    # make column names bq friendly
    data.columns = data.columns.str.replace(
       '[^0-9a-zA-Z]+','_', regex=True
    )
    schema = [
       bigquery.SchemaField(name, 'STRING') for name in data.columns
    ]

    job_config = bigquery.LoadJobConfig( 
        write_disposition="WRITE_TRUNCATE",      
        autodetect=False,
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        allow_jagged_rows=True,
        allow_quoted_newlines=True
    )
    
    job = client.load_table_from_dataframe(
        data, table_bq, job_config=job_config
    )
