from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import json
import apache_beam as beam


PROJECT_ID = "crypto-solutions-437016"
SUBSCRIPTION_ID = "test-sub"

SCHEMA = {
    'fields': [
        {'name': 'symbol', 
         'type': 'STRING', 
         'mode': 'NULLABLE'},

        {'name': 'price', 
         'type': 'FLOAT', 
         'mode': 'NULLABLE'},

        {'name': 'high_price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'low_price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'volume', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ]
}

def transform_data(message):
    data = json.loads(message)

    transform_data = {
        'symbol': data['s'],
        'price': float(data['p']),
        'price_change': float(data['c']),
        'high_price': float(data['h']),
        'low_price': float(data['l']),
        'volume': float(data['v']),
        'timestamp': data['E']
    }

    return transform_data


def run():
    # Define your pipeline options
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = 'cryptonexus-436910'
    gcp_options.job_name = 'binance-pubsub-transform'
    gcp_options.temp_location = 'gs://crypto-bucket-tics/temp'  # Replace with your GCS bucket

    # Create the Apache Beam pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription='projects/cryptonexus-436910/subscriptions/crypto-sub')
            | 'TransformMessages' >> beam.Map(transform_binance_message)
            | 'Window' >> beam.WindowInto(beam.window.FixedWindows(300))
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                table='crypto_data.crypto_stream',
                schema=SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    run()