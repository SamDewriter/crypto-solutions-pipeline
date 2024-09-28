from apache_beam.options.pipeline_options import PipelineOptions
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

