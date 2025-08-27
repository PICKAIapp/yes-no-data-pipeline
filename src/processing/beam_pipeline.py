"""Apache Beam pipeline for batch processing."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
import json

class ExtractFeatures(beam.DoFn):
    """Extract features from market data."""
    
    def process(self, element):
        data = json.loads(element)
        features = {
            'market_id': data['market_id'],
            'volume': data.get('volume', 0),
            'price': data.get('price', 0),
            'timestamp': data['timestamp']
        }
        yield features

def run_pipeline():
    """Run the Apache Beam pipeline."""
    options = PipelineOptions([
        '--runner=DirectRunner',
        '--streaming'
    ])
    
    with beam.Pipeline(options=options) as p:
        (p
         | 'Read from Kafka' >> beam.io.ReadFromKafka(
             consumer_config={'bootstrap.servers': 'localhost:9092'},
             topics=['market_events']
         )
         | 'Extract Features' >> beam.ParDo(ExtractFeatures())
         | 'Window' >> beam.WindowInto(window.FixedWindows(60))
         | 'Aggregate' >> beam.CombinePerKey(sum)
         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             'yes-no-analytics.market_data.aggregated',
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
         ))

if __name__ == '__main__':
    run_pipeline()
