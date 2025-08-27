"""
PICKAI Real-time Data Pipeline
High-throughput stream processing with Apache Kafka and Apache Beam
"""

from apache_beam import Pipeline, PTransform, DoFn, window
from apache_beam.io import ReadFromKafka, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from confluent_kafka import Producer, Consumer, KafkaError
from typing import Dict, List, Optional, Tuple
import apache_beam as beam
import json
import numpy as np
from datetime import datetime, timedelta
import hashlib


class MarketEventProcessor(DoFn):
    """
    Process market events with deduplication, validation, and enrichment
    """
    
    def __init__(self):
        self.processed_events = set()
        self.metrics_namespace = 'pickai.pipeline'
        
    def setup(self):
        """Initialize connections and caches"""
        self.redis_client = self._init_redis()
        self.feature_store = self._init_feature_store()
        
    def process(self, element):
        """
        Process individual market event
        """
        try:
            # Parse event
            event = json.loads(element.value.decode('utf-8'))
            event_id = self._generate_event_id(event)
            
            # Deduplication
            if event_id in self.processed_events:
                self.inc_counter('duplicates')
                return
            
            # Validation
            if not self._validate_event(event):
                self.inc_counter('invalid_events')
                return
                
            # Enrichment
            enriched = self._enrich_event(event)
            
            # Feature extraction
            features = self._extract_features(enriched)
            
            # Add metadata
            enriched['processing_timestamp'] = datetime.utcnow().isoformat()
            enriched['features'] = features
            enriched['event_id'] = event_id
            
            self.processed_events.add(event_id)
            self.inc_counter('processed_events')
            
            yield enriched
            
        except Exception as e:
            self.inc_counter('processing_errors')
            logging.error(f"Processing error: {e}")
            
    def _generate_event_id(self, event: Dict) -> str:
        """Generate unique event ID"""
        content = f"{event.get('market_id')}_{event.get('timestamp')}_{event.get('type')}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
        
    def _validate_event(self, event: Dict) -> bool:
        """Validate event schema and data quality"""
        required_fields = ['market_id', 'timestamp', 'type', 'data']
        
        # Check required fields
        if not all(field in event for field in required_fields):
            return False
            
        # Validate timestamp
        try:
            ts = datetime.fromisoformat(event['timestamp'])
            if ts > datetime.utcnow() + timedelta(minutes=5):
                return False  # Future timestamp
        except:
            return False
            
        # Validate data types
        if not isinstance(event['data'], dict):
            return False
            
        return True
        
    def _enrich_event(self, event: Dict) -> Dict:
        """Enrich event with additional context"""
        enriched = event.copy()
        
        # Add market metadata
        market_id = event['market_id']
        market_meta = self._fetch_market_metadata(market_id)
        enriched['market_metadata'] = market_meta
        
        # Add historical context
        historical = self._fetch_historical_data(market_id)
        enriched['historical_stats'] = {
            'avg_volume_24h': historical.get('avg_volume'),
            'volatility_7d': historical.get('volatility'),
            'total_trades': historical.get('trade_count')
        }
        
        # Add real-time sentiment
        sentiment = self._fetch_sentiment_data(market_id)
        enriched['sentiment'] = sentiment
        
        return enriched
        
    def _extract_features(self, event: Dict) -> Dict:
        """Extract ML features from event"""
        features = {}
        
        # Price features
        if 'price' in event['data']:
            price = event['data']['price']
            features['price'] = price
            features['log_price'] = np.log(price) if price > 0 else 0
            
        # Volume features
        if 'volume' in event['data']:
            volume = event['data']['volume']
            features['volume'] = volume
            features['volume_zscore'] = self._calculate_zscore(volume)
            
        # Time features
        ts = datetime.fromisoformat(event['timestamp'])
        features['hour_of_day'] = ts.hour
        features['day_of_week'] = ts.weekday()
        features['is_weekend'] = ts.weekday() >= 5
        
        # Market features
        if 'market_metadata' in event:
            meta = event['market_metadata']
            features['market_category'] = meta.get('category', 'unknown')
            features['days_until_resolution'] = meta.get('days_until_resolution', -1)
            
        return features


class AggregationTransform(PTransform):
    """
    Custom aggregation for market metrics
    """
    
    def expand(self, pcoll):
        return (
            pcoll
            | 'Window' >> beam.WindowInto(
                window.FixedWindows(60),  # 1-minute windows
                trigger=window.AfterWatermark(early_firing=window.AfterProcessingTime(10)),
                accumulation_mode=window.AccumulationMode.ACCUMULATING
            )
            | 'GroupByMarket' >> beam.GroupBy(lambda x: x['market_id'])
            | 'ComputeMetrics' >> beam.ParDo(MetricsComputation())
        )


class MetricsComputation(DoFn):
    """
    Compute real-time market metrics
    """
    
    def process(self, element):
        market_id, events = element
        events_list = list(events)
        
        metrics = {
            'market_id': market_id,
            'timestamp': datetime.utcnow().isoformat(),
            'event_count': len(events_list),
            'unique_users': len(set(e.get('user_id') for e in events_list if 'user_id' in e)),
        }
        
        # Price metrics
        prices = [e['data'].get('price') for e in events_list if 'price' in e.get('data', {})]
        if prices:
            metrics['price_mean'] = np.mean(prices)
            metrics['price_std'] = np.std(prices)
            metrics['price_min'] = np.min(prices)
            metrics['price_max'] = np.max(prices)
            
        # Volume metrics
        volumes = [e['data'].get('volume') for e in events_list if 'volume' in e.get('data', {})]
        if volumes:
            metrics['volume_total'] = np.sum(volumes)
            metrics['volume_mean'] = np.mean(volumes)
            
        # Sentiment metrics
        sentiments = [e.get('sentiment', {}).get('score') for e in events_list]
        sentiments = [s for s in sentiments if s is not None]
        if sentiments:
            metrics['sentiment_mean'] = np.mean(sentiments)
            metrics['sentiment_std'] = np.std(sentiments)
            
        yield metrics


class AnomalyDetection(DoFn):
    """
    Real-time anomaly detection using statistical methods
    """
    
    def __init__(self, sensitivity: float = 3.0):
        self.sensitivity = sensitivity
        self.historical_stats = {}
        
    def process(self, element):
        market_id = element['market_id']
        
        # Initialize historical stats if needed
        if market_id not in self.historical_stats:
            self.historical_stats[market_id] = {
                'prices': [],
                'volumes': [],
                'rolling_mean': 0,
                'rolling_std': 1
            }
            
        stats = self.historical_stats[market_id]
        
        # Check for anomalies
        anomalies = []
        
        # Price anomaly
        if 'price_mean' in element:
            price = element['price_mean']
            if stats['prices']:
                z_score = abs((price - stats['rolling_mean']) / (stats['rolling_std'] + 1e-6))
                if z_score > self.sensitivity:
                    anomalies.append({
                        'type': 'price_spike',
                        'severity': min(z_score / self.sensitivity, 5.0),
                        'value': price,
                        'z_score': z_score
                    })
                    
        # Volume anomaly
        if 'volume_total' in element:
            volume = element['volume_total']
            if stats['volumes']:
                avg_volume = np.mean(stats['volumes'][-20:])
                if volume > avg_volume * 5:
                    anomalies.append({
                        'type': 'volume_spike',
                        'severity': min(volume / avg_volume / 5, 5.0),
                        'value': volume,
                        'ratio': volume / avg_volume
                    })
                    
        # Update historical stats
        if 'price_mean' in element:
            stats['prices'].append(element['price_mean'])
            stats['prices'] = stats['prices'][-100:]  # Keep last 100
            stats['rolling_mean'] = np.mean(stats['prices'])
            stats['rolling_std'] = np.std(stats['prices'])
            
        if 'volume_total' in element:
            stats['volumes'].append(element['volume_total'])
            stats['volumes'] = stats['volumes'][-100:]
            
        # Add anomalies to element
        if anomalies:
            element['anomalies'] = anomalies
            yield element


def run_pipeline():
    """
    Main pipeline execution
    """
    
    options = PipelineOptions([
        '--runner=DataflowRunner',
        '--project=pickai-production',
        '--region=us-central1',
        '--temp_location=gs://pickai-temp/dataflow',
        '--staging_location=gs://pickai-staging/dataflow',
        '--streaming',
        '--enable_streaming_engine',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--max_num_workers=100',
    ])
    
    with Pipeline(options=options) as pipeline:
        
        # Read from Kafka
        events = (
            pipeline
            | 'ReadFromKafka' >> ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': 'kafka-cluster:9092',
                    'group.id': 'pickai-pipeline',
                    'auto.offset.reset': 'latest'
                },
                topics=['market-events', 'user-actions', 'oracle-updates']
            )
        )
        
        # Process events
        processed = (
            events
            | 'ProcessEvents' >> beam.ParDo(MarketEventProcessor())
            | 'FilterValid' >> beam.Filter(lambda x: x is not None)
        )
        
        # Branch 1: Real-time aggregation
        aggregated = (
            processed
            | 'Aggregate' >> AggregationTransform()
        )
        
        # Branch 2: Anomaly detection
        anomalies = (
            aggregated
            | 'DetectAnomalies' >> beam.ParDo(AnomalyDetection())
            | 'FilterAnomalies' >> beam.Filter(lambda x: 'anomalies' in x)
        )
        
        # Branch 3: Feature store update
        features = (
            processed
            | 'ExtractFeatures' >> beam.Map(lambda x: x['features'])
            | 'WriteToFeatureStore' >> beam.ParDo(FeatureStoreWriter())
        )
        
        # Write to BigQuery
        processed | 'WriteToBigQuery' >> WriteToBigQuery(
            table='pickai-production:warehouse.market_events',
            schema='SCHEMA_AUTODETECT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        
        # Write anomalies to alerting system
        anomalies | 'AlertAnomalies' >> beam.ParDo(AlertingSystem())
        
        # Write metrics to monitoring
        aggregated | 'WriteMetrics' >> beam.ParDo(MetricsWriter())


if __name__ == '__main__':
    run_pipeline()
