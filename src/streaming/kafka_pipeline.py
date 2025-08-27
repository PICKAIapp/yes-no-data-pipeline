"""Real-time data streaming pipeline using Kafka."""

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
import asyncio

@dataclass
class MarketEvent:
    """Market event data structure."""
    event_id: str
    timestamp: float
    market_id: str
    event_type: str
    data: Dict[str, Any]

class YesNoDataPipeline:
    """High-throughput data pipeline for yes-no markets."""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumer = None
        self.logger = logging.getLogger(__name__)
        
    def initialize_producer(self):
        """Initialize Kafka producer."""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='snappy',
            batch_size=16384,
            linger_ms=10
        )
    
    async def process_event(self, event: MarketEvent) -> Dict[str, Any]:
        """Process incoming market event."""
        # Apply transformations
        processed = {
            "event_id": event.event_id,
            "timestamp": event.timestamp,
            "market_id": event.market_id,
            "processed_at": asyncio.get_event_loop().time(),
            "features": self._extract_features(event.data)
        }
        
        # Send to downstream topics
        await self._send_to_topic("processed_events", processed)
        return processed
    
    def _extract_features(self, data: Dict) -> Dict:
        """Extract features from raw event data."""
        return {
            "volume": data.get("volume", 0),
            "price": data.get("price", 0),
            "liquidity": data.get("liquidity", 0),
            "sentiment": data.get("sentiment", 0)
        }
    
    async def _send_to_topic(self, topic: str, data: Dict):
        """Send data to Kafka topic."""
        future = self.producer.send(topic, value=data)
        try:
            await asyncio.get_event_loop().run_in_executor(None, future.get, 10)
        except KafkaError as e:
            self.logger.error(f"Failed to send to {topic}: {e}")
