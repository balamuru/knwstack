import asyncio
import nats
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import json
import logging

logger = logging.getLogger(__name__)

class NatsSourcePartition(StatelessSourcePartition):
    """
    A single partition reading from a NATS JetStream subject.
    In a real cluster, NATS Queue Groups will automatically balance 
    the load across multiple Bytewax workers.
    """
    def __init__(self, nats_url: str, subject: str, queue_group: str):
        self.nats_url = nats_url
        self.subject = subject
        self.queue_group = queue_group
        self.nc = None
        self.js = None
        self.sub = None
        
        # We run the async NATS client in its own event loop 
        # since Bytewax calls this synchronously
        self.loop = asyncio.new_event_loop()
        self.loop.run_until_complete(self._connect())

    async def _connect(self):
        logger.info(f"NatsSource: Connecting to {self.nats_url}...")
        self.nc = await nats.connect(self.nats_url)
        self.js = self.nc.jetstream()
        # Subscribe using a pull subscription for efficient batching and multi-tenant load balancing
        logger.info(f"NatsSource: Subscribing to '{self.subject}' with durable '{self.queue_group}'")
        self.sub = await self.js.pull_subscribe(self.subject, durable=self.queue_group)
        logger.info("NatsSource: Connected and subscribed.")

    def next_batch(self):
        """Called by Bytewax to fetch the next batch of events."""
        batch = []
        try:
            # Try to fetch up to 10 messages, timeout quickly to not block the engine
            msgs = self.loop.run_until_complete(self.sub.fetch(batch=10, timeout=0.1))
            for msg in msgs:
                # Acknowledge the JetStream message
                self.loop.run_until_complete(msg.ack())
                # Parse JSON payload
                data = json.loads(msg.data.decode())
                # Yield tuple of (routing_key, event_data)
                logger.debug(f"NatsSource: Received event on '{msg.subject}'")
                batch.append((msg.subject, data))
        except TimeoutError:
            pass # No new messages
        except Exception as e:
            logger.error(f"Error fetching from NATS: {e}")
            
        return batch

    def close(self):
        self.loop.run_until_complete(self.nc.close())
        self.loop.close()

class NatsSource(DynamicSource):
    """
    Bytewax Input Connector for NATS JetStream.
    """
    def __init__(self, nats_url: str, subject: str, queue_group: str = "knwstack_workers"):
        self.nats_url = nats_url
        self.subject = subject
        self.queue_group = queue_group

    def build(self, step_id, worker_index, worker_count):
        return NatsSourcePartition(self.nats_url, self.subject, self.queue_group)

class NatsSinkPartition(StatelessSinkPartition):
    """Writes actions/events back to NATS."""
    def __init__(self, nats_url: str):
        self.nats_url = nats_url
        self.nc = None
        self.loop = asyncio.new_event_loop()
        self.loop.run_until_complete(self._connect())

    async def _connect(self):
        self.nc = await nats.connect(self.nats_url)

    def write_batch(self, items):
        for subject, data in items:
            payload = json.dumps(data).encode()
            self.loop.run_until_complete(self.nc.publish(subject, payload))

    def close(self):
        self.loop.run_until_complete(self.nc.close())
        self.loop.close()

class NatsSink(DynamicSink):
    """
    Bytewax Output Connector for NATS JetStream.
    """
    def __init__(self, nats_url: str):
        self.nats_url = nats_url

    def build(self, step_id, worker_index, worker_count):
        return NatsSinkPartition(self.nats_url)
