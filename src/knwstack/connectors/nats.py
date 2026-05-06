import asyncio
import nats
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import json
import logging
import threading
import queue
import time

logger = logging.getLogger(__name__)

class NatsSourcePartition(StatelessSourcePartition):
    """
    A single partition reading from a NATS JetStream subject.
    Runs a dedicated event loop in a background thread to maintain
    NATS connection stability and high-performance ingestion.
    """
    def __init__(self, nats_url: str, subject: str, queue_group: str):
        self.nats_url = nats_url
        self.subject = subject
        self.queue_group = queue_group
        
        self.msg_queue = queue.Queue()
        self.should_exit = threading.Event()
        
        # Start the background thread
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        
        # Wait for connection to be established
        start_time = time.time()
        while self.thread.is_alive() and not hasattr(self, 'connected'):
            if time.time() - start_time > 10:
                raise RuntimeError("Failed to connect to NATS within 10 seconds")
            time.sleep(0.1)

    def _run_loop(self):
        """Thread entry point: runs the asyncio loop."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self._worker())
        except Exception as e:
            logger.error(f"NATS Thread Error: {e}")
        finally:
            loop.close()

    async def _worker(self):
        """Async worker: maintains connection and pulls messages."""
        logger.info(f"NatsSource: Connecting to {self.nats_url}...")
        nc = await nats.connect(self.nats_url)
        js = nc.jetstream()
        
        logger.info(f"NatsSource: Subscribing to '{self.subject}' with durable '{self.queue_group}'")
        sub = await js.pull_subscribe(self.subject, durable=self.queue_group)
        
        self.connected = True
        logger.info("NatsSource: Connected and subscribed.")
        
        try:
            while not self.should_exit.is_set():
                try:
                    # Pull messages in batches of 10
                    # We use a 1s timeout here to check should_exit frequently
                    msgs = await sub.fetch(batch=10, timeout=1.0)
                    for msg in msgs:
                        await msg.ack()
                        data = json.loads(msg.data.decode())
                        self.msg_queue.put((msg.subject, data))
                except TimeoutError:
                    # No messages, just loop and check should_exit
                    continue
                except Exception as e:
                    logger.error(f"Error fetching from NATS: {e}")
                    await asyncio.sleep(1)
        finally:
            await nc.close()

    def next_batch(self):
        """Called by Bytewax to fetch the next batch of events."""
        batch = []
        
        # Drain the queue into a batch (up to 100 items to avoid blocking too long)
        try:
            while len(batch) < 100:
                item = self.msg_queue.get_nowait()
                if not item[0].startswith("knwstack.internal."):
                    logger.info(f"NatsSource: Received event on '{item[0]}'")
                batch.append(item)
        except queue.Empty:
            pass
            
        # If no messages, return a heartbeat to keep Bytewax windows moving
        if not batch:
            batch.append(("knwstack.internal.heartbeat", {}))
            
        return batch

    def close(self):
        self.should_exit.set()
        if self.thread.is_alive():
            self.thread.join(timeout=2.0)

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
        if items:
            logger.info(f"NatsSink: Writing {len(items)} items to NATS")
        for subject, data in items:
            payload = json.dumps(data).encode()
            self.loop.run_until_complete(self.nc.publish(subject, payload))
        if items:
            self.loop.run_until_complete(self.nc.flush())

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
