import asyncio
import json
import nats
import pathway as pw
from pathway.io.python import ConnectorSubject
import logging

logger = logging.getLogger(__name__)

class NatsSource(ConnectorSubject):
    def __init__(self, nats_url: str, subjects: list, jetstream: bool = False):
        super().__init__()
        self.nats_url = nats_url
        self.subjects = subjects
        self.jetstream = jetstream

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def main():
            nc = await nats.connect(
                self.nats_url,
                reconnect_time_wait=2,
                max_reconnect_attempts=-1,
                pending_size=128 * 1024 * 1024  # 128 MB client-side buffer
            )
            
            if self.jetstream:
                js = nc.jetstream()
                
            async def cb(msg):
                try:
                    subject = msg.subject
                    data = json.loads(msg.data.decode())
                    import time
                    self.next(subject=subject, data=data, time=int(time.time() * 1000))
                except Exception as e:
                    logger.error(f"Error processing NATS message: {e}")

            for sub in self.subjects:
                if self.jetstream:
                    # Higher limits to avoid Slow Consumer errors during bursts
                    await js.subscribe(sub, cb=cb, pending_msgs_limit=1000000, pending_bytes_limit=128*1024*1024)
                else:
                    await nc.subscribe(sub, cb=cb, pending_msgs_limit=1000000, pending_bytes_limit=128*1024*1024)
            
            # Keep alive
            while True:
                await asyncio.sleep(1)

        loop.run_until_complete(main())
