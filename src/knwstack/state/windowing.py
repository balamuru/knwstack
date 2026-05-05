from datetime import timedelta, datetime, timezone
from bytewax.operators.windowing import SystemClock, TumblingWindower

def get_cep_window_config(window_size_seconds: int = 1):
    """
    Returns a Bytewax TumblingWindower configuration for CEP joins.
    Aggregates events across streams that occur within the same time window.
    """
    # Define a tumbling window (e.g., 1-second chunks)
    # Ensure align_to is timezone-aware as required by Bytewax 0.21+
    window = TumblingWindower(
        length=timedelta(seconds=window_size_seconds),
        align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
    )
    
    # Use SystemClock for the reference architecture to ensure 
    # reliable window closing based on processing time.
    clock = SystemClock()
    
    return clock, window


