from datetime import timedelta, datetime
from bytewax.window import EventClockConfig, TumblingWindow

def get_cep_window_config(window_size_seconds: int = 1):
    """
    Returns a Bytewax TumblingWindow configuration for CEP joins.
    Aggregates events across streams that occur within the same time window.
    """
    # Define a tumbling window (e.g., 1-second chunks)
    window = TumblingWindow(
        length=timedelta(seconds=window_size_seconds),
        align_to=datetime(2023, 1, 1) # Arbitrary alignment epoch
    )
    
    # Use event time if available, otherwise fallback to system processing time.
    # For this reference architecture, we assume events have a 'timestamp' field.
    # If not present, we default to the current time.
    def get_event_time(event_data):
        if "timestamp" in event_data:
            return datetime.fromisoformat(event_data["timestamp"])
        return datetime.utcnow()

    clock = EventClockConfig(
        dt_getter=get_event_time,
        wait_for_system_duration=timedelta(seconds=0.5) # Wait 500ms for late arrivals
    )
    
    return clock, window
