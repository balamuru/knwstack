import pathway as pw
print(f"pw.MonitoringLevel: {hasattr(pw, 'MonitoringLevel')}")
if hasattr(pw, 'MonitoringLevel'):
    print(f"Levels: {list(pw.MonitoringLevel)}")
