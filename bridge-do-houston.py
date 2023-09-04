from .bridge import Bridge
from datetime import datetime, timedelta

dttm = datetime.now() - timedelta(hours=1)
Bridge("houston", "houston", dttm.strftime("%Y-%m-%d %H:%M:%S.%f")[:23])
