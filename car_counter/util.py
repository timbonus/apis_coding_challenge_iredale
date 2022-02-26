from datetime import datetime

def line_to_record( row ):
    timestamp, data = row.split()
    datetime_timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
    return datetime_timestamp, int(data)