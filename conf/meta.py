
Meta = {
    "houston" : {
        "partitions": ["event", "year", "month", "day"]
    },
    "track" : {
        "partitions": ["event", "year", "month", "day"]
    },
    "page" : {
        "partitions": ["year", "month", "day"]
    },
    "identify" : {
        "partitions": ["year", "month", "day"]
    },
    "alias" : {
        "partitions": ["year", "month", "day"]
    }
}

def partitionby(event, values):
    part = Meta[event]
    if part is None:
        return []
    return " and ".join([f"{e[0]}={e[1]}" for e in zip(part['partitions'], values)])

def partitionEvName(event, values):
    part = Meta[event]
    if part is None:
        return event
    
    if part["partitions"][0] == "event":
        return values[0]
    
    return event
    