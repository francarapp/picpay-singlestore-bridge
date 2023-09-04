
Meta = {
    "houston" : {
        "partitions": ["event", "year", "month", "day"]
    },
    "track" : {
        "partitions": ["event", "year", "month", "day"]
    },
    "page" : {
        "partitions": ["year", "month", "day"]
    }
}

def partitionby(event, values):
    part = Meta[event]
    if part is None:
        return []
    return " and ".join([f"{e[0]}={e[1]}" for e in zip(part['partitions'], values)])
