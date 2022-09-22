def parse_aggregations(data: dict, depth: int = 1):
    level = data.get(str(depth), None)
    if level is None:
        return

    if 'buckets' in level:
        result = {}
        buckets = level['buckets']
        for b in buckets:
            key = b['key']
            count = b['doc_count']
            children = parse_aggregations(b, depth + 1)
            result[key] = children if children else count
        return result
    else:
        value = level['value']
        return value
