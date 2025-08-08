def simple_dedup(rows, key="id"):
    seen = set()
    out = []
    for r in rows:
        k = r.get(key)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out
