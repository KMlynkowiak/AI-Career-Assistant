from services.worker.etl.dedup import simple_dedup

def test_simple_dedup():
    rows = [
        {"id":"1","title":"Data Scientist","company":"ACME","location":"Poznań","desc":"Python, SQL"},
        {"id":"1","title":"Data Scientist","company":"ACME","location":"Poznań","desc":"Python, SQL"},
        {"id":"2","title":"Data Engineer","company":"ACME","location":"Warszawa","desc":"AWS, Python"}
    ]
    out = simple_dedup(rows, key="id")
    assert len(out) == 2
