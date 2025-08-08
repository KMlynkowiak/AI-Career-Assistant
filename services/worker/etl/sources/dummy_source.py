def fetch_jobs():
    # Demo source returning a few fake postings
    return [
        {
            "id": "pl-001",
            "title": "Junior Data Scientist",
            "company": "ACME",
            "location": "Poznań",
            "description": "We need a Junior Data Scientist with Python, SQL, pandas. Nice to have: AWS, scikit-learn.",
            "source": "demo"
        },
        {
            "id": "pl-002",
            "title": "Data Engineer",
            "company": "ACME",
            "location": "Warszawa",
            "description": "Build pipelines with Python, Airflow, Spark, AWS. Docker and Kubernetes are a plus.",
            "source": "demo"
        },
        {
            "id": "pl-003",
            "title": "AI Engineer",
            "company": "StartUp XYZ",
            "location": "Remote",
            "description": "Deep learning (PyTorch/TensorFlow), ML Ops on GCP or AWS, Kubernetes, Kafka.",
            "source": "demo"
        }
    ]
