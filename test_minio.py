from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="admin12345",
    secure=False
)

buckets = client.list_buckets()

for bucket in buckets:
    print(bucket.name)  