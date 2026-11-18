import json
import boto3
import pandas as pd
from io import StringIO


s3_client = boto3.client("s3")


SOURCE_BUCKET = "bld-tseed-workshop-input-semillero-2026-q4-c"
SOURCE_KEY = "johan_suarez/users_dataset_100.csv"  # folder/archivo de entrada


OUTPUT_BUCKET = "bld-tseed-workshop-output-semillero-2026-q4-c"
OUTPUT_SUBFOLDER = "johan_suarez/users_json/"


def lambda_handler(event, context):
    response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
    body = response["Body"].read().decode("utf-8-sig")
    df = pd.read_csv(StringIO(body))

    records = json.loads(df.to_json(orient="records"))

    for record in records:
        key = f"{OUTPUT_SUBFOLDER}{record['user_id']}.json"
        s3_client.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=key,
            Body=json.dumps(record, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

    return {"statusCode": 200, "body": json.dumps({"rows": len(records)})}
