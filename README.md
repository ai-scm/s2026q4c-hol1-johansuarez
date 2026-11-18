# HandsOnLab1 - Johan Suarez

## Resumen

Pipeline de datos tipo **ETL** sobre AWS: un usuario carga un archivo `.csv` en un bucket de entrada, una función **Lambda** lo lee y genera **un archivo JSON por cada fila** (100 JSON a partir de `users_dataset_100.csv`), y los guarda en un bucket de salida. Luego, **Glue** cataloga esos JSON y **Athena** permite consultarlos con SQL.

## Flujo

```text
  Usuario
     │  sube users_dataset_100.csv
     ▼
┌──────────────────────────────────────────────┐
│ S3 (input)                                   │
│ bld-tseed-workshop-input-semillero-2026-q4-c │
│ └── johan_suarez/                            │
└──────────────────────────────────────────────┘
     │  lee el CSV
     ▼
┌──────────────────────────────┐
│ AWS Lambda (Python + Pandas) │   ── Parte 1
│ 1 fila del CSV = 1 JSON      │
└──────────────────────────────┘
     │  put de 100 JSON
     ▼
┌──────────────────────────────────────────────┐
│ S3 (output)                                  │
│ bld-tseed-workshop-output-semillero-2026-q4-c│
│ └── johan_suarez/users_json/                 │
└──────────────────────────────────────────────┘
     │  crawler on demand
     ▼
┌──────────────────────────────────────┐
│ AWS Glue                             │   ── Parte 2
│ DB: bld-tseed-workshop-johan-suarez  │
│ Crawler → tabla con el esquema       │
└──────────────────────────────────────┘
     │  consultas SQL
     ▼
┌──────────────────────────────────────────────┐
│ Amazon Athena                                │
│ Resultados guardados en el bucket output     │
│ └── johan_suarez/                            │
└──────────────────────────────────────────────┘
```

---

## Parte 1: Lambda (CSV → JSON)

### 1. Bucket de entrada

- El bucket `bld-tseed-workshop-input-semillero-2026-q4-c` ya estaba creado.
- Se creó el folder `johan_suarez/`, donde se encuentra el archivo `.csv`.

### 2. Bucket de salida

- El bucket `bld-tseed-workshop-output-semillero-2026-q4-c` ya estaba creado.
- Se creó el folder `johan_suarez/`, donde irá la carpeta `users_json/` con los JSON generados.

### 3. Función Lambda

La función está escrita en **Python** y recorre el CSV generando un JSON por cada fila. Para que funcionara se requirió:

- **Layer con Pandas:** se agregó un layer con la librería Pandas, compatible con la versión de Python configurada en la función.
- **Timeout:** se aumentó el tiempo de espera de la función a **15 segundos** para evitar errores de timeout.
- **Políticas de permisos (IAM):** se asociaron políticas al rol de la función para que pudiera leer el CSV del bucket de entrada y escribir los JSON en el bucket de salida (ver detalle abajo).

#### Políticas IAM

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadSourceBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::bld-tseed-workshop-input-semillero-2026-q4-c/johan_suarez/*"
      ]
    },
    {
      "Sid": "ReadWriteOutputBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::bld-tseed-workshop-output-semillero-2026-q4-c/johan_suarez/*"
      ]
    }
  ]
}
```

#### Código de la función

```python
import json
import boto3
import pandas as pd
from io import StringIO

s3_client = boto3.client("s3")

SOURCE_BUCKET = "bld-tseed-workshop-input-semillero-2026-q4-c"
SOURCE_KEY = "johan_suarez/users_dataset_100.csv"   # folder/archivo de entrada

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
```

**Resultado:** 100 archivos JSON en `s3://bld-tseed-workshop-output-semillero-2026-q4-c/johan_suarez/users_json/`.

<!-- Captura: docs/img/lambda-json-generados.png -->

---

## Parte 2: Glue y Athena (consulta de los datos)

### 1. Base de datos y crawler (AWS Glue)

- Se creó la base de datos `bld-tseed-workshop-johan-suarez` en el Glue Data Catalog.
- Dentro de ella se creó un **crawler** en modo **on demand** (se ejecuta manualmente), apuntando a la carpeta `johan_suarezz/` donde están la carpeta con los JSON.

### 2. Creación de la tabla

Al ejecutar el crawler se creó automáticamente la tabla con el **esquema** inferido a partir de los JSON:

| Columna | Tipo |
|---|---|
| `user_id` | `int` |
| `name` | `string` |
| `country` | `string` |
| `city` | `string` |
| `role` | `string` |
| `department` | `string` |
| `education` | `string` |
| `age` | `int` |
| `gender` | `string` |
| `marital_status` | `string` |
| `experience_years` | `int` |
| `skills` | `string` |
| `languages` | `string` |
| `hobbies` | `string` |
| `salary` | `double` |
| `email` | `string` |
| `phone` | `string` |
| `partition_0` | `string` (clave de partición) |

<!-- Captura: docs/img/glue-tabla-esquema.png -->

### 3. Consultas con Athena

Se ejecutaron las siguientes consultas sobre la tabla `johan_suarez` de la base de datos `bld-tseed-workshop-johan-suarez`. Los resultados se guardan en el bucket de salida.

**Consulta 1: primeros 10 registros**

```sql
SELECT * FROM "AwsDataCatalog"."bld-tseed-workshop-johan-suarez"."johan_suarez" limit 10;
```

<!-- Captura: docs/img/athena-consulta-1.png -->

**Consulta 2: filtro por edad e idioma**

```sql
SELECT * FROM "AwsDataCatalog"."bld-tseed-workshop-johan-suarez"."johan_suarez"
WHERE age > 35 AND languages = 'English';
```

<!-- Captura: docs/img/athena-consulta-2.png -->

**Consulta 3: resumen por departamento**

```sql
SELECT
  department,
  COUNT(*) AS total_usuarios,
  ROUND(AVG(salary), 2) AS salario_promedio,
  ROUND(AVG(experience_years), 1) AS experiencia_promedio
FROM "AwsDataCatalog"."bld-tseed-workshop-johan-suarez"."johan_suarez"
GROUP BY department
ORDER BY salario_promedio DESC;
```

<!-- Captura: docs/img/athena-consulta-3.png -->
