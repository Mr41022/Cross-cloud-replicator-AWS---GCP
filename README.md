sequenceDiagram

&nbsp;   Client->>+API: POST /v1/replicate {s3\_bucket, s3\_key}

&nbsp;   API->>+S3: Head Object (for ETag)

&nbsp;   S3-->>-API: ETag

&nbsp;   API->>+GCS: Check Blob Exists \& MD5

&nbsp;   GCS-->>-API: Exists? Skip if match

&nbsp;   alt New/Mismatch

&nbsp;       API->>+S3: Get Object (stream)

&nbsp;       S3-->>-API: Stream

&nbsp;       API->>+GCS: Upload Stream (with retry)

&nbsp;       GCS-->>-API: Success

&nbsp;   end

&nbsp;   API-->>-Client: 200/409 Response

