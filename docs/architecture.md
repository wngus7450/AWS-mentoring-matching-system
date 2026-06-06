# Architecture Evolution

## V1: Minimum Working Architecture

```mermaid
flowchart LR
    U[User Browser] --> S3[Amazon S3\nStatic Frontend]
    S3 --> API[Backend API\nNode.js / Express]
    API --> RDS[(RDS MySQL)]
    API --> S3Files[Amazon S3\nFile Storage]
```

### Purpose
V1 focuses on implementing the required mentoring matching and consultation record features with the smallest practical infrastructure.

### Limitation
- Backend responsibilities are concentrated in one application.
- Request processing is mostly synchronous.
- Scaling the backend requires managing the runtime environment.
- More traffic can increase database connection pressure.

## V2: Cloud-Native Serverless Architecture

```mermaid
flowchart LR
    U[User Browser] --> FE[Amazon S3\nStatic Frontend]
    FE --> APIGW[Amazon API Gateway]

    APIGW --> AUTH[Auth Lambda]
    APIGW --> ADMIN[Admin Lambda]
    APIGW --> MENTOR[Mentor Profile Lambda]
    APIGW --> REQUEST[Mentoring Request Lambda]
    APIGW --> MATCH[Matching Lambda]
    APIGW --> SCHEDULE[Schedule Lambda]
    APIGW --> ASSIGN[Assignment Lambda]
    APIGW --> RECORD[Consultation Record Lambda]

    AUTH --> DDB[(Amazon DynamoDB)]
    ADMIN --> DDB
    MENTOR --> DDB
    REQUEST --> DDB
    MATCH --> DDB
    SCHEDULE --> DDB
    ASSIGN --> DDB
    RECORD --> DDB

    MATCH --> BEDROCK[Amazon Bedrock\nEmbedding-based Matching]
    MENTOR --> S3V[Amazon S3\nVerification Files]
    REQUEST --> SNS[Amazon SNS]
    SCHEDULE --> SNS
    ASSIGN --> SNS
    SNS --> SQS[Amazon SQS]
    SQS --> NOTI[Notification Lambda]
    NOTI --> DDB
```

### Improvement
- API Gateway provides a stable API entry point.
- Lambda functions are separated by business capability.
- SNS and SQS decouple notification/event processing.
- DynamoDB supports serverless data access for user/request-oriented workloads.
- Bedrock-based embedding logic supports mentor matching.

## Final Roadmap: 10 Million Users

```mermaid
flowchart LR
    U[Global Users] --> CDN[CDN / Edge Cache]
    CDN --> FE[Static Frontend Hosting]
    FE --> WAF[WAF / DDoS Protection]
    WAF --> APIGW[API Gateway]
    APIGW --> AUTHZ[Authentication / Authorization Layer]
    AUTHZ --> LAMBDA[Serverless Business APIs]
    LAMBDA --> CACHE[Cache Layer]
    LAMBDA --> DDB[DynamoDB / NoSQL Tables]
    LAMBDA --> RDB[(Relational DB\nfor transactional data)]
    RDB --> RR[Read Replicas]
    LAMBDA --> EVENT[Event Bus / SNS]
    EVENT --> QUEUE[SQS Queues]
    QUEUE --> WORKER[Async Workers]
    LAMBDA --> OBS[Monitoring / Logging / Tracing]
```

### Roadmap Direction
- Add CDN and cache to reduce latency and repeated database reads.
- Apply Multi-AZ and read replicas for high availability.
- Separate transactional data, logs, notification history, and analytics data.
- Strengthen authentication, authorization, encryption, and audit logging.
- Monitor API latency, Lambda errors, queue backlog, and database capacity.
