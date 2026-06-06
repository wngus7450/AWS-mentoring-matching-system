# Presentation Summary

## Project
AWS Mentoring Matching and Consultation Record System

## Problem
A mentoring service needs to support mentee applications, mentor profiles, administrator assignment, schedule confirmation, consultation records, and notifications.

## V1
V1 was designed as a minimum working system. The main goal was to implement the required features first and verify that the service works end-to-end.

## V1 Limitations
- Backend logic was concentrated in one application.
- Notification and main request processing were not sufficiently separated.
- More traffic could increase database and server management burden.
- Internal backend changes could affect frontend integration.

## V2
V2 improved the architecture using AWS cloud services. The frontend is hosted on Amazon S3, API Gateway acts as a stable API entry point, and backend functions are separated into multiple Lambda functions.

## V2 Improvements
- API Gateway reduces frontend dependency on backend implementation details.
- Lambda separation improves maintainability and independent scaling.
- SNS/SQS enables asynchronous notification processing.
- DynamoDB supports serverless data handling for user, request, and notification data.
- Bedrock embedding logic supports mentor matching.

## Final Roadmap
For 10 million users, the system should add CDN, caching, stronger security, high availability, read scaling, monitoring, and data separation strategies.
