# Storied BC ETL

Book Club ETL Pipeline

An automated data orchestration engine that manages the lifecycle of book club metadata, member reads, and social graphs.

## Architecture Overview
The pipeline follows a multi-phase staging strategy to ensure data integrity and minimize downtime:

    Phase 1: Extraction (GSheet → MongoDB Staging): Pulls raw data from Google Sheets into a staging environment using dynamic latency-based throttling.

    Phase 2: Transformation (Staging → Main): Detects deltas, syncs images to Azure Blob Storage, and loads cleaned documents into the production MongoDB cluster.

    Phase 3: Graph Sync (Main → Neo4j AuraDB): Transforms MongoDB documents into nodes and relationships for Neo4j, enabling graph-based features like social connections and recommendation paths.

## Key Features

    Idempotent Syncing: Uses full-document hashing to identify changes, ensuring only modified records are processed.

    Graph Orchestration: Automatically manages Neo4j constraints, node upserts, and complex relationship mapping (e.g., USER_READ, MEMBER_OF_CLUB).

    Asset Management: Synchronizes book covers and user avatars directly from source URLs to Azure Blob Storage.

    Infrastructure-Agnostic Config: Implements "Lazy Bootstrapping" via src/config.py to reconstruct .env and secret JSON files from Base64 environment variables at runtime.

    Resilient Design: Built-in retry decorators for network-sensitive operations (MongoDB, Neo4j, GSheets).

## Tech Stack

    Database: MongoDB (Primary), Neo4j AuraDB (Graph).

    Storage: Azure Blob Storage.

    Compute: Azure Container Apps (Jobs).

    Network: Azure VNet + NAT Gateway for static outbound IP whitelisting.

    Language: Python 3.11 (Loguru, Pymongo, Neo4j-Driver, Sentence-Transformers).

## Setup & Configuration
Environment Variables

The pipeline requires several Base64-encoded environment variables to reconstruct its configuration files at runtime:

    ENV_FILE_BASE64: Encoded .env file containing database URIs and API keys.

    GSHEET_JSON_BASE64: Encoded Google Service Account JSON.

    KEYS_JSON_BASE64: Encoded encryption key registry for secure data handling.

Local Development

    Clone the repository and install dependencies: pip install -r requirements.txt.

    Ensure your .env and src/secrets/ files are populated.

    Run the full pipeline: python src/etl.py.

## Automated Deployment

This pipeline is deployed as an Azure Container Apps Job triggered via a weekly cron schedule.

CI/CD Workflow:

    Build: GitHub Actions builds the Docker image on every push to main.

    Push: The image is pushed to Azure Container Registry (ACR).

    Trigger: The workflow updates the Container App Job with the latest image.

    Network: Outbound requests are routed through a NAT Gateway to provide a static IP for MongoDB Atlas and Neo4j AuraDB whitelisting.

## Roadmap

    Model Decoupling: Moving the GemmaEmbedder logic into a dedicated microservice/package for shared use across GraphRAG and Frontend services.

    Job Granularity: Splitting the monolithic etl.py into individual, decoupled Azure Jobs for better fault tolerance.

    IaC Migration: Transitioning manual infrastructure setup to Terraform for reproducible environment management.
