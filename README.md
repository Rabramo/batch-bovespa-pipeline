# Batch Bovespa Pipeline

This repository contains the implementation of a batch data pipeline for the B3 (Brasil, Bolsa, Balcão) stock exchange, using **AWS services** such as S3, Lambda, Glue (visual mode), Glue Catalog, and Athena.  
It was developed as part of the **Tech Challenge – Phase 2** of the **Machine Learning Engineer** course at **FIAP**, 1st semester of 2025.  
This solution was built by **Group 214**, with the participant **Rogério Abramo Alves Pretti** (rabramo@gmail.com).


## Objective

The challenge is to extract, transform, and analyze stock trading data using a batch data architecture based on AWS services, including **S3, Lambda, Glue (visual mode), Glue Catalog, and Athena**.

## Requirements

The pipeline must implement the following features:

1. **Scrape data** from the B3 website using a specific URL.
2. Store **raw data in S3** in Parquet format, partitioned by day.
3. Configure the S3 bucket to **trigger a Lambda function**.
4. The Lambda function (any language) must **start a Glue Job**.
5. The Glue Job, built using **Glue Studio (visual mode)**, must:
   - Perform **aggregations** (e.g., sum, count),
   - **Rename two columns** in addition to grouping fields,
   - Include a **date-based calculation** (e.g., duration or difference).
6. Save **refined data in S3** under a `refined/` folder, in Parquet format, partitioned by **date and stock name or abbreviation**.
7. The Glue Job must **automatically register data** in the Glue Catalog in the `default` database.
8. The dataset must be **queryable in Athena**.
9. Create an **Athena notebook** with graphical visualization.

## Project Structure

```bash
batch-bovespa-pipeline/
├── src/
│   └── lambda/
├── glue-scripts/
├── sql/
├── docs/
├── requirements.txt
└── README.md
