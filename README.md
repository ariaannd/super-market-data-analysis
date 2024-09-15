# Supermarket Transaction Data Analysis Pipeline

## Project Overview
This project aims to analyze supermarket transactional data by building an **Airflow pipeline** for data preprocessing and visualizing the results using **Elasticsearch** and **Kibana** to extract actionable business insights.

## Workflow
1. **Data Ingestion**: Collect raw transactional data from the supermarket system.
2. **Preprocessing (Airflow)**:
   - Clean, transform, and normalize the data.
   - Manage the workflow using Airflow DAGs (Directed Acyclic Graphs) for efficient task scheduling.
3. **Data Indexing (Elasticsearch)**:
   - Store preprocessed data in Elasticsearch for real-time search and analysis.
4. **Visualization (Kibana)**:
   - Create dashboards to visualize key metrics such as sales trends, product performance, and customer behavior.

## Insights
Visualize key business metrics in **Kibana** to drive insights such as:
- Sales patterns
- Product category performance
- Customer buying behavior

## Conclusion
This pipeline enables efficient processing of supermarket data and provides visual insights for data-driven decision-making.