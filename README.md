# 🌍 Real-Time Climate Risk Mapping with BigQuery AI

## Overview
This project leverages **Google BigQuery AI** to integrate **NOAA climate data** with **GDELT global news sentiment** in order to deliver **real-time climate risk intelligence**. Using BigQuery’s **vector search, embeddings, and generative AI**, we detect anomalies (heatwaves, floods, storms), connect them to relevant news, and generate concise **AI-powered climate briefs**.

---

## Features
- **Climate Anomaly Detection**: NOAA GSOD daily weather observations → anomaly flags (heatwaves, heavy rain, storms).
- **Semantic News Matching**: Embeds GDELT news articles and retrieves semantically similar events using **BigQuery Vector Index**.
- **AI Climate Briefs**: Auto-generated regional summaries via `ML.GENERATE_TEXT` (Gemini models).
- **Interactive Dashboard**: Looker Studio map with hotspots, time slider, sentiment analysis, and AI-generated briefs.

---

## Technical Architecture
1. **Data Ingestion**
   - NOAA GSOD (`bigquery-public-data.noaa_gsod`) → climate anomalies.
   - GDELT Events & GKG (`gdelt-bq.gdeltv2`) → news + sentiment.

2. **Feature Engineering**
   - Z-scores of temperature/precipitation → anomaly flags.
   - Extracted GDELT sentiment + geo-coordinates.

3. **AI & Vector Search**
   - `ML.GENERATE_EMBEDDING` → embeddings for news text.
   - `CREATE VECTOR INDEX` → fast similarity search.
   - `ML.GENERATE_TEXT` → AI-generated regional briefs.

4. **Visualization**
   - Looker Studio → maps, anomaly filters, and summaries.

---

## Example Query Workflows

**Detect anomalies**
```sql
SELECT station_id, obs_date, temp,
       CASE WHEN z_temp >= 2 THEN 'HEATWAVE' ELSE 'NORMAL' END AS anomaly
FROM `yourproj.climate_ai.noaa_anom`
WHERE obs_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY);
