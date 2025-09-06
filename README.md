🌍 Real-Time Climate Risk Mapping with BigQuery AI
📌 Overview

This project leverages Google BigQuery AI to integrate NOAA climate data with GDELT global news sentiment in order to deliver real-time climate risk intelligence. Using BigQuery’s vector search, embeddings, and generative AI, we detect anomalies (heatwaves, floods, storms), connect them to relevant news, and generate concise AI-powered climate briefs.

🚀 Features

Climate Anomaly Detection: Uses NOAA GSOD daily weather observations to flag heatwaves, heavy rain, and storm events.

Semantic News Matching: Embeds GDELT news articles and retrieves semantically similar events using BigQuery Vector Index.

AI Climate Briefs: Auto-generated summaries per region via ML.GENERATE_TEXT (Gemini models).

Interactive Dashboard: Looker Studio map with hotspots, time slider, sentiment analysis, and AI-generated briefs.

⚙️ Technical Architecture

Data Ingestion

NOAA GSOD (bigquery-public-data.noaa_gsod) → climate anomalies.

GDELT Events & GKG (gdelt-bq.gdeltv2) → news + sentiment.

Feature Engineering

Z-scores of temperature/precipitation → anomaly flags.

Extracted GDELT sentiment + geo-coordinates.

AI & Vector Search

ML.GENERATE_EMBEDDING → embeddings for news text.

Vector index (CREATE VECTOR INDEX) → fast similarity search.

ML.GENERATE_TEXT → AI-generated regional briefs.

Visualization

Looker Studio → maps, anomaly filters, and summaries.

📊 Example Query Workflows

Detect anomalies:

SELECT station_id, obs_date, temp,
       CASE WHEN z_temp >= 2 THEN 'HEATWAVE' ELSE 'NORMAL' END AS anomaly
FROM `yourproj.climate_ai.noaa_anom`
WHERE obs_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY);


Generate embeddings:

SELECT SourceURL,
       ML.GENERATE_EMBEDDING(MODEL `yourproj.climate_ai.embed_model`,
       headline) AS embedding
FROM `yourproj.climate_ai.gdelt_recent`;


AI summary for region:

SELECT ML.GENERATE_TEXT(
  MODEL `yourproj.climate_ai.gemini_text`,
  "Summarize heatwave and storm events in Eastern Canada over last 14 days"
).text;

🌟 Impact

Policy & Governance: Climate-aware policy recommendations informed by real-time insights.

Disaster Response: Faster situational awareness combining hard data + sentiment.

Research & Academia: Novel integration of structured + unstructured climate data.

📦 Deliverables

 BigQuery SQL pipelines (NOAA + GDELT integration)

 Vector search + embeddings workflow

 AI climate briefs generator (Gemini)

 Looker Studio interactive dashboard

 Kaggle Notebook (end-to-end walkthrough)

🛠️ Tools & Tech

Google BigQuery AI – embeddings, vector search, generative AI in SQL

Datasets – NOAA GSOD, GDELT v2 Events & GKG

Looker Studio – visualization layer

Kaggle – competition notebook + reproducibility

📅 Timeline

Data Prep & Ingestion – Week 1

Pipeline + AI Integration – Week 2

Dashboard & Polishing – Final days before Deadline: Sept 22, 2025 (23:59 UTC)

✅ Conclusion

This project demonstrates how BigQuery AI can unite global climate data and media sentiment into a real-time, AI-powered risk monitoring system. It’s scalable, socially impactful, and a strong showcase of innovation at the intersection of climate + AI.
