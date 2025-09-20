-- Phase 1: Complete Database Setup for Flood Prediction System
-- Copy and paste each section into BigQuery Console

-- IMPORTANT: Replace 'flood-prediction-hackathon' with your actual project ID throughout!

-- 1. Create the main dataset (if not already created)
CREATE SCHEMA IF NOT EXISTS `flood-prediction-hackathon.flood_prediction`
OPTIONS (
  description = "Flood Prediction System for US/Canada - BigQuery AI Hackathon",
  location = "US"
);

-- 2. Raw Weather Data Table (Unstructured Data Input)
CREATE OR REPLACE TABLE `flood-prediction-hackathon.flood_prediction.raw_weather_data` (
  id STRING NOT NULL,
  source STRING NOT NULL, -- 'NOAA', 'ECCC', 'LOCAL'
  location_name STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  country STRING, -- 'US' or 'CA'
  state_province STRING,
  data_type STRING, -- 'forecast', 'warning', 'alert', 'bulletin'
  raw_content STRING, -- Unstructured text content - KEY FOR AI PROCESSING
  document_url STRING,
  collected_timestamp TIMESTAMP,
  forecast_timestamp TIMESTAMP,
  severity_level STRING -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
) 
CLUSTER BY country, state_province;

-- 3. Social Media Data Table (Unstructured Social Content)
CREATE OR REPLACE TABLE `flood-prediction-hackathon.flood_prediction.social_media_data` (
  id STRING NOT NULL,
  platform STRING, -- 'twitter', 'reddit', 'facebook'
  content STRING, -- Unstructured social media text
  author STRING,
  location_mentioned STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  hashtags ARRAY<STRING>,
  mentions ARRAY<STRING>,
  engagement_score INT64,
  posted_timestamp TIMESTAMP,
  collected_timestamp TIMESTAMP,
  sentiment_score FLOAT64,
  flood_relevance_score FLOAT64
)
CLUSTER BY platform, posted_timestamp;

-- 4. News Articles Table (Unstructured News Content)
CREATE OR REPLACE TABLE `flood-prediction-hackathon.flood_prediction.news_articles` (
  id STRING NOT NULL,
  title STRING,
  content STRING, -- Unstructured article text
  source STRING,
  author STRING,
  published_timestamp TIMESTAMP,
  collected_timestamp TIMESTAMP,
  url STRING,
  locations_mentioned ARRAY<STRING>,
  keywords ARRAY<STRING>,
  flood_severity_mentioned STRING,
  article_type STRING -- 'breaking', 'forecast', 'historical'
);

-- 5. Historical Flood Events (Structured + Unstructured Descriptions)
CREATE OR REPLACE TABLE `flood-prediction-hackathon.flood_prediction.historical_floods` (
  event_id STRING NOT NULL,
  event_name STRING,
  location_name STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  country STRING,
  state_province STRING,
  start_date DATE,
  end_date DATE,
  severity_rating STRING, -- 'MINOR', 'MODERATE', 'MAJOR', 'CATASTROPHIC'
  damage_estimate FLOAT64,
  casualties INT64,
  displacement_count INT64,
  cause_type STRING, -- 'RIVER', 'FLASH', 'COASTAL', 'URBAN'
  peak_discharge FLOAT64,
  max_water_level FLOAT64,
  description STRING, -- Unstructured event description for AI analysis
  sources ARRAY<STRING>
);

-- 6. Real-time Risk Assessment Table (AI-Generated Structured Output)
CREATE OR REPLACE TABLE `flood-prediction-hackathon.flood_prediction.flood_risk_assessment` (
  assessment_id STRING NOT NULL,
  location_name STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  country STRING,
  state_province STRING,
  risk_score FLOAT64, -- 0-100 scale (AI-calculated)
  risk_level STRING, -- 'LOW', 'MODERATE', 'HIGH', 'EXTREME' (AI-determined)
  probability_24h FLOAT64, -- 0-1 probability (AI-predicted)
  probability_7d FLOAT64,
  contributing_factors ARRAY<STRING>, -- AI-extracted factors
  data_sources_used ARRAY<STRING>,
  confidence_score FLOAT64, -- AI confidence in assessment
  assessment_timestamp TIMESTAMP,
  expires_timestamp TIMESTAMP,
  alert_status STRING -- 'NONE', 'WATCH', 'WARNING', 'EMERGENCY'
);

-- 7. Prevention Recommendations Table (AI-Generated Advice)
CREATE OR REPLACE TABLE `flood-prediction-hackathon.flood_prediction.prevention_recommendations` (
  recommendation_id STRING NOT NULL,
  location_name STRING,
  risk_level STRING,
  flood_type STRING, -- 'RIVER', 'FLASH', 'COASTAL', 'URBAN'
  time_frame STRING, -- 'IMMEDIATE', 'SHORT_TERM', 'LONG_TERM'
  category STRING, -- 'EVACUATION', 'PREPARATION', 'PROTECTION', 'RECOVERY'
  recommendation_text STRING, -- AI-generated recommendations
  priority_level INT64, -- 1-5, 1 being highest
  resources_needed ARRAY<STRING>,
  estimated_time STRING,
  cost_estimate STRING,
  effectiveness_score FLOAT64,
  applicable_regions ARRAY<STRING>
);

-- 8. AI Insights Table (BigQuery AI Processing Results)
CREATE OR REPLACE TABLE `flood-prediction-hackathon.flood_prediction.ai_insights` (
  insight_id STRING NOT NULL,
  source_data_id STRING,
  source_table STRING,
  insight_type STRING, -- 'PATTERN', 'ANOMALY', 'PREDICTION', 'CORRELATION'
  confidence_level FLOAT64,
  insight_text STRING, -- AI-generated insights from unstructured data
  key_indicators ARRAY<STRING>, -- AI-extracted key indicators
  affected_locations ARRAY<STRING>, -- AI-identified locations
  time_relevance STRING, -- 'CURRENT', 'FORECASTED', 'HISTORICAL'
  generated_timestamp TIMESTAMP,
  model_version STRING
);

-- 9. Create Views for Easy Access to High-Risk Areas
CREATE OR REPLACE VIEW `flood-prediction-hackathon.flood_prediction.current_high_risk_areas` AS
SELECT 
  location_name,
  latitude,
  longitude,
  country,
  state_province,
  risk_score,
  risk_level,
  probability_24h,
  alert_status,
  assessment_timestamp,
  contributing_factors
FROM `flood-prediction-hackathon.flood_prediction.flood_risk_assessment`
WHERE 
  risk_level IN ('HIGH', 'EXTREME') 
  AND assessment_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
  AND expires_timestamp > CURRENT_TIMESTAMP()
ORDER BY risk_score DESC;

-- 10. View for Recent Weather Alerts (Unstructured Data Processing)
CREATE OR REPLACE VIEW `flood-prediction-hackathon.flood_prediction.recent_weather_alerts` AS
SELECT 
  location_name,
  country,
  state_province,
  data_type,
  severity_level,
  raw_content, -- This unstructured content will be processed by AI
  collected_timestamp,
  -- Preview of AI processing - extract flood keywords
  CASE 
    WHEN LOWER(raw_content) LIKE '%flash flood emergency%' THEN 'EMERGENCY'
    WHEN LOWER(raw_content) LIKE '%flood warning%' THEN 'WARNING'  
    WHEN LOWER(raw_content) LIKE '%flood watch%' THEN 'WATCH'
    ELSE 'ADVISORY'
  END AS alert_type,
  -- Extract mentioned precipitation amounts using regex
  REGEXP_EXTRACT(raw_content, r'(\d+(?:\.\d+)?)\s*(?:inch|in|mm)') AS precipitation_mentioned
FROM `flood-prediction-hackathon.flood_prediction.raw_weather_data`
WHERE 
  collected_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND (LOWER(raw_content) LIKE '%flood%' OR LOWER(raw_content) LIKE '%rain%' OR LOWER(raw_content) LIKE '%storm%')
ORDER BY collected_timestamp DESC;

-- 11. Test Query to Verify Setup
SELECT 
  'Database setup completed successfully!' as status,
  CURRENT_TIMESTAMP() as setup_time,
  'Ready for data ingestion and AI processing' as next_step;
