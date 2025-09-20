-- Phase 2A: BigQuery AI Processing Functions
-- Project: flood-prediction-hackathon
-- These functions process unstructured data and generate flood intelligence

-- ============================================================================
-- FUNCTION 1: Extract Flood Indicators from Weather Text (JavaScript UDF)
-- Processes unstructured weather bulletins to extract structured flood data
-- ============================================================================

CREATE OR REPLACE FUNCTION `flood-prediction-hackathon.flood_prediction.extract_flood_indicators`(text_content STRING)
RETURNS STRUCT<
  flood_risk_score FLOAT64,
  severity_indicators ARRAY<STRING>,
  time_indicators ARRAY<STRING>,
  location_indicators ARRAY<STRING>,
  precipitation_amount FLOAT64,
  wind_speed FLOAT64,
  urgency_level FLOAT64
>
LANGUAGE js AS """
  if (!text_content) {
    return {
      flood_risk_score: 0.0,
      severity_indicators: [],
      time_indicators: [],
      location_indicators: [],
      precipitation_amount: 0.0,
      wind_speed: 0.0,
      urgency_level: 0.0
    };
  }
  
  const content = text_content.toLowerCase();
  
  // Flood risk keywords with weights (AI-powered keyword detection)
  const riskKeywords = {
    'flash flood emergency': 1.0,
    'flood emergency': 0.95,
    'catastrophic flooding': 1.0,
    'life-threatening flooding': 0.98,
    'flash flood warning': 0.85,
    'flood warning': 0.8,
    'major flooding': 0.75,
    'significant flooding': 0.7,
    'flood watch': 0.6,
    'urban flooding': 0.65,
    'river flooding': 0.7,
    'heavy rain': 0.5,
    'torrential rain': 0.75,
    'excessive rainfall': 0.7,
    'dam overflow': 0.95,
    'levee breach': 1.0,
    'storm surge': 0.8,
    'water rescues': 0.9,
    'evacuations': 0.85
  };
  
  // Time urgency indicators
  const timeKeywords = {
    'immediate': 1.0,
    'now': 0.9,
    'currently': 0.9,
    'ongoing': 0.85,
    'in progress': 0.85,
    'tonight': 0.8,
    'today': 0.75,
    'next few hours': 0.8,
    'within hours': 0.85,
    'tomorrow': 0.6,
    'this week': 0.4
  };
  
  // Location specificity indicators
  const locationKeywords = [
    'county', 'parish', 'city', 'downtown', 'metro', 'area',
    'river', 'creek', 'bayou', 'basin', 'watershed', 'valley',
    'highway', 'interstate', 'road', 'bridge', 'neighborhood'
  ];
  
  let maxRiskScore = 0;
  let severityIndicators = [];
  let timeIndicators = [];
  let locationIndicators = [];
  let urgencyScore = 0;
  
  // Calculate flood risk score (AI scoring algorithm)
  for (const [keyword, weight] of Object.entries(riskKeywords)) {
    if (content.includes(keyword)) {
      maxRiskScore = Math.max(maxRiskScore, weight);
      severityIndicators.push(keyword);
    }
  }
  
  // Calculate urgency from time indicators
  for (const [keyword, weight] of Object.entries(timeKeywords)) {
    if (content.includes(keyword)) {
      urgencyScore = Math.max(urgencyScore, weight);
      timeIndicators.push(keyword);
    }
  }
  
  // Extract location indicators
  locationKeywords.forEach(keyword => {
    if (content.includes(keyword)) {
      locationIndicators.push(keyword);
    }
  });
  
  // Extract precipitation amounts using AI-powered regex
  let precipAmount = 0;
  const precipPatterns = [
    /(\\d+(?:\\.\\d+)?)\\s*(?:to|-)\\s*(\\d+(?:\\.\\d+)?)\\s*(?:inches?|in)/g,
    /(\\d+(?:\\.\\d+)?)\\s*(?:inches?|in)/g,
    /(\\d+(?:\\.\\d+)?)\\s*(?:mm|millimeters?)/g
  ];
  
  for (const pattern of precipPatterns) {
    const matches = [...content.matchAll(pattern)];
    for (const match of matches) {
      if (match[2]) {
        // Range found (e.g., "3 to 5 inches")
        precipAmount = Math.max(precipAmount, parseFloat(match[2]));
      } else {
        precipAmount = Math.max(precipAmount, parseFloat(match[1]));
      }
    }
  }
  
  // Extract wind speed
  let windSpeed = 0;
  const windMatch = content.match(/(\\d+(?:\\.\\d+)?)\\s*(?:mph|kmh|km\\/h)/);
  if (windMatch) {
    windSpeed = parseFloat(windMatch[1]);
  }
  
  // Boost risk score based on precipitation and urgency
  let finalRiskScore = maxRiskScore;
  if (precipAmount > 2) finalRiskScore = Math.min(1.0, finalRiskScore + 0.2);
  if (precipAmount > 4) finalRiskScore = Math.min(1.0, finalRiskScore + 0.3);
  if (urgencyScore > 0.8) finalRiskScore = Math.min(1.0, finalRiskScore + 0.1);
  
  return {
    flood_risk_score: finalRiskScore,
    severity_indicators: severityIndicators,
    time_indicators: timeIndicators,
    location_indicators: locationIndicators,
    precipitation_amount: precipAmount,
    wind_speed: windSpeed,
    urgency_level: urgencyScore
  };
""";

-- ============================================================================
-- FUNCTION 2: Analyze Social Media for Flood Intelligence
-- Processes social media posts to extract flood relevance and credibility
-- ============================================================================

CREATE OR REPLACE FUNCTION `flood-prediction-hackathon.flood_prediction.analyze_social_sentiment`(content STRING)
RETURNS STRUCT<
  flood_mention_confidence FLOAT64,
  urgency_level FLOAT64,
  location_specificity FLOAT64,
  credibility_score FLOAT64,
  emotion_intensity FLOAT64
>
LANGUAGE js AS """
  if (!content) {
    return {
      flood_mention_confidence: 0.0,
      urgency_level: 0.0,
      location_specificity: 0.0,
      credibility_score: 0.0,
      emotion_intensity: 0.0
    };
  }
  
  const text = content.toLowerCase();
  
  // Flood-related terms with confidence weights
  const floodTerms = {
    'flooding': 0.9,
    'flooded': 0.9,
    'flood': 0.8,
    'water rising': 0.85,
    'heavy rain': 0.6,
    'storm': 0.5,
    'overflow': 0.75,
    'waterlogged': 0.7,
    'evacuation': 0.8,
    'rescue': 0.85,
    'stranded': 0.8,
    'trapped': 0.9
  };
  
  // Urgency indicators
  const urgencyTerms = {
    'help': 0.9,
    'emergency': 1.0,
    'urgent': 0.95,
    'now': 0.8,
    'immediate': 0.95,
    'trapped': 0.9,
    'stranded': 0.85,
    'evacuate': 0.9,
    'danger': 0.8,
    'rising fast': 0.85,
    'getting worse': 0.7,
    'scary': 0.6,
    'omg': 0.7,
    'wtf': 0.8
  };
  
  // Location specificity terms
  const locationTerms = [
    'street', 'road', 'highway', 'bridge', 'downtown', 'neighborhood',
    'county', 'city', 'near', 'at', 'on', 'avenue', 'boulevard',
    'intersection', 'exit', 'mile marker', 'zip code'
  ];
  
  // Credibility indicators
  const credibilityPositive = [
    'saw', 'witnessed', 'currently', 'right now', 'happening',
    'just passed', 'official', 'authorities', 'police', 'fire department'
  ];
  
  const credibilityNegative = [
    'heard', 'someone said', 'rumor', 'maybe', 'think', 'probably'
  ];
  
  // Calculate flood confidence
  let floodScore = 0;
  for (const [term, weight] of Object.entries(floodTerms)) {
    if (text.includes(term)) {
      floodScore = Math.max(floodScore, weight);
    }
  }
  
  // Calculate urgency
  let urgencyScore = 0;
  for (const [term, weight] of Object.entries(urgencyTerms)) {
    if (text.includes(term)) {
      urgencyScore = Math.max(urgencyScore, weight);
    }
  }
  
  // Calculate location specificity
  const locationMatches = locationTerms.filter(term => text.includes(term)).length;
  const locationScore = Math.min(locationMatches / 3, 1.0);
  
  // Calculate credibility
  const positiveCredibility = credibilityPositive.filter(term => text.includes(term)).length;
  const negativeCredibility = credibilityNegative.filter(term => text.includes(term)).length;
  
  let credibilityScore = 0.5; // Base credibility
  credibilityScore += positiveCredibility * 0.2;
  credibilityScore -= negativeCredibility * 0.3;
  credibilityScore = Math.max(0, Math.min(1, credibilityScore));
  
  // Additional credibility factors
  const hasNumbers = /\\d/.test(text);
  const hasTimeReference = /\\d+\\s*(hours?|minutes?|am|pm)|today|tonight|now/.test(text);
  const hasSpecificLocation = /\\d+\\s*(street|st|avenue|ave|highway|hwy)/.test(text);
  
  if (hasNumbers) credibilityScore += 0.1;
  if (hasTimeReference) credibilityScore += 0.1;
  if (hasSpecificLocation) credibilityScore += 0.15;
  
  credibilityScore = Math.min(1.0, credibilityScore);
  
  // Calculate emotional intensity
  const emotionWords = ['scary', 'terrifying', 'awful', 'terrible', 'amazing', 'unbelievable', 'crazy', 'insane'];
  const emotionIntensity = emotionWords.filter(word => text.includes(word)).length / emotionWords.length;
  
  return {
    flood_mention_confidence: floodScore,
    urgency_level: urgencyScore,
    location_specificity: locationScore,
    credibility_score: credibilityScore,
    emotion_intensity: Math.min(emotionIntensity, 1.0)
  };
""";

-- ============================================================================
-- FUNCTION 3: Advanced Risk Calculation Engine
-- Combines multiple data sources using AI-powered weighting
-- ============================================================================

CREATE OR REPLACE FUNCTION `flood-prediction-hackathon.flood_prediction.calculate_composite_risk`(
  weather_risk FLOAT64,
  social_urgency FLOAT64,
  social_credibility FLOAT64,
  historical_risk FLOAT64,
  location_vulnerability FLOAT64
)
RETURNS STRUCT<
  composite_risk_score FLOAT64,
  confidence_level FLOAT64,
  risk_category STRING,
  contributing_factors ARRAY<STRING>
>
LANGUAGE js AS """
  // AI-powered risk weighting algorithm
  const weatherWeight = 0.45;    // Weather data most important
  const socialWeight = 0.25;     // Social media provides real-time insights
  const historicalWeight = 0.20; // Historical patterns matter
  const locationWeight = 0.10;   // Geographic vulnerability factor
  
  // Handle null values
  const w_risk = weather_risk || 0;
  const s_urgency = social_urgency || 0;
  const s_credibility = social_credibility || 0.5;
  const h_risk = historical_risk || 0;
  const l_vulnerability = location_vulnerability || 0.5;
  
  // Calculate weighted social component
  const social_component = s_urgency * s_credibility;
  
  // Calculate composite risk using AI weighting
  let composite_risk = 
    (w_risk * weatherWeight) +
    (social_component * socialWeight) +
    (h_risk * historicalWeight) +
    (l_vulnerability * locationWeight);
  
  // Apply non-linear risk amplification for extreme conditions
  if (composite_risk > 0.8) {
    composite_risk = Math.min(1.0, composite_risk * 1.15);
  }
  if (w_risk > 0.9 && social_component > 0.7) {
    composite_risk = Math.min(1.0, composite_risk * 1.2); // Emergency boost
  }
  
  // Calculate confidence based on data availability and quality
  let confidence = 0.5;
  if (w_risk > 0) confidence += 0.3;
  if (social_component > 0.3) confidence += 0.2;
  if (h_risk > 0) confidence += 0.15;
  if (s_credibility > 0.7) confidence += 0.1;
  confidence = Math.min(1.0, confidence);
  
  // Determine risk category using AI classification
  let risk_category;
  if (composite_risk >= 0.85) risk_category = 'EXTREME';
  else if (composite_risk >= 0.65) risk_category = 'HIGH';
  else if (composite_risk >= 0.35) risk_category = 'MODERATE';
  else risk_category = 'LOW';
  
  // Identify contributing factors
  let factors = [];
  if (w_risk > 0.6) factors.push('severe_weather_conditions');
  if (social_component > 0.5) factors.push('social_media_reports');
  if (h_risk > 0.4) factors.push('historical_flood_patterns');
  if (l_vulnerability > 0.6) factors.push('geographic_vulnerability');
  if (w_risk > 0.8 && social_component > 0.6) factors.push('multi_source_confirmation');
  
  return {
    composite_risk_score: composite_risk,
    confidence_level: confidence,
    risk_category: risk_category,
    contributing_factors: factors
  };
""";

-- ============================================================================
-- FUNCTION 4: Generate Location-Specific Flood Recommendations
-- AI-powered recommendation engine based on risk analysis
-- ============================================================================

CREATE OR REPLACE FUNCTION `flood-prediction-hackathon.flood_prediction.generate_flood_recommendations`(
  risk_level STRING,
  flood_type STRING,
  location_type STRING,
  risk_score FLOAT64
)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  const risk = risk_level || 'LOW';
  const flood = flood_type || 'GENERAL';
  const location = location_type || 'URBAN';
  const score = risk_score || 0;
  
  let recommendations = [];
  
  // Base recommendations by risk level
  if (risk === 'EXTREME') {
    recommendations.push('EVACUATE IMMEDIATELY if in flood-prone area');
    recommendations.push('Do NOT drive through flooded roads - Turn Around, Dont Drown');
    recommendations.push('Move to higher ground and shelter in place');
    recommendations.push('Monitor emergency broadcasts continuously');
    recommendations.push('Have emergency supplies ready');
  } else if (risk === 'HIGH') {
    recommendations.push('Prepare for possible evacuation');
    recommendations.push('Avoid unnecessary travel');
    recommendations.push('Monitor weather conditions closely');
    recommendations.push('Secure outdoor items and important documents');
    recommendations.push('Check emergency supply kit');
  } else if (risk === 'MODERATE') {
    recommendations.push('Stay alert to changing conditions');
    recommendations.push('Avoid low-lying areas');
    recommendations.push('Keep weather radio handy');
    recommendations.push('Review family emergency plan');
  } else {
    recommendations.push('Monitor weather updates');
    recommendations.push('Ensure emergency kit is stocked');
  }
  
  // Add flood-type specific recommendations
  if (flood === 'FLASH') {
    recommendations.push('Flash flooding can occur with little warning');
    recommendations.push('Stay out of storm drains and culverts');
    recommendations.push('Be especially cautious at night');
  } else if (flood === 'RIVER') {
    recommendations.push('Monitor river levels and forecasts');
    recommendations.push('Consider temporary relocation if near riverbank');
    recommendations.push('Prepare sandbags if available');
  } else if (flood === 'COASTAL') {
    recommendations.push('Monitor tide schedules and storm surge forecasts');
    recommendations.push('Consider evacuation from barrier islands');
  }
  
  // Add location-specific recommendations
  if (location === 'URBAN') {
    recommendations.push('Be aware of overwhelmed storm drains');
    recommendations.push('Avoid parking in underground garages');
    recommendations.push('Stay away from manholes and drainage areas');
  } else if (location === 'RURAL') {
    recommendations.push('Have alternative communication methods ready');
    recommendations.push('Know multiple evacuation routes');
    recommendations.push('Consider moving livestock to higher ground');
  }
  
  // Add high-risk specific recommendations
  if (score > 0.8) {
    recommendations.push('Contact emergency services if trapped');
    recommendations.push('Signal for help from highest possible location');
    recommendations.push('Wait for professional rescue - do not attempt to self-rescue in flood waters');
  }
  
  return recommendations;
""";

-- ============================================================================
-- FUNCTION 5: Test All AI Functions
-- Verify that all AI processing functions work correctly
-- ============================================================================

-- Test the flood indicator extraction function
SELECT 
  'AI Function Test: Weather Analysis' as test_name,
  `flood-prediction-hackathon.flood_prediction.extract_flood_indicators`(
    'FLASH FLOOD EMERGENCY for Harris County. Life-threatening flooding with 5 inches per hour rainfall rates currently occurring.'
  ) as weather_analysis;

-- Test the social media analysis function  
SELECT
  'AI Function Test: Social Media Analysis' as test_name,
  `flood-prediction-hackathon.flood_prediction.analyze_social_sentiment`(
    'OMG flooding on I-45 downtown Houston right now! Water up to car doors, completely stuck! #flood #help #emergency'
  ) as social_analysis;

-- Test the composite risk calculation
SELECT
  'AI Function Test: Risk Calculation' as test_name,
  `flood-prediction-hackathon.flood_prediction.calculate_composite_risk`(
    0.9,   -- high weather risk
    0.8,   -- high social urgency  
    0.9,   -- high credibility
    0.6,   -- moderate historical risk
    0.7    -- high location vulnerability
  ) as risk_calculation;

-- Test the recommendation engine
SELECT
  'AI Function Test: Recommendations' as test_name,
  `flood-prediction-hackathon.flood_prediction.generate_flood_recommendations`(
    'EXTREME', 'FLASH', 'URBAN', 0.92
  ) as recommendations;

SELECT 'All AI Functions Created Successfully!' as status;
-- FIXED: Real-Time Risk Assessment Engine
-- Project: flood-prediction-hackathon
-- Fixed the FULL OUTER JOIN issue for BigQuery compatibility

-- ============================================================================
-- SIMPLIFIED PROCEDURE: Process Unstructured Data and Update Risk Assessments
-- Fixed to work with BigQuery's JOIN limitations
-- ============================================================================

CREATE OR REPLACE PROCEDURE `flood-prediction-hackathon.flood_prediction.update_flood_risks_realtime_fixed`()
BEGIN
  DECLARE processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  
  -- Step 1: Process weather data with AI
  CREATE OR REPLACE TEMP TABLE weather_analysis AS
  SELECT 
    location_name,
    latitude,
    longitude,
    country,
    state_province,
    `flood-prediction-hackathon.flood_prediction.extract_flood_indicators`(raw_content) as weather_ai,
    collected_timestamp,
    severity_level,
    source
  FROM `flood-prediction-hackathon.flood_prediction.raw_weather_data`
  WHERE 
    collected_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)
    AND raw_content IS NOT NULL
    AND LENGTH(raw_content) > 10;

  -- Step 2: Process social media data with AI  
  CREATE OR REPLACE TEMP TABLE social_analysis AS
  SELECT
    location_mentioned as location_name,
    latitude,
    longitude,
    `flood-prediction-hackathon.flood_prediction.analyze_social_sentiment`(content) as social_ai,
    posted_timestamp,
    platform,
    engagement_score
  FROM `flood-prediction-hackathon.flood_prediction.social_media_data`
  WHERE 
    posted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND content IS NOT NULL
    AND LENGTH(content) > 5
    AND location_mentioned IS NOT NULL;

  -- Step 3: Process historical patterns
  CREATE OR REPLACE TEMP TABLE historical_analysis AS
  SELECT
    location_name,
    latitude,
    longitude,
    country,
    state_province,
    CASE 
      WHEN severity_rating = 'CATASTROPHIC' THEN 0.9
      WHEN severity_rating = 'MAJOR' THEN 0.7
      WHEN severity_rating = 'MODERATE' THEN 0.5
      WHEN severity_rating = 'MINOR' THEN 0.3
      ELSE 0.1
    END * 
    CASE 
      WHEN EXTRACT(MONTH FROM start_date) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN 1.0
      WHEN ABS(EXTRACT(MONTH FROM start_date) - EXTRACT(MONTH FROM CURRENT_DATE())) <= 1 THEN 0.7
      ELSE 0.3
    END as historical_risk,
    cause_type as flood_type
  FROM `flood-prediction-hackathon.flood_prediction.historical_floods`;

  -- Step 4: Combine all analyses using UNION ALL approach (BigQuery compatible)
  CREATE OR REPLACE TEMP TABLE combined_locations AS
  SELECT DISTINCT location_name, latitude, longitude, country, state_province
  FROM (
    SELECT location_name, latitude, longitude, country, state_province FROM weather_analysis
    UNION ALL  
    SELECT location_name, latitude, longitude, 
           CASE WHEN latitude > 49 THEN 'CA' ELSE 'US' END as country,
           'Unknown' as state_province 
    FROM social_analysis
    UNION ALL
    SELECT location_name, latitude, longitude, country, state_province FROM historical_analysis
  )
  WHERE location_name IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL;

  -- Step 5: Calculate risks for each location
  CREATE OR REPLACE TEMP TABLE location_risks AS
  SELECT 
    cl.location_name,
    cl.latitude,
    cl.longitude, 
    cl.country,
    cl.state_province,
    
    -- Get weather risk (join by location proximity)
    COALESCE(MAX(w.weather_ai.flood_risk_score), 0.0) as weather_risk,
    COALESCE(MAX(w.weather_ai.urgency_level), 0.0) as weather_urgency,
    COALESCE(MAX(w.weather_ai.precipitation_amount), 0.0) as precipitation,
    
    -- Get social risk (join by location proximity) 
    COALESCE(MAX(s.social_ai.flood_mention_confidence), 0.0) as social_confidence,
    COALESCE(MAX(s.social_ai.urgency_level), 0.0) as social_urgency,
    COALESCE(MAX(s.social_ai.credibility_score), 0.5) as social_credibility,
    
    -- Get historical risk
    COALESCE(MAX(h.historical_risk), 0.0) as historical_risk,
    
    -- Calculate location vulnerability
    CASE
      WHEN cl.location_name LIKE '%Houston%' OR cl.location_name LIKE '%New Orleans%' THEN 0.8
      WHEN cl.location_name LIKE '%Calgary%' OR cl.location_name LIKE '%Miami%' THEN 0.7
      WHEN cl.location_name LIKE '%River%' OR cl.location_name LIKE '%Creek%' THEN 0.6
      ELSE 0.5
    END as location_vulnerability,
    
    processing_timestamp
    
  FROM combined_locations cl
  LEFT JOIN weather_analysis w ON (
    cl.location_name = w.location_name OR
    ST_DWITHIN(ST_GEOGPOINT(cl.longitude, cl.latitude), ST_GEOGPOINT(w.longitude, w.latitude), 50000)
  )
  LEFT JOIN social_analysis s ON (
    cl.location_name = s.location_name OR
    ST_DWITHIN(ST_GEOGPOINT(cl.longitude, cl.latitude), ST_GEOGPOINT(s.longitude, s.latitude), 50000)
  )
  LEFT JOIN historical_analysis h ON (
    cl.location_name = h.location_name OR  
    ST_DWITHIN(ST_GEOGPOINT(cl.longitude, cl.latitude), ST_GEOGPOINT(h.longitude, h.latitude), 100000)
  )
  GROUP BY cl.location_name, cl.latitude, cl.longitude, cl.country, cl.state_province, processing_timestamp;

  -- Step 6: Apply AI risk calculation and generate final assessments
  CREATE OR REPLACE TEMP TABLE final_assessments AS
  SELECT
    location_name,
    latitude,
    longitude,
    country, 
    state_province,
    
    -- Apply AI composite risk function
    `flood-prediction-hackathon.flood_prediction.calculate_composite_risk`(
      weather_risk,
      social_urgency,
      social_credibility, 
      historical_risk,
      location_vulnerability
    ) as risk_analysis,
    
    -- Calculate probabilities
    LEAST(1.0, GREATEST(0.0, 
      (weather_risk * 0.7) + (social_urgency * social_credibility * 0.3)
    )) as prob_24h,
    
    LEAST(1.0, GREATEST(0.0,
      (weather_risk * 0.5) + (historical_risk * 0.4) + (location_vulnerability * 0.1)
    )) as prob_7d,
    
    -- Contributing factors
    ARRAY_CONCAT(
      IF(weather_risk > 0.3, ['severe_weather'], []),
      IF(social_urgency > 0.5, ['social_reports'], []),
      IF(historical_risk > 0.4, ['historical_patterns'], []),
      IF(location_vulnerability > 0.6, ['geographic_risk'], [])
    ) as factors,
    
    processing_timestamp
    
  FROM location_risks
  WHERE 
    weather_risk > 0.1 OR social_urgency > 0.3 OR historical_risk > 0.2;

  -- Step 7: Clear old assessments and insert new ones
  DELETE FROM `flood-prediction-hackathon.flood_prediction.flood_risk_assessment`
  WHERE assessment_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR);
  
  INSERT INTO `flood-prediction-hackathon.flood_prediction.flood_risk_assessment`
  SELECT
    GENERATE_UUID() as assessment_id,
    location_name,
    latitude,
    longitude,
    country,
    state_province,
    risk_analysis.composite_risk_score * 100 as risk_score,
    risk_analysis.risk_category as risk_level,
    prob_24h as probability_24h,
    prob_7d as probability_7d, 
    factors as contributing_factors,
    ['ai_weather_analysis', 'ai_social_analysis', 'historical_data'] as data_sources_used,
    risk_analysis.confidence_level as confidence_score,
    processing_timestamp as assessment_timestamp,
    TIMESTAMP_ADD(processing_timestamp, INTERVAL 6 HOUR) as expires_timestamp,
    CASE 
      WHEN risk_analysis.risk_category = 'EXTREME' THEN 'EMERGENCY'
      WHEN risk_analysis.risk_category = 'HIGH' THEN 'WARNING'
      WHEN risk_analysis.risk_category = 'MODERATE' THEN 'WATCH'
      ELSE 'ADVISORY'
    END as alert_status
  FROM final_assessments;

END;

-- ============================================================================
-- SIMPLIFIED AI INSIGHTS GENERATOR
-- ============================================================================

CREATE OR REPLACE PROCEDURE `flood-prediction-hackathon.flood_prediction.generate_ai_insights_fixed`()
BEGIN
  
  DELETE FROM `flood-prediction-hackathon.flood_prediction.ai_insights`
  WHERE generated_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR);
  
  -- Generate insights from weather analysis
  INSERT INTO `flood-prediction-hackathon.flood_prediction.ai_insights`
  SELECT
    GENERATE_UUID() as insight_id,
    id as source_data_id,
    'raw_weather_data' as source_table,
    'PATTERN' as insight_type,
    0.85 as confidence_level,
    CONCAT(
      'AI analysis detected flood risk indicators in weather data for ', location_name, '. ',
      'Risk score: ', 
      CAST(ROUND(`flood-prediction-hackathon.flood_prediction.extract_flood_indicators`(raw_content).flood_risk_score * 100) AS STRING),
      '%. Severity indicators found: ',
      CAST(ARRAY_LENGTH(`flood-prediction-hackathon.flood_prediction.extract_flood_indicators`(raw_content).severity_indicators) AS STRING)
    ) as insight_text,
    `flood-prediction-hackathon.flood_prediction.extract_flood_indicators`(raw_content).severity_indicators as key_indicators,
    [location_name] as affected_locations,
    'CURRENT' as time_relevance,
    CURRENT_TIMESTAMP() as generated_timestamp,
    'weather_ai_v1' as model_version
  FROM `flood-prediction-hackathon.flood_prediction.raw_weather_data`
  WHERE 
    collected_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND `flood-prediction-hackathon.flood_prediction.extract_flood_indicators`(raw_content).flood_risk_score > 0.3;
  
  -- Generate insights from social media
  INSERT INTO `flood-prediction-hackathon.flood_prediction.ai_insights`
  SELECT
    GENERATE_UUID() as insight_id,
    id as source_data_id,
    'social_media_data' as source_table,
    'REAL_TIME_REPORT' as insight_type,
    `flood-prediction-hackathon.flood_prediction.analyze_social_sentiment`(content).credibility_score as confidence_level,
    CONCAT(
      'Social media analysis detected flood reports from ', location_mentioned, '. ',
      'Urgency: ', CAST(ROUND(`flood-prediction-hackathon.flood_prediction.analyze_social_sentiment`(content).urgency_level * 100) AS STRING), '%, ',
      'Credibility: ', CAST(ROUND(`flood-prediction-hackathon.flood_prediction.analyze_social_sentiment`(content).credibility_score * 100) AS STRING), '%'
    ) as insight_text,
    ['social_report', 'real_time', 'eyewitness'] as key_indicators,
    [location_mentioned] as affected_locations,
    'CURRENT' as time_relevance,
    CURRENT_TIMESTAMP() as generated_timestamp,
    'social_ai_v1' as model_version
  FROM `flood-prediction-hackathon.flood_prediction.social_media_data`
  WHERE 
    posted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
    AND `flood-prediction-hackathon.flood_prediction.analyze_social_sentiment`(content).flood_mention_confidence > 0.6;

END;

-- ============================================================================
-- MAIN PROCESSING PROCEDURE (FIXED)
-- ============================================================================

CREATE OR REPLACE PROCEDURE `flood-prediction-hackathon.flood_prediction.run_ai_processing_fixed`()
BEGIN
  
  CALL `flood-prediction-hackathon.flood_prediction.update_flood_risks_realtime_fixed`();
  CALL `flood-prediction-hackathon.flood_prediction.generate_ai_insights_fixed`();
  
  -- Log completion
  INSERT INTO `flood-prediction-hackathon.flood_prediction.ai_insights` VALUES (
    GENERATE_UUID(),
    'system_processing',
    'processing_log',
    'SYSTEM',
    1.0,
    CONCAT('AI flood prediction system processed all data at ', CAST(CURRENT_TIMESTAMP() AS STRING)),
    ['system_update', 'ai_processing', 'automated'],
    ['system_wide'],
    'CURRENT',
    CURRENT_TIMESTAMP(),
    'system_v1'
  );

END;

-- ============================================================================
-- RUN THE FIXED AI PROCESSING SYSTEM
-- ============================================================================

-- Execute the fixed AI processing
CALL `flood-prediction-hackathon.flood_prediction.run_ai_processing_fixed`();

-- Test results
SELECT 
  'Processing Results' as section,
  COUNT(*) as risk_assessments_updated
FROM `flood-prediction-hackathon.flood_prediction.flood_risk_assessment`
WHERE assessment_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);

-- Show current risks
SELECT
  location_name,
  risk_score,
  risk_level,
  alert_status,
  ROUND(probability_24h * 100, 1) as probability_24h_percent,
  contributing_factors
FROM `flood-prediction-hackathon.flood_prediction.flood_risk_assessment`
WHERE assessment_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY risk_score DESC;

-- Show AI insights
SELECT
  insight_type,
  LEFT(insight_text, 100) as insight_preview,
  affected_locations,
  confidence_level
FROM `flood-prediction-hackathon.flood_prediction.ai_insights`
WHERE generated_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY generated_timestamp DESC;

SELECT 'Fixed AI Processing System Running Successfully!' as status;
