-- Sample Data Loading for Flood Prediction System
-- This demonstrates how unstructured data will be processed by BigQuery AI

-- Project ID: flood-prediction-hackathon

-- 1. Insert Sample Unstructured Weather Data
INSERT INTO `flood-prediction-hackathon.flood_prediction.raw_weather_data` VALUES
(
  'noaa_houston_001',
  'NOAA',
  'Houston, TX',
  29.7604,
  -95.3698,
  'US',
  'TX',
  'emergency',
  'FLASH FLOOD EMERGENCY for Harris County Texas until 10 PM CDT. Life-threatening flooding occurring along Buffalo Bayou and surrounding areas. Rainfall rates of 3 to 5 inches per hour continue across the metro area. Multiple water rescues are in progress. Do not drive through flooded roadways. Turn Around Dont Drown. Move to higher ground immediately if you are in a flood prone area.',
  'https://alerts.weather.gov/cap/tx.php?x=1',
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  'CRITICAL'
),
(
  'noaa_calgary_001', 
  'ECCC',
  'Calgary, AB',
  51.0447,
  -114.0719,
  'CA',
  'AB',
  'warning',
  'FLOOD WARNING issued for Bow River Basin including Calgary. River levels are expected to reach flood stage within the next 24 hours due to rapid snowmelt in the Rocky Mountains combined with 40-60mm of rainfall expected. Residents in flood-prone areas along the Bow and Elbow Rivers should prepare for possible evacuations. Monitor Alberta Emergency Alert for updates.',
  'https://weather.gc.ca/warnings/report_e.html?ab9',
  CURRENT_TIMESTAMP(),
  TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR),
  'HIGH'
),
(
  'noaa_new_orleans_001',
  'NOAA', 
  'New Orleans, LA',
  29.9511,
  -90.0715,
  'US',
  'LA',
  'watch',
  'FLOOD WATCH in effect for Orleans and Jefferson Parishes. A slow-moving storm system is expected to bring 4 to 8 inches of rainfall over the next 48 hours with locally higher amounts possible. Urban and poor drainage flooding is likely. Pump stations are operating at full capacity. Residents should avoid unnecessary travel and monitor weather conditions closely.',
  'https://alerts.weather.gov/cap/la.php?x=2',
  CURRENT_TIMESTAMP(),
  TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
  'MEDIUM'
);

-- 2. Insert Sample Unstructured Social Media Data
INSERT INTO `flood-prediction-hackathon.flood_prediction.social_media_data` VALUES
(
  'twitter_001',
  'twitter',
  'OMG major flooding on I-45 near downtown Houston right now! Water up to my car doors, completely stuck in traffic. This is scary! #flood #houston #help #emergency',
  'user_houston_123',
  'Houston, TX',
  29.7604,
  -95.3698,
  ['#flood', '#houston', '#help', '#emergency'],
  [],
  156,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR),
  CURRENT_TIMESTAMP(),
  -0.8, -- Negative sentiment (scary, emergency)
  0.9   -- High flood relevance
),
(
  'facebook_001',
  'facebook',
  'UPDATE: Bow River levels rising quickly in Calgary. Saw sandbags being distributed at the community center on 4th Street. Officials recommend people in Sunnyside and Hillhurst areas prepare for possible evacuation. Stay safe everyone! 🙏',
  'user_calgary_456',
  'Calgary, AB',
  51.0447,
  -114.0719,
  [],
  [],
  89,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR),
  CURRENT_TIMESTAMP(),
  0.2,  -- Slightly positive sentiment (helpful info)
  0.85  -- High flood relevance
),
(
  'twitter_002',
  'twitter',
  'Basement flooded again in Metairie. This is the third time this year! When will the city fix the drainage system? Water everywhere 😡 #flooding #neworleans #drainage',
  'user_nola_789',
  'New Orleans, LA',
  29.9511,
  -90.0715,
  ['#flooding', '#neworleans', '#drainage'],
  [],
  67,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR),
  CURRENT_TIMESTAMP(),
  -0.6, -- Negative sentiment (frustrated)
  0.75  -- Good flood relevance
);

-- 3. Insert Sample Unstructured News Articles
INSERT INTO `flood-prediction-hackathon.flood_prediction.news_articles` VALUES
(
  'news_001',
  'Flash Flood Emergency Declared in Harris County as Heavy Rains Pummel Houston',
  'HOUSTON - The National Weather Service has declared a flash flood emergency for Harris County as torrential rains continue to pound the greater Houston metropolitan area. Rainfall rates of 3 to 5 inches per hour have been reported across the region, with some areas receiving over 8 inches of rain in just 4 hours. Multiple high-water rescues are underway as major roadways including Interstate 45, US 59, and the Southwest Freeway have become impassable. Houston Fire Department reports they have conducted more than 50 water rescues since the flooding began this morning. Mayor Turner has advised all residents to shelter in place and avoid all non-essential travel.',
  'Houston Chronicle',
  'Sarah Johnson',
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
  CURRENT_TIMESTAMP(),
  'https://houstonchronicle.com/news/flash-flood-emergency-harris-county',
  ['Harris County', 'Houston', 'Interstate 45', 'Southwest Freeway'],
  ['flash flood', 'emergency', 'torrential rain', 'water rescue'],
  'EXTREME',
  'breaking'
),
(
  'news_002',
  'Calgary Prepares for Spring Flood Season as Bow River Levels Rise',
  'CALGARY - City officials are closely monitoring rising water levels in the Bow River as spring snowmelt combines with recent precipitation to create elevated flood risk. The Calgary Emergency Management Agency (CEMA) has activated its flood response plan and is working with provincial authorities to assess the situation. Sandbag distribution points have been set up in flood-prone communities including Sunnyside, Hillhurst, and Eau Claire. This comes exactly 11 years after the devastating 2013 floods that caused billions in damage and displaced over 100,000 residents. The city has invested heavily in flood mitigation infrastructure since then, but officials urge residents to remain vigilant and prepared.',
  'CBC Calgary',
  'Mike Thompson',
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR),
  CURRENT_TIMESTAMP(),
  'https://cbc.ca/news/calgary-spring-flood-preparation-2024',
  ['Calgary', 'Bow River', 'Sunnyside', 'Hillhurst', 'Eau Claire'],
  ['spring flood', 'snowmelt', 'flood mitigation', 'sandbags'],
  'HIGH',
  'forecast'
);

-- 4. Insert Sample Historical Flood Events (with unstructured descriptions)
INSERT INTO `flood-prediction-hackathon.flood_prediction.historical_floods` VALUES
(
  'hurricane_harvey_2017',
  'Hurricane Harvey Houston Flooding',
  'Houston, TX',
  29.7604,
  -95.3698,
  'US',
  'TX',
  '2017-08-25',
  '2017-09-01',
  'CATASTROPHIC',
  125000000000.0,
  68,
  30000,
  'HURRICANE',
  NULL,
  NULL,
  'Hurricane Harvey brought unprecedented rainfall to the Houston metropolitan area, with some areas receiving over 60 inches of rain in just four days. The storm stalled over the region, causing catastrophic flooding that inundated hundreds of thousands of homes and businesses. Buffalo Bayou, Braes Bayou, and other waterways overflowed their banks, turning streets into rivers. Emergency responders conducted over 17,000 water rescues. The flooding was exacerbated by the urban heat island effect and extensive development in flood-prone areas. Many victims were trapped in their homes and had to be rescued from rooftops. The economic impact included disruption to the energy sector, with numerous refineries and petrochemical plants shutting down operations.',
  ['FEMA', 'NOAA', 'Harris County Flood Control']
),
(
  'calgary_flood_2013',
  'Calgary Alberta Flood',
  'Calgary, AB',
  51.0447,
  -114.0719,
  'CA',
  'AB',
  '2013-06-19',
  '2013-06-25',
  'MAJOR',
  6000000000.0,
  5,
  100000,
  'RIVER',
  1750.0,
  4.5,
  'Heavy rainfall in the Rocky Mountains caused severe flooding along the Bow and Elbow rivers in Calgary and surrounding areas. The storm system brought 100mm to 200mm of rainfall to the mountains in just 36 hours, causing rapid snowmelt and unprecedented river flows. Downtown Calgary was evacuated as the Bow River reached levels not seen since 1932. The Saddledome was flooded with water reaching row 8 of the lower bowl. Entire neighborhoods including Sunnyside, Hillhurst, Bowness, and Elbow Park were inundated. The flooding forced the evacuation of over 100,000 people, making it one of the largest peacetime evacuations in Canadian history. Infrastructure damage was extensive, with bridges, roads, and the CTrain system severely impacted.',
  ['Environment Canada', 'Alberta Emergency Management', 'City of Calgary']
);

-- 5. Create Initial Risk Assessments (AI will update these)
INSERT INTO `flood-prediction-hackathon.flood_prediction.flood_risk_assessment` VALUES
(
  'assessment_houston_001',
  'Houston, TX',
  29.7604,
  -95.3698,
  'US',
  'TX',
  87.5, -- High risk score based on current emergency
  'EXTREME',
  0.85, -- 85% probability in next 24 hours
  0.72, -- 72% probability in next 7 days
  ['flash_flood_emergency', 'heavy_rainfall', 'urban_flooding', 'historical_vulnerability'],
  ['noaa_alerts', 'social_media', 'historical_patterns'],
  0.94, -- High confidence
  CURRENT_TIMESTAMP(),
  TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
  'EMERGENCY'
),
(
  'assessment_calgary_001',
  'Calgary, AB',
  51.0447,
  -114.0719,
  'CA',
  'AB',
  69.2, -- High risk score
  'HIGH',
  0.48, -- 48% probability in next 24 hours
  0.65, -- 65% probability in next 7 days  
  ['river_flooding', 'snowmelt', 'spring_conditions', 'historical_2013_flood'],
  ['eccc_warnings', 'local_monitoring', 'historical_patterns'],
  0.89, -- Good confidence
  CURRENT_TIMESTAMP(),
  TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 8 HOUR),
  'WARNING'
),
(
  'assessment_new_orleans_001',
  'New Orleans, LA',
  29.9511,
  -90.0715,
  'US',
  'LA',
  74.3, -- High risk score
  'HIGH',
  0.62, -- 62% probability in next 24 hours
  0.58, -- 58% probability in next 7 days
  ['urban_flooding', 'poor_drainage', 'below_sea_level', 'pump_system_capacity'],
  ['noaa_watches', 'social_media', 'infrastructure_monitoring'],
  0.82, -- Good confidence
  CURRENT_TIMESTAMP(),
  TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
  'WARNING'
);

-- 6. Insert Prevention Recommendations
INSERT INTO `flood-prediction-hackathon.flood_prediction.prevention_recommendations` VALUES
(
  'rec_houston_001',
  'Houston, TX',
  'EXTREME',
  'FLASH',
  'IMMEDIATE',
  'EVACUATION',
  'Evacuate low-lying areas immediately. Do not drive through flooded roads - Turn Around, Dont Drown. Move to higher ground and shelter in place until conditions improve.',
  1, -- Highest priority
  ['emergency_shelter', 'transportation', 'communication'],
  'Immediate - within 1 hour',
  'Free - emergency services',
  0.95,
  ['Harris County', 'Houston Metro', 'Southeast Texas']
),
(
  'rec_calgary_001',
  'Calgary, AB',
  'HIGH', 
  'RIVER',
  'SHORT_TERM',
  'PREPARATION',
  'Prepare sandbags around property perimeter. Review evacuation routes and emergency plans. Keep important documents in waterproof containers. Monitor Alberta Emergency Alert system.',
  2,
  ['sandbags', 'emergency_kit', 'communication_device'],
  '2-4 hours preparation',
  '$50-200 for supplies',
  0.88,
  ['Calgary', 'Bow River Basin', 'Southern Alberta']
);

-- 7. Test Query - Verify All Data Loaded Successfully
SELECT 
  'weather_data' as table_name,
  COUNT(*) as record_count
FROM `flood-prediction-hackathon.flood_prediction.raw_weather_data`
UNION ALL
SELECT 
  'social_media_data' as table_name,
  COUNT(*) as record_count
FROM `flood-prediction-hackathon.flood_prediction.social_media_data`  
UNION ALL
SELECT 
  'news_articles' as table_name,
  COUNT(*) as record_count
FROM `flood-prediction-hackathon.flood_prediction.news_articles`
UNION ALL
SELECT 
  'historical_floods' as table_name,
  COUNT(*) as record_count
FROM `flood-prediction-hackathon.flood_prediction.historical_floods`
UNION ALL
SELECT 
  'risk_assessments' as table_name,
  COUNT(*) as record_count
FROM `flood-prediction-hackathon.flood_prediction.flood_risk_assessment`
ORDER BY table_name;

-- 8. Test the Views and Unstructured Data Processing
SELECT 
  'Sample unstructured weather analysis complete!' as status,
  'Data ready for AI processing' as next_step;
