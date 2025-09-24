-- Migration script to update the pulled_properties table schema
-- Run this in your Supabase SQL editor AFTER updating the main schema.sql

-- Add new columns
ALTER TABLE pulled_properties ADD COLUMN IF NOT EXISTS gps_coordinates VARCHAR(255);
ALTER TABLE pulled_properties ADD COLUMN IF NOT EXISTS message_url TEXT;

-- Create index for the new amenities structure (if not exists)
CREATE INDEX IF NOT EXISTS idx_pulled_properties_amenities ON pulled_properties USING GIN(amenities);

-- Remove old boolean amenity columns (ONLY run this if you're sure you don't need the old data)
-- Uncomment these lines if you want to remove the old boolean columns:

-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_pool;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_garden;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_elevator;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_balcony;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_terrace;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_gym;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_security;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS pet_friendly;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS furnished;

-- Optional: Update existing records to have structured amenities
-- This is a one-time migration script to convert old boolean flags to new structure
-- Uncomment and run if you have existing data:

-- UPDATE pulled_properties 
-- SET amenities = jsonb_build_object(
--     'recreation', 
--     CASE WHEN has_pool THEN ARRAY['Pool'] ELSE ARRAY[]::text[] END ||
--     CASE WHEN has_gym THEN ARRAY['Gym'] ELSE ARRAY[]::text[] END,
--     'exterior',
--     CASE WHEN has_garden THEN ARRAY['Garden'] ELSE ARRAY[]::text[] END ||
--     CASE WHEN has_balcony THEN ARRAY['Balcony'] ELSE ARRAY[]::text[] END ||
--     CASE WHEN has_terrace THEN ARRAY['Terrace'] ELSE ARRAY[]::text[] END,
--     'general',
--     CASE WHEN has_elevator THEN ARRAY['Elevator'] ELSE ARRAY[]::text[] END ||
--     CASE WHEN has_security THEN ARRAY['24 Hour Security'] ELSE ARRAY[]::text[] END ||
--     CASE WHEN furnished THEN ARRAY['Furnished'] ELSE ARRAY[]::text[] END,
--     'policies',
--     CASE WHEN pet_friendly THEN ARRAY['Pets Allowed'] ELSE ARRAY[]::text[] END
-- )
-- WHERE amenities IS NULL OR amenities = '{}'::jsonb;

-- Update GPS coordinates from existing lat/lng data
UPDATE pulled_properties 
SET gps_coordinates = CONCAT(latitude::text, ',', longitude::text)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND gps_coordinates IS NULL; 