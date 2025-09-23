-- Clear pulled_properties table for fresh data
-- Run this in your Supabase SQL Editor to bypass RLS policies

-- Option 1: Delete all records (preserves table structure)
DELETE FROM pulled_properties;

-- Option 2: If you want to reset the table completely and recreate it
-- DROP TABLE IF EXISTS pulled_properties CASCADE;

-- Option 3: Truncate table (faster for large datasets, resets auto-increment)
-- TRUNCATE TABLE pulled_properties RESTART IDENTITY CASCADE;

-- Verify the table is empty
SELECT COUNT(*) as remaining_records FROM pulled_properties;

-- Optional: Check other related tables
SELECT COUNT(*) as scraping_sessions FROM scraping_sessions;
SELECT COUNT(*) as scraping_errors FROM scraping_errors; 