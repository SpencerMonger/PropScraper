-- Supabase SQL Schema for Inmuebles24.com Property Data
-- Run this in your Supabase SQL editor

-- Create the main properties table
CREATE TABLE IF NOT EXISTS pulled_properties (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    
    -- Basic property information
    property_id VARCHAR(255) UNIQUE, -- Internal property ID from the site
    title TEXT NOT NULL,
    description TEXT,
    property_type VARCHAR(100), -- casa, departamento, oficina, etc.
    operation_type VARCHAR(50), -- venta, renta
    
    -- Location data
    address TEXT,
    neighborhood VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    postal_code VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    gps_coordinates VARCHAR(255), -- Store GPS coordinates as string for easy parsing
    
    -- Property details
    price DECIMAL(15, 2),
    currency VARCHAR(10) DEFAULT 'MXN',
    price_per_m2 DECIMAL(10, 2),
    bedrooms INTEGER,
    bathrooms INTEGER,
    half_bathrooms INTEGER,
    parking_spaces INTEGER,
    
    -- Area measurements
    total_area_m2 DECIMAL(10, 2),
    covered_area_m2 DECIMAL(10, 2),
    lot_size_m2 DECIMAL(10, 2),
    
    -- Property characteristics
    floor_number INTEGER,
    total_floors INTEGER,
    age_years INTEGER,
    construction_year INTEGER,
    
    -- Features and amenities (structured to mirror webpage categories)
    features JSONB,
    amenities JSONB, -- Will contain structured categories like: {"exterior": ["covered_parking", "street_parking"], "general": ["accessibility_for_elderly", "laundry_room"], "policies": ["pets_allowed"], "recreation": ["pool", "tennis_court"]}
    
    -- Media
    main_image_url TEXT,
    image_urls JSONB, -- Array of image URLs
    virtual_tour_url TEXT,
    video_url TEXT,
    
    -- Agent/Contact information
    agent_name VARCHAR(255),
    agent_phone VARCHAR(50),
    agent_email VARCHAR(255),
    agency_name VARCHAR(255),
    message_url TEXT, -- URL for "send message" button
    
    -- Property status
    status VARCHAR(50) DEFAULT 'active', -- active, sold, rented, inactive
    is_featured BOOLEAN DEFAULT false,
    is_premium BOOLEAN DEFAULT false,
    
    -- Metadata
    source_url TEXT NOT NULL,
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()),
    page_number INTEGER,
    listing_date DATE,
    
    -- SEO and search optimization
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('spanish', COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(neighborhood, '') || ' ' || COALESCE(city, ''))
    ) STORED
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_pulled_properties_property_type ON pulled_properties(property_type);
CREATE INDEX IF NOT EXISTS idx_pulled_properties_operation_type ON pulled_properties(operation_type);
CREATE INDEX IF NOT EXISTS idx_pulled_properties_city ON pulled_properties(city);
CREATE INDEX IF NOT EXISTS idx_pulled_properties_neighborhood ON pulled_properties(neighborhood);
CREATE INDEX IF NOT EXISTS idx_pulled_properties_price ON pulled_properties(price);
CREATE INDEX IF NOT EXISTS idx_pulled_properties_bedrooms ON pulled_properties(bedrooms);
CREATE INDEX IF NOT EXISTS idx_pulled_properties_status ON pulled_properties(status);
CREATE INDEX IF NOT EXISTS idx_pulled_properties_scraped_at ON pulled_properties(scraped_at);
CREATE INDEX IF NOT EXISTS idx_pulled_properties_latitude ON pulled_properties(latitude) WHERE latitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pulled_properties_longitude ON pulled_properties(longitude) WHERE longitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pulled_properties_search ON pulled_properties USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_pulled_properties_amenities ON pulled_properties USING GIN(amenities);

-- Create a trigger to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = TIMEZONE('utc'::text, NOW());
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_pulled_properties_updated_at 
    BEFORE UPDATE ON pulled_properties 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Create a table for scraping sessions to track progress
CREATE TABLE IF NOT EXISTS scraping_sessions (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    session_name VARCHAR(255),
    base_url TEXT NOT NULL,
    total_pages INTEGER,
    pages_scraped INTEGER DEFAULT 0,
    properties_found INTEGER DEFAULT 0,
    properties_inserted INTEGER DEFAULT 0,
    properties_updated INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'running', -- running, completed, failed, paused
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()),
    completed_at TIMESTAMP WITH TIME ZONE,
    filters_applied JSONB
);

-- Create a table for tracking errors and failed URLs
CREATE TABLE IF NOT EXISTS scraping_errors (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    session_id UUID REFERENCES scraping_sessions(id),
    url TEXT NOT NULL,
    error_type VARCHAR(100),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    resolved BOOLEAN DEFAULT false
);

-- Enable Row Level Security (RLS) - optional but recommended
ALTER TABLE pulled_properties ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraping_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraping_errors ENABLE ROW LEVEL SECURITY;

-- Create policies for RLS (adjust based on your needs)
CREATE POLICY "Enable read access for all users" ON pulled_properties FOR SELECT USING (true);
CREATE POLICY "Enable insert access for all users" ON pulled_properties FOR INSERT WITH CHECK (true);
CREATE POLICY "Enable update access for all users" ON pulled_properties FOR UPDATE USING (true);

CREATE POLICY "Enable read access for all users" ON scraping_sessions FOR SELECT USING (true);
CREATE POLICY "Enable insert access for all users" ON scraping_sessions FOR INSERT WITH CHECK (true);
CREATE POLICY "Enable update access for all users" ON scraping_sessions FOR UPDATE USING (true);

CREATE POLICY "Enable read access for all users" ON scraping_errors FOR SELECT USING (true);
CREATE POLICY "Enable insert access for all users" ON scraping_errors FOR INSERT WITH CHECK (true);
CREATE POLICY "Enable update access for all users" ON scraping_errors FOR UPDATE USING (true);

-- Migration script to remove old boolean amenity columns (run after updating the schema)
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_pool;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_garden;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_elevator;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_balcony;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_terrace;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_gym;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS has_security;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS pet_friendly;
-- ALTER TABLE pulled_properties DROP COLUMN IF EXISTS furnished; 