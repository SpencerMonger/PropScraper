from supabase import create_client
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase client
client = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_ANON_KEY'))

# Check staging table
try:
    result = client.table('property_scrapes_staging').select('*').limit(5).execute()
    print(f"Found {len(result.data)} properties in staging table")
    
    if result.data:
        print("\nFirst property:")
        for key, value in result.data[0].items():
            print(f"  {key}: {value}")
    
    # Check sessions table
    sessions = client.table('scraping_sessions').select('*').order('created_at', desc=True).limit(3).execute()
    print(f"\nFound {len(sessions.data)} recent sessions")
    
    if sessions.data:
        print("\nMost recent session:")
        session = sessions.data[0]
        for key, value in session.items():
            print(f"  {key}: {value}")

except Exception as e:
    print(f"Error: {e}") 