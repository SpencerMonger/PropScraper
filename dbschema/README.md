# Database Schema Documentation Generator

This script generates comprehensive database schema documentation from your Supabase PostgreSQL database.

## Prerequisites

1. **Install Required Packages**
   ```bash
   pip install psycopg2-binary python-dotenv
   ```
   Or install all project requirements:
   ```bash
   pip install -r ../requirements.txt
   ```

2. **Database Credentials**
   
   You need to add your database credentials to your `.env` file. There are two ways to do this:

   ### Option 1: Use DATABASE_URL (Recommended)
   ```env
   DATABASE_URL=postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
   ```

   ### Option 2: Use Individual Variables
   ```env
   SUPABASE_URL=https://[YOUR-PROJECT-REF].supabase.co
   SUPABASE_DB_PASSWORD=[YOUR-DB-PASSWORD]
   ```

## How to Get Your Database Credentials

### From Supabase Dashboard:

1. Go to your [Supabase Dashboard](https://app.supabase.com/)
2. Select your project
3. Go to **Settings** → **Database**
4. Scroll down to **Connection String** section
5. Select **URI** tab
6. Copy the connection string (it looks like: `postgresql://postgres:[YOUR-PASSWORD]@db.xxxxx.supabase.co:5432/postgres`)
7. Replace `[YOUR-PASSWORD]` with your actual database password

### Database Password Location:
- The password is shown when you first create the project
- You can reset it in **Settings** → **Database** → **Database Password** → **Reset Database Password**

## Usage

Run the script from the `dbschema` directory:

```bash
cd dbschema
python generate_schema_doc.py
```

Or from the project root:

```bash
python dbschema/generate_schema_doc.py
```

## Output

The script will generate a markdown file named `database_schema_YYYYMMDD_HHMMSS.md` containing:

- Complete table schemas with all columns, data types, and constraints
- Primary keys and foreign keys
- All indexes
- Database functions and procedures
- Entity relationship diagrams
- Data type documentation
- Performance considerations

## What Was Fixed

The previous version had issues because:

1. ❌ It tried to use a non-existent RPC function `execute_sql`
2. ❌ The fallback method only sampled data from tables
3. ❌ Empty tables showed only 1 column
4. ❌ NULL values were marked as `unknown` type

The new version:

1. ✅ Uses direct PostgreSQL connection via `psycopg2`
2. ✅ Queries `information_schema` directly
3. ✅ Gets actual column definitions from the database
4. ✅ Works even with empty tables
5. ✅ Shows proper data types for all columns

## Troubleshooting

### Error: "Missing database credentials"
- Make sure your `.env` file contains either `DATABASE_URL` or both `SUPABASE_URL` and `SUPABASE_DB_PASSWORD`

### Error: "connection refused" or "could not connect"
- Check that your database password is correct
- Verify your project reference is correct in the connection string
- Ensure your IP is allowed (Supabase allows all IPs by default)

### Error: "module 'psycopg2' has no attribute 'connect'"
- Install psycopg2-binary: `pip install psycopg2-binary`
- Uninstall psycopg2 if you have it: `pip uninstall psycopg2`

## Example .env File

Create a `.env` file in your project root:

```env
# Supabase Configuration
SUPABASE_URL=https://xxxxxxxxxxxxx.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
SUPABASE_SERVICE_ROLE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# Database Direct Connection (REQUIRED for schema generator)
DATABASE_URL=postgresql://postgres:your-db-password@db.xxxxxxxxxxxxx.supabase.co:5432/postgres

# OR use these individual variables:
# SUPABASE_DB_PASSWORD=your-db-password
``` 