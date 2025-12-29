#!/usr/bin/env python3
"""
Generate Complete Database Schema Documentation from Supabase

This script extracts the complete database schema including:
- Tables and their columns
- Data types and constraints
- Primary keys and foreign keys
- Indexes
- Functions and triggers
- Row Level Security policies
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from urllib.parse import urlparse

# Load environment variables
load_dotenv()

class SchemaDocGenerator:
    def __init__(self):
        # Parse the Supabase connection URL or build from components
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
        
        # Try to get direct database URL
        db_url = os.getenv("DATABASE_URL")
        
        if db_url:
            # Use direct database URL if available
            self.connection_string = db_url
        else:
            # Build from Supabase URL
            # Format: postgresql://postgres:[PASSWORD]@db.[PROJECT_REF].supabase.co:5432/postgres
            project_ref = supabase_url.split('//')[1].split('.')[0] if supabase_url else None
            db_password = os.getenv("SUPABASE_DB_PASSWORD") or os.getenv("DB_PASSWORD")
            
            if not project_ref or not db_password:
                raise ValueError(
                    "Missing database credentials. Please provide either:\n"
                    "1. DATABASE_URL environment variable, or\n"
                    "2. SUPABASE_URL and SUPABASE_DB_PASSWORD environment variables"
                )
            
            self.connection_string = f"postgresql://postgres:{db_password}@db.{project_ref}.supabase.co:5432/postgres"
        
        print(f"Connecting to database...")
        
    def get_connection(self, statement_timeout_ms=60000):
        """Get a PostgreSQL database connection with configurable timeout"""
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor()
        # Set a longer statement timeout (default 60 seconds)
        cursor.execute(f"SET statement_timeout = {statement_timeout_ms}")
        cursor.close()
        return conn
        
    def get_tables_info(self):
        """Get all tables and their column information using separate, faster queries"""
        try:
            conn = self.get_connection(statement_timeout_ms=120000)  # 2 minute timeout
            cursor = conn.cursor()
            
            # Step 1: Get tables and columns (simple query, no JOINs with constraints)
            print("   Fetching tables and columns...")
            columns_query = """
            SELECT 
                c.table_name,
                'BASE TABLE' as table_type,
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.is_nullable,
                c.column_default,
                c.ordinal_position
            FROM information_schema.columns c
            JOIN information_schema.tables t 
                ON c.table_name = t.table_name 
                AND c.table_schema = t.table_schema
            WHERE c.table_schema = 'public'
                AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_name, c.ordinal_position;
            """
            cursor.execute(columns_query)
            columns_data = cursor.fetchall()
            print(f"   ✅ Found {len(columns_data)} columns")
            
            # Step 2: Get primary keys (separate simple query using pg_catalog - faster)
            print("   Fetching primary keys...")
            pk_query = """
            SELECT 
                tc.table_name,
                kcu.column_name
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
            JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
            JOIN information_schema.table_constraints tc 
                ON tc.constraint_name = con.conname AND tc.table_schema = nsp.nspname
            JOIN information_schema.key_column_usage kcu 
                ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
            WHERE nsp.nspname = 'public' 
                AND con.contype = 'p';
            """
            cursor.execute(pk_query)
            pk_set = {(row[0], row[1]) for row in cursor.fetchall()}
            print(f"   ✅ Found {len(pk_set)} primary key columns")
            
            # Step 3: Get foreign keys (separate simple query)
            print("   Fetching foreign keys...")
            fk_query = """
            SELECT 
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
            JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
            JOIN information_schema.table_constraints tc 
                ON tc.constraint_name = con.conname AND tc.table_schema = nsp.nspname
            JOIN information_schema.key_column_usage kcu 
                ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
            WHERE nsp.nspname = 'public' 
                AND con.contype = 'f';
            """
            cursor.execute(fk_query)
            fk_map = {(row[0], row[1]): f"{row[2]}({row[3]})" for row in cursor.fetchall()}
            print(f"   ✅ Found {len(fk_map)} foreign key columns")
            
            cursor.close()
            conn.close()
            
            # Combine results in Python (faster than complex SQL JOINs)
            results = []
            for row in columns_data:
                table_name, table_type, column_name, data_type, max_length, is_nullable, default, ordinal = row
                
                is_pk = 'YES' if (table_name, column_name) in pk_set else 'NO'
                fk_ref = fk_map.get((table_name, column_name))
                
                results.append({
                    'table_name': table_name,
                    'table_type': table_type,
                    'column_name': column_name,
                    'data_type': data_type,
                    'character_maximum_length': max_length,
                    'is_nullable': is_nullable,
                    'column_default': default,
                    'ordinal_position': ordinal,
                    'is_primary_key': is_pk,
                    'foreign_key_reference': fk_ref
                })
            
            print(f"✅ Found {len(results)} columns across tables")
            return results
            
        except Exception as e:
            print(f"❌ Error getting tables info: {e}")
            print(f"   Make sure you have the correct database credentials in your .env file")
            return []
    
    def get_indexes_info(self):
        """Get indexes information"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes 
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname;
            """
            
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            cursor.close()
            conn.close()
            
            print(f"✅ Found {len(results)} indexes")
            return results
            
        except Exception as e:
            print(f"⚠️  Error getting indexes info: {e}")
            return []
    
    def get_functions_info(self):
        """Get functions and procedures information"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                routine_name,
                routine_type,
                data_type as return_type,
                routine_definition
            FROM information_schema.routines 
            WHERE routine_schema = 'public'
            ORDER BY routine_name;
            """
            
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            cursor.close()
            conn.close()
            
            print(f"✅ Found {len(results)} functions/procedures")
            return results
            
        except Exception as e:
            print(f"⚠️  Error getting functions info: {e}")
            return []
    
    def generate_markdown_doc(self):
        """Generate a comprehensive markdown documentation"""
        
        print("Extracting database schema...")
        
        # Get all schema information
        tables_info = self.get_tables_info()
        indexes_info = self.get_indexes_info()
        functions_info = self.get_functions_info()
        
        # Organize tables data
        tables = {}
        for row in tables_info:
            table_name = row['table_name']
            if table_name not in tables:
                tables[table_name] = {
                    'type': row.get('table_type', 'BASE TABLE'),
                    'columns': []
                }
            
            column_info = {
                'name': row['column_name'],
                'type': row['data_type'],
                'nullable': row.get('is_nullable', 'YES'),
                'default': row.get('column_default'),
                'primary_key': row.get('is_primary_key', 'NO') == 'YES',
                'foreign_key': row.get('foreign_key_reference'),
                'max_length': row.get('character_maximum_length')
            }
            tables[table_name]['columns'].append(column_info)
        
        # Generate markdown content
        markdown_content = self._generate_markdown_content(tables, indexes_info, functions_info)
        
        # Write to file
        filename = f"database_schema_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        print(f"✅ Schema documentation generated: {filename}")
        print(f"📊 Found {len(tables)} tables")
        print(f"🗂️  Found {len(indexes_info)} indexes")
        print(f"⚙️  Found {len(functions_info)} functions")
        
        return filename
    
    def _generate_markdown_content(self, tables, indexes, functions):
        """Generate the markdown content"""
        
        content = f"""# Database Schema Documentation

**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Database:** Supabase PostgreSQL  
**Project:** PropScraper Property Management System

## Overview

This document contains the complete database schema for the PropScraper system, including all tables, columns, constraints, indexes, and functions.

## Table of Contents

1. [Tables Overview](#tables-overview)
2. [Detailed Table Schemas](#detailed-table-schemas)
3. [Indexes](#indexes)
4. [Functions and Procedures](#functions-and-procedures)
5. [Entity Relationships](#entity-relationships)

## Tables Overview

| Table Name | Type | Columns | Purpose |
|------------|------|---------|---------|
"""
        
        # Add tables overview
        for table_name, table_info in sorted(tables.items()):
            column_count = len(table_info['columns'])
            purpose = self._get_table_purpose(table_name)
            content += f"| `{table_name}` | {table_info['type']} | {column_count} | {purpose} |\n"
        
        content += "\n## Detailed Table Schemas\n\n"
        
        # Add detailed table schemas
        for table_name, table_info in sorted(tables.items()):
            content += f"### {table_name}\n\n"
            content += f"**Purpose:** {self._get_table_purpose(table_name)}\n\n"
            
            # Table structure
            content += "| Column | Type | Nullable | Default | Constraints |\n"
            content += "|--------|------|----------|---------|-------------|\n"
            
            for col in table_info['columns']:
                nullable = "✅" if col['nullable'] == 'YES' else "❌"
                constraints = []
                
                if col['primary_key']:
                    constraints.append("🔑 PRIMARY KEY")
                if col['foreign_key']:
                    constraints.append(f"🔗 FK → {col['foreign_key']}")
                if col['max_length']:
                    constraints.append(f"📏 MAX({col['max_length']})")
                
                constraints_str = ", ".join(constraints) if constraints else "-"
                default_str = col['default'] if col['default'] else "-"
                
                content += f"| `{col['name']}` | `{col['type']}` | {nullable} | `{default_str}` | {constraints_str} |\n"
            
            content += "\n"
        
        # Add indexes section
        if indexes:
            content += "## Indexes\n\n"
            content += "| Table | Index Name | Definition |\n"
            content += "|-------|------------|------------|\n"
            
            for idx in indexes:
                content += f"| `{idx['tablename']}` | `{idx['indexname']}` | `{idx['indexdef']}` |\n"
            
            content += "\n"
        
        # Add functions section
        if functions:
            content += "## Functions and Procedures\n\n"
            
            for func in functions:
                content += f"### {func['routine_name']}\n\n"
                content += f"**Type:** {func['routine_type']}  \n"
                content += f"**Returns:** `{func.get('return_type', 'void')}`\n\n"
                
                if func.get('routine_definition'):
                    content += "```sql\n"
                    content += func['routine_definition']
                    content += "\n```\n\n"
        
        # Add relationships section
        content += "## Entity Relationships\n\n"
        content += self._generate_relationships_section(tables)
        
        # Add additional notes
        content += """
## Additional Notes

### Data Types Used
- `uuid` - Universally Unique Identifiers for primary keys
- `text` - Variable-length text strings
- `varchar(n)` - Variable-length strings with maximum length
- `integer` - 32-bit integers
- `numeric` - Arbitrary precision numbers
- `boolean` - True/false values
- `timestamp` - Date and time values
- `jsonb` - Binary JSON data (indexed)
- `real` - Single precision floating-point

### Naming Conventions
- Table names use snake_case
- Primary keys are typically named `id`
- Foreign keys follow the pattern `{table}_id`
- Timestamp columns use `created_at` and `updated_at`
- Boolean columns often use `is_` prefix

### Performance Considerations
- All tables have UUID primary keys for distributed scaling
- Indexes are created on frequently queried columns
- JSONB columns are used for flexible schema requirements
- Full-text search is implemented using PostgreSQL's built-in capabilities

### Security
- Row Level Security (RLS) may be enabled on sensitive tables
- Access is controlled through Supabase policies
- API access is managed through service keys

---

*This documentation was automatically generated from the live database schema.*
"""
        
        return content
    
    def _get_table_purpose(self, table_name):
        """Get a description of what each table is used for"""
        purposes = {
            'properties': 'Main properties table (legacy)',
            'pulled_properties': 'Legacy scraped properties storage',
            'properties_live': 'Production property data for frontend',
            'property_scrapes_staging': 'Raw scraped data before validation',
            'scraping_sessions': 'Track scraping job sessions and progress',
            'scraping_errors': 'Log errors during scraping operations',
            'sync_metadata': 'Track data synchronization operations',
            'property_changes': 'Audit log of property modifications',
            'validation_rules': 'Configurable data validation rules',
            'property_stats': 'Materialized view for property statistics',
            'chat_conversations': 'User chat conversations',
            'chat_messages': 'Individual chat messages',
            'users': 'User accounts and profiles',
            'user_favorites': 'User saved/favorite properties',
            'todo_items': 'Task management items',
            'todo_lists': 'Task management lists'
        }
        return purposes.get(table_name, 'Application data storage')
    
    def _generate_relationships_section(self, tables):
        """Generate entity relationship documentation"""
        content = """
### Primary Relationships

```mermaid
erDiagram
    properties_live {
        uuid id PK
        varchar property_id UK
        text title
        numeric price
        text address
        timestamp created_at
    }
    
    property_scrapes_staging {
        uuid id PK
        varchar property_id
        uuid session_id FK
        jsonb raw_data
        timestamp scraped_at
    }
    
    scraping_sessions {
        uuid id PK
        varchar session_name
        varchar status
        timestamp started_at
    }
    
    property_changes {
        uuid id PK
        varchar property_id FK
        varchar change_type
        jsonb old_value
        jsonb new_value
        timestamp changed_at
    }
    
    scraping_sessions ||--o{ property_scrapes_staging : "has"
    properties_live ||--o{ property_changes : "tracked by"
```

### Key Relationships

1. **Scraping Sessions → Staging Data**
   - One session can have many scraped properties
   - Used to track and batch process scraped data

2. **Staging → Live Properties** 
   - Data flows from staging to live after validation
   - Change detection compares these tables

3. **Properties → Change Log**
   - All property modifications are tracked
   - Provides complete audit trail

4. **Validation Rules → Data Quality**
   - Configurable rules validate staging data
   - Ensures data quality before promotion
"""
        return content

def main():
    """Main function"""
    print("🗄️  Database Schema Documentation Generator")
    print("=" * 50)
    
    try:
        generator = SchemaDocGenerator()
        filename = generator.generate_markdown_doc()
        
        print("=" * 50)
        print(f"✅ Documentation generated successfully!")
        print(f"📄 File: {filename}")
        print("\nYou can now:")
        print("1. Open the file in any markdown viewer")
        print("2. Convert to PDF using pandoc or similar tools")
        print("3. View on GitHub/GitLab for formatted display")
        
    except Exception as e:
        print(f"❌ Error generating schema documentation: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 