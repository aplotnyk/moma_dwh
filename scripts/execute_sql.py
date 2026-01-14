"""
Executes SQL DDL scripts to create database schemas
"""
import psycopg2
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_CONFIG, FILES

def get_db_connection():
    """
    Create database connection
    
    Returns:
        psycopg2 connection object
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        raise

def execute_sql_file(sql_file_path: Path, description: str = "SQL script") -> bool:
    """
    Execute SQL from a file
    
    Args:
        sql_file_path: Path to SQL file
        description: Human-readable description for logging
        
    Returns:
        True if successful
        
    Raises:
        Exception if execution fails
    """
    print("\n" + "="*60)
    print(f"Executing: {description}")
    print("="*60)
    print(f"File: {sql_file_path}")
    
    # Check if file exists
    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
    
    try:
        # Read SQL file
        print(f"📖 Reading SQL file...")
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        print(f"   File size: {len(sql)} characters")
        
        # Execute SQL
        print(f"🔧 Executing SQL...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Execute the entire script
        cursor.execute(sql)
        conn.commit()
        
        # Get row count if available
        if cursor.rowcount >= 0:
            print(f"   Affected rows: {cursor.rowcount}")
        
        cursor.close()
        conn.close()
        
        print(f"✅ {description} executed successfully!")
        return True
        
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL error executing {description}:")
        print(f"   Error code: {e.pgcode}")
        print(f"   Error message: {e.pgerror}")
        raise
    except Exception as e:
        print(f"❌ Error executing {description}: {e}")
        raise

def create_staging_schema() -> bool:
    """
    Create staging schema and tables
    
    Returns:
        True if successful
    """
    sql_file = FILES['staging_schema_sql']
    return execute_sql_file(sql_file, "Staging Schema DDL")

def create_transformed_schema() -> bool:
    """
    Create transformed schema and tables
    
    Returns:
        True if successful
    """
    sql_file = FILES['transformed_schema_sql']
    return execute_sql_file(sql_file, "Transformed Schema DDL")

def create_dimensional_schema() -> bool:
    """
    Create dimensional schema and tables
    
    Returns:
        True if successful
    """
    sql_file = FILES['dimensional_schema_sql']
    return execute_sql_file(sql_file, "Dimensional Schema DDL")

def create_all_schemas() -> dict:
    """
    Create all schemas in order
    
    Returns:
        Dictionary with results for each schema
    """
    print("\n" + "="*60)
    print("Creating All Database Schemas")
    print("="*60)
    
    results = {}
    
    try:
        # Create schemas in order
        print("\n🏗️  Creating staging schema...")
        results['staging'] = create_staging_schema()
        
        print("\n🏗️  Creating transformed schema...")
        results['transformed'] = create_transformed_schema()
        
        print("\n🏗️  Creating dimensional schema...")
        results['dimensional'] = create_dimensional_schema()
        
        print("\n" + "="*60)
        print("✅ All Schemas Created Successfully!")
        print("="*60)
        
        # Verify schemas exist
        verify_schemas()
        
        return results
        
    except Exception as e:
        print("\n" + "="*60)
        print("❌ Schema Creation Failed!")
        print("="*60)
        print(f"Error: {e}")
        raise

def verify_schemas() -> None:
    """
    Verify that all schemas and tables were created
    """
    print("\n" + "="*60)
    print("Verifying Schema Creation")
    print("="*60)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check schemas
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('staging', 'transformed', 'dimensional')
            ORDER BY schema_name
        """)
        schemas = cursor.fetchall()
        
        print("\n📁 Schemas created:")
        for schema in schemas:
            print(f"   ✅ {schema[0]}")
        
        # Check tables in each schema
        for schema_name in ['staging', 'transformed', 'dimensional']:
            cursor.execute("""
                SELECT table_name, 
                       (SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_schema = %s 
                          AND table_name = t.table_name) as column_count
                FROM information_schema.tables t
                WHERE table_schema = %s
                ORDER BY table_name
            """, (schema_name, schema_name))
            
            tables = cursor.fetchall()
            
            if tables:
                print(f"\n📊 Tables in {schema_name} schema:")
                for table_name, col_count in tables:
                    print(f"   ✅ {table_name} ({col_count} columns)")
            else:
                print(f"\n⚠️  No tables found in {schema_name} schema")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        raise

def drop_all_schemas() -> None:
    """
    Drop all schemas (CAUTION!)
    To be used for testing/resetting
    """
    print("\n" + "="*60)
    print("⚠️  DROPPING ALL SCHEMAS (with CASCADE)")
    print("="*60)
    
    confirmation = input("Are you sure? Type 'YES' to confirm: ")
    
    if confirmation != 'YES':
        print("❌ Cancelled")
        return
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Drop schemas with cascade (removes all tables)
        for schema in ['dimensional', 'transformed', 'staging']:
            print(f"🗑️  Dropping {schema} schema...")
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ All schemas dropped successfully")
        
    except Exception as e:
        print(f"❌ Error dropping schemas: {e}")
        raise

# For testing the script directly
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Execute MoMA SQL DDL scripts')
    parser.add_argument('--schema', 
                       choices=['staging', 'transformed', 'dimensional', 'all'],
                       default='all',
                       help='Which schema to create (default: all)')
    parser.add_argument('--drop', 
                       action='store_true',
                       help='Drop all schemas first (DANGEROUS!)')
    
    args = parser.parse_args()
    
    try:
        # Drop schemas if requested
        if args.drop:
            drop_all_schemas()
        
        # Create schemas
        if args.schema == 'staging':
            create_staging_schema()
        elif args.schema == 'transformed':
            create_transformed_schema()
        elif args.schema == 'dimensional':
            create_dimensional_schema()
        else:  # all
            create_all_schemas()
        
        print("\n✅ Script completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Script failed: {e}")
        sys.exit(1)