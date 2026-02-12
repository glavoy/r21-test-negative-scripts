"""
R21 Test Negative - Complete Data Processing Pipeline
Fetches data from FTP, imports to SQL Server, exports, and zips results.
"""

import ftplib
import json
import re
import zipfile
import sqlite3
import csv
import pyodbc
import shutil
from pathlib import Path
from datetime import datetime
import sys
import socket


# ============================================================================
# CONFIGURATION
# ============================================================================

# Data folders to process (add new folder numbers here as needed)
DATA_FOLDERS = ['13', '14']

# Tables to process (add new tables here as needed)
TABLE_NAMES = ['enrollee', 'formchanges']

# SQL Server configuration
SQL_SERVER = 'PONTIAC'
SQL_DATABASE = 'r21_neg'

# Paths
DATA_ROOT = Path('D:/R21TestNegative/data')
EXPORT_PATH = DATA_ROOT / 'export'
ZIP_OUTPUT_PATH = Path('C:/inetpub/wwwroot/r21neg/wwwroot/downloads/r21_neg_data.zip')

# SQL scripts path
SQL_SCRIPTS_PATH = Path(__file__).parent / 'sql'

# Field configurations
FIELD_CONFIG = {
    frozenset(['starttime', 'startdate', 'deviceid', 'deviceid2', 'mrc', 'subjid', 'district', 'mayuge_warning', 'busia_warning', 'subcounty', 'parish', 'village', 'placeofresidence', 'dob', 'age_calculated', 'agemonths_calculated', 'age_at_apr2025', 'age_in_range', 'age', 'agemonths', 'age_eligible', 'age_warning', 'age_warning_reason', 'mal_test_eligible', 'consent_eligible', 'participantsname', 'gender', 'hhheadeduclevel', 'diagnostic', 'result', 'vx_card', 'vx_any', 'vx_any_no', 'vx_any_no_oth', 'vx_doses_received', 'vx_doses_received_ver', 'vx_doses_received_ver_oth', 'vx_dose1_where', 'vx_dose1_where_oth', 'vx_dose1_date', 'vx_dose1_date_ver', 'vx_dose1_date_ver_oth', 'vx_dose2_where', 'vx_dose2_where_oth', 'vx_dose2_date', 'dose2_warning_time', 'vx_dose2_datecheck', 'vx_dose2_date_ver', 'vx_dose2_date_ver_oth', 'vx_dose3_where', 'vx_dose3_where_oth', 'vx_dose3_date', 'dose3_warning_time', 'vx_dose3_datecheck', 'vx_dose3_date_ver', 'vx_dose3_date_ver_oth', 'vx_dose4_where', 'vx_dose4_where_oth', 'vx_dose4_date', 'dose4_warning_time', 'vx_dose4_datecheck', 'vx_dose4_date_ver', 'vx_dose4_date_ver_oth', 'vx_dose_summary', 'vx_doses_miss', 'vx_doses_miss_reas', 'vx_doses_miss_reas_oth', 'vx_dose_off_sched', 'vx_doses_offsched_reas', 'vx_offsched_miss_reas_oth', 'hib_any', 'hib_doses_received', 'hib_doses_received_ver', 'hib_doses_received_ver_oth', 'hib_dose1_where', 'hib_dose1_where_oth', 'hib_dose1_date', 'hib_dose1_date_ver', 'hib_dose1_date_ver_oth', 'hib_dose2_where', 'hib_dose2_where_oth', 'hib_dose2_date', 'hibdose2_warning_time', 'hib_dose2_datecheck', 'hib_dose2_date_ver', 'hib_dose2_date_ver_oth', 'hib_dose3_where', 'hib_dose3_where_oth', 'hib_dose3_date', 'hibdose3_warning_time', 'hib_dose3_datecheck', 'hib_dose3_date_ver', 'hib_dos32_date_ver_oth', 'hib_dose_summary', 'timetobed', 'spray', 'bednetlastnight', 'bednettwoweeks', 'prevdiag', 'prevdiag_when', 'prevdiag_lastmonth', 'prevdiag_al', 'prevdiag_othermeds', 'prevdiag_descothermed', 'consent', 'consent2', 'uid', 'uid2', 'fpbarcode1_r21', 'fpbarcode2_r21', 'enroll_immrse', 'fpbarcode1_immrse', 'fpbarcode2_immrse', 'comments', 'uniqueid', 'swver', 'survey_id', 'lastmod', 'stoptime']): 'copy_enrollee_staging_to_main_2026-01-05.sql',
    frozenset(['starttime', 'startdate', 'deviceid', 'deviceid2', 'mrc', 'subjid', 'district', 'mayuge_warning', 'busia_warning', 'subcounty', 'parish', 'village', 'placeofresidence', 'dob', 'age_calculated', 'agemonths_calculated', 'age_at_apr2025', 'age_in_range', 'age', 'agemonths', 'age_eligible', 'age_warning', 'age_warning_reason', 'mal_test_eligible', 'ill_noteligible', 'consent_eligible', 'participantsname', 'gender', 'hhheadeduclevel', 'diagnostic', 'result', 'vx_card', 'vx_card_no', 'vx_card_no_oth', 'vx_any', 'vx_any_no', 'vx_any_no_oth', 'vx_doses_received', 'vx_doses_received_ver', 'vx_doses_received_ver_oth', 'vx_dose1_where', 'vx_dose1_where_oth', 'vx_dose1_date', 'vx_dose1_date_ver', 'vx_dose1_date_ver_oth', 'vx_dose2_where', 'vx_dose2_where_oth', 'vx_dose2_date', 'dose2_warning_time', 'vx_dose2_datecheck', 'vx_dose2_date_ver', 'vx_dose2_date_ver_oth', 'vx_dose3_where', 'vx_dose3_where_oth', 'vx_dose3_date', 'dose3_warning_time', 'vx_dose3_datecheck', 'vx_dose3_date_ver', 'vx_dose3_date_ver_oth', 'vx_dose4_where', 'vx_dose4_where_oth', 'vx_dose4_date', 'dose4_warning_time', 'vx_dose4_datecheck', 'vx_dose4_date_ver', 'vx_dose4_date_ver_oth', 'vx_dose_summary', 'vx_doses_miss', 'vx_doses_miss_reas', 'vx_doses_miss_reas_oth', 'vx_dose_off_sched', 'vx_doses_offsched_reas', 'vx_offsched_miss_reas_oth', 'hib_any', 'hib_doses_received', 'hib_doses_received_ver', 'hib_doses_received_ver_oth', 'hib_dose1_where', 'hib_dose1_where_oth', 'hib_dose1_date', 'hib_dose1_date_ver', 'hib_dose1_date_ver_oth', 'hib_dose2_where', 'hib_dose2_where_oth', 'hib_dose2_date', 'hibdose2_warning_time', 'hib_dose2_datecheck', 'hib_dose2_date_ver', 'hib_dose2_date_ver_oth', 'hib_dose3_where', 'hib_dose3_where_oth', 'hib_dose3_date', 'hibdose3_warning_time', 'hib_dose3_datecheck', 'hib_dose3_date_ver', 'hib_dos32_date_ver_oth', 'hib_dose_summary', 'timetobed', 'spray', 'bednetlastnight', 'bednettwoweeks', 'prevdiag', 'prevdiag_when', 'prevdiag_lastmonth', 'prevdiag_al', 'prevdiag_othermeds', 'prevdiag_descothermed', 'consent', 'consent2', 'uid', 'uid2', 'fpbarcode1_r21', 'fpbarcode2_r21', 'enroll_immrse', 'fpbarcode1_immrse', 'fpbarcode2_immrse', 'comments', 'uniqueid', 'swver', 'survey_id', 'lastmod', 'stoptime']): 'copy_enrollee_staging_to_main_2026-02-02.sql'
}


# ============================================================================
# LOGGING SETUP
# ============================================================================

def log(message, level="INFO"):
    """Simple logging function."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {level}: {message}")
    sys.stdout.flush()


def log_section(title):
    """Log a section header."""
    print("\n" + "=" * 60)
    log(title)
    print("=" * 60)


def log_error(message):
    """Log an error message."""
    log(message, "ERROR")


def log_success(message):
    """Log a success message."""
    log(message, "SUCCESS")


# ============================================================================
# STEP 1: FTP DOWNLOAD FUNCTIONS (from existing script)
# ============================================================================

def load_config():
    """Load FTP configuration from config.json."""
    script_dir = Path(__file__).parent
    config_path = script_dir / "config.json"
    
    with open(config_path, 'r') as f:
        return json.load(f)


def parse_filename(filename):
    """Parse the zip filename to extract components."""
    pattern = r'^r21_test_negative_(\d{4}-\d{2}-\d{2})_(\d{2,4})_(\d{4}-\d{2}-\d{2})_(\d{2})_(\d{2})\.zip$'
    match = re.match(pattern, filename)
    
    if match:
        version_date = match.group(1)
        number = match.group(2)
        timestamp_date = match.group(3)
        timestamp_hour = match.group(4)
        timestamp_minute = match.group(5)
        
        timestamp_str = f"{timestamp_date} {timestamp_hour}:{timestamp_minute}"
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
        
        return {
            'version': version_date,
            'number': number,
            'timestamp': timestamp,
            'filename': filename
        }
    return None


def ensure_folder_exists(folder_path):
    """Create folder if it doesn't exist."""
    folder_path.mkdir(parents=True, exist_ok=True)


def download_files_from_ftp(config):
    """Connect to FTP and download new zip files."""
    host = config['ftp_host']
    username = config['ftp_username']
    password = config['ftp_password']
    
    # Set FTP timeout (in seconds) - read from config or default to 60
    FTP_TIMEOUT = config.get('ftp_timeout', 60)
    
    downloaded_files = {}
    
    log(f"Connecting to FTP server: {host}")
    
    with ftplib.FTP(host, timeout=FTP_TIMEOUT) as ftp:
        ftp.login(username, password)
        log("FTP login successful")
        
        ftp.cwd('data')
        log("Changed to 'data' directory")
        
        file_list = []
        ftp.retrlines('LIST', file_list.append)
        
        files_info = []
        for line in file_list:
            parts = line.split()
            if len(parts) >= 9:
                size = int(parts[4])
                filename = ' '.join(parts[8:])
                if filename.endswith('.zip'):
                    files_info.append({'filename': filename, 'size': size})
        
        log(f"Found {len(files_info)} zip files on server")
        
        files_by_number = {}
        for file_info in files_info:
            parsed = parse_filename(file_info['filename'])
            if parsed:
                number = parsed['number']
                if number not in files_by_number:
                    files_by_number[number] = []
                files_by_number[number].append({
                    **parsed,
                    'size': file_info['size']
                })
        
        for number, files in files_by_number.items():
            number_folder = DATA_ROOT / number
            ensure_folder_exists(number_folder)
            
            downloaded_files[number] = []
            
            for file_info in files:
                filename = file_info['filename']
                local_path = number_folder / filename
                
                if local_path.exists():
                    continue
                
                if file_info['size'] == 0:
                    log(f"Skipping empty file: {filename}")
                    continue
                
                log(f"Downloading: {filename} ({file_info['size']} bytes)")
                with open(local_path, 'wb') as f:
                    ftp.retrbinary(f'RETR {filename}', f.write)
                
                downloaded_files[number].append({
                    'path': local_path,
                    **file_info
                })
    
    return downloaded_files


def find_most_recent_zip(folder_path):
    """Find the most recent zip file in a folder."""
    zip_files = list(folder_path.glob('r21_test_negative_*.zip'))
    
    if not zip_files:
        return None
    
    parsed_files = []
    for zip_path in zip_files:
        parsed = parse_filename(zip_path.name)
        if parsed:
            file_mtime = datetime.fromtimestamp(zip_path.stat().st_mtime)
            parsed_files.append({
                'path': zip_path,
                'filename_timestamp': parsed['timestamp'],
                'file_mtime': file_mtime
            })
    
    if not parsed_files:
        return None
    
    parsed_files.sort(key=lambda x: (x['filename_timestamp'], x['file_mtime']), reverse=True)
    return parsed_files[0]['path']


def extract_zip(zip_path, extract_to):
    """Extract a zip file."""
    log(f"Extracting: {zip_path.name}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)


def find_sqlite_database(folder_path):
    """Find the SQLite database in a folder."""
    sqlite_files = list(folder_path.glob('*.sqlite'))
    if not sqlite_files:
        sqlite_files = list(folder_path.rglob('*.sqlite'))
    return sqlite_files[0] if sqlite_files else None


def export_sqlite_table_to_csv(db_path, table_name, output_path):
    """Export a SQLite table to a CSV file."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f'SELECT * FROM {table_name}')
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        writer.writerows(rows)
    
    conn.close()
    log(f"Exported {table_name} to {output_path.name} ({len(rows)} rows)")


def process_ftp_folder(folder_path):
    """Process a numbered folder: extract zip and export tables."""
    most_recent_zip = find_most_recent_zip(folder_path)
    
    if not most_recent_zip:
        log(f"No valid zip files found in {folder_path}")
        return False
    
    log(f"Processing: {most_recent_zip.name}")
    extract_zip(most_recent_zip, folder_path)
    
    db_path = find_sqlite_database(folder_path)
    if not db_path:
        log_error(f"No SQLite database found in {folder_path}")
        return False
    
    log(f"Found database: {db_path.name}")
    
    for table in TABLE_NAMES:
        csv_path = folder_path / f"{table}.csv"
        try:
            export_sqlite_table_to_csv(db_path, table, csv_path)
        except sqlite3.OperationalError as e:
            log_error(f"Error exporting {table}: {e}")
            return False
    
    return True


def step1_fetch_data():
    """Step 1: Fetch data from FTP server."""
    log_section("STEP 1: Fetching data from FTP server")
    
    try:
        config = load_config()
    except FileNotFoundError:
        log_error("config.json not found")
        return False
    except json.JSONDecodeError:
        log_error("config.json is not valid JSON")
        return False
    
    ensure_folder_exists(DATA_ROOT)
    
    try:
        downloaded_files = download_files_from_ftp(config)
    except ftplib.all_errors as e:
        log_error(f"FTP Error: {e}")
        return False
    
    folders_with_new_files = [
        number for number, files in downloaded_files.items() if files
    ]
    
    if not folders_with_new_files:
        log("No new files downloaded")
    else:
        log(f"Downloaded files for folders: {sorted(folders_with_new_files)}")
        for number in sorted(folders_with_new_files):
            folder_path = DATA_ROOT / number
            if not process_ftp_folder(folder_path):
                return False
    
    log_success("Step 1 completed successfully")
    return True


# ============================================================================
# STEP 2: SQL SERVER IMPORT FUNCTIONS
# ============================================================================

def get_sql_connection():
    """Create a connection to SQL Server."""
    conn_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_string)


def truncate_table(cursor, table_name):
    """Truncate a SQL Server table."""
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_after = cursor.fetchone()[0]
        if count_after == 0:
            log(f"Truncated table: {table_name} (now has {count_after} rows)")
        else:
            log_error(f"Failed to truncate {table_name}. {count_after} rows remaining.")
            raise Exception(f"Failed to truncate {table_name}")
    except pyodbc.Error as e:
        log_error(f"Failed to truncate {table_name}: {e}")
        raise


def get_csv_columns(csv_path):
    """Get column names from CSV file."""
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        return next(reader)


def get_table_columns(cursor, table_name):
    """Get column names from SQL Server table."""
    cursor.execute(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """)
    return [row[0] for row in cursor.fetchall()]


def verify_columns(csv_columns, table_columns, csv_file, table_name):
    """Verify that CSV columns match table columns."""
    csv_set = set(csv_columns)
    table_set = set(table_columns)
    
    if csv_set != table_set:
        missing_in_csv = table_set - csv_set
        extra_in_csv = csv_set - table_set
        
        if missing_in_csv:
            log_error(f"Columns in table {table_name} but not in {csv_file}: {missing_in_csv}")
        if extra_in_csv:
            log_error(f"Columns in {csv_file} but not in table {table_name}: {extra_in_csv}")
        return False
    
    return True


def import_csv_to_table(cursor, csv_path, table_name):
    """Import CSV file to SQL Server table."""
    log(f"Importing {csv_path.name} to {table_name}")
    
    # Columns to ignore during import (not present in database tables)
    IGNORED_COLUMNS = ['changeid']
    
    # Get columns
    csv_columns = get_csv_columns(csv_path)
    
    # Filter out ignored columns from CSV columns
    csv_columns_filtered = [col for col in csv_columns if col not in IGNORED_COLUMNS]
    
    # Build insert statement with filtered column names to handle different ordering
    columns_str = ', '.join([f"[{col}]" for col in csv_columns_filtered])
    placeholders = ', '.join(['?' for _ in csv_columns_filtered])
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    # Get indices of columns we want to import (excluding ignored columns)
    column_indices = [i for i, col in enumerate(csv_columns) if col not in IGNORED_COLUMNS]
    
    # Import data
    row_count = 0
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        
        for row in reader:
            # Extract only the columns we want (skip ignored columns)
            filtered_row = [row[i] for i in column_indices]
            # Convert empty strings to None for SQL NULL
            filtered_row = [None if val == '' else val for val in filtered_row]
            cursor.execute(insert_sql, filtered_row)
            row_count += 1
    
    log(f"Imported {row_count} rows into {table_name}")
    return csv_columns


def verify_staging_data(cursor):
    """
    Verify that staging data is valid before copying to main tables.
    Returns: (success, message, tables_with_new_data)
    """
    log("Verifying staging data quality...")
    
    checks_passed = True
    tables_with_new_data = []
    
    for table_name in TABLE_NAMES:
        staging_table = f"{table_name}_staging"
        
        # Check 1: Staging table has more records than main table
        cursor.execute(f"SELECT COUNT(*) FROM {staging_table}")
        staging_count = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        main_count = cursor.fetchone()[0]
        
        if staging_count < main_count:
            # Fewer records in staging - this is a real error
            log_error(f"{staging_table} has {staging_count} records, but {table_name} has {main_count} records")
            log_error("Refusing to replace main table with fewer records - possible data loss!")
            checks_passed = False
        elif staging_count == main_count:
            # Same number of records - no new data
            log(f"Info: {staging_table} has {staging_count} records, same as {table_name} - no new data")
        else:
            # More records in staging - this is good
            log(f"OK: Record count check passed: {staging_table}={staging_count}, {table_name}={main_count}")
            tables_with_new_data.append(table_name)
        
        # Check 2: Staging table is not empty
        if staging_count == 0:
            log_error(f"{staging_table} is empty - cannot proceed")
            checks_passed = False
        else:
            log(f"OK: Non-empty check passed: {staging_table} has {staging_count} records")
        
        # Check 3: Check for duplicate uniqueid (only for enrollee table)
        # formchanges table can have duplicate uniqueids since it's a change log
        if table_name == 'enrollee':
            cursor.execute(f"""
                SELECT COUNT(*) as total, COUNT(DISTINCT uniqueid) as unique_ids
                FROM {staging_table}
            """)
            result = cursor.fetchone()
            total = result[0]
            unique_ids = result[1]
            
            if total != unique_ids:
                duplicates = total - unique_ids
                log_error(f"{staging_table} has {duplicates} duplicate uniqueid(s)")
                checks_passed = False
            else:
                log(f"OK: Duplicate check passed: {staging_table} has no duplicate uniqueids")
        else:
            log(f"Info: Skipping duplicate check for {staging_table} (change log table can have duplicate uniqueids)")
        
        # Check 4: Check for NULL values in critical columns
        if table_name == 'enrollee':
            # For enrollee table: check uniqueid and subjid
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {staging_table}
                WHERE uniqueid IS NULL
            """)
            null_uniqueid_count = cursor.fetchone()[0]
            
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {staging_table}
                WHERE subjid IS NULL
            """)
            null_subjid_count = cursor.fetchone()[0]
            
            if null_uniqueid_count > 0:
                log_error(f"{staging_table} has {null_uniqueid_count} record(s) with NULL uniqueid")
                checks_passed = False
            else:
                log(f"OK: NULL check passed: {staging_table} has no NULL uniqueids")
            
            if null_subjid_count > 0:
                log_error(f"{staging_table} has {null_subjid_count} record(s) with NULL subjid")
                checks_passed = False
            else:
                log(f"OK: NULL check passed: {staging_table} has no NULL subjids")
                
        elif table_name == 'formchanges':
            # For formchanges table: check all columns except oldvalue and newvalue
            # First get all column names
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{staging_table}'
                AND COLUMN_NAME NOT IN ('oldvalue', 'newvalue')
                ORDER BY ORDINAL_POSITION
            """)
            columns_to_check = [row[0] for row in cursor.fetchall()]
            
            null_found = False
            for column in columns_to_check:
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM {staging_table}
                    WHERE [{column}] IS NULL
                """)
                null_count = cursor.fetchone()[0]
                
                if null_count > 0:
                    log_error(f"{staging_table} has {null_count} record(s) with NULL {column}")
                    checks_passed = False
                    null_found = True
            
            if not null_found:
                log(f"OK: NULL check passed: {staging_table} has no NULL values in required columns")
    
    # Check if we have new data
    if not tables_with_new_data:
        log("Info: No new data detected in any table - skipping import")
        return True, "NO_NEW_DATA", []  # FIXED: Always return 3 values
    
    if checks_passed:
        log_success(f"All data quality checks passed - tables with new data: {', '.join(tables_with_new_data)}")
        return True, None, tables_with_new_data
    else:
        return False, "Data quality checks failed - see errors above", []
    

def create_backup_tables(cursor):
    """
    Create backup of main tables before truncating.
    This keeps the last good copy in case something goes wrong.
    """
    log("Creating backup tables...")
    
    for table_name in TABLE_NAMES:
        backup_table = f"{table_name}_backup"
        
        # Drop backup table if it exists
        cursor.execute(f"""
            IF OBJECT_ID('{backup_table}', 'U') IS NOT NULL
                DROP TABLE {backup_table}
        """)
        
        # Create backup as a copy of current main table
        cursor.execute(f"""
            SELECT * INTO {backup_table}
            FROM {table_name}
        """)
        
        cursor.execute(f"SELECT COUNT(*) FROM {backup_table}")
        backup_count = cursor.fetchone()[0]
        log(f"Created backup: {backup_table} ({backup_count} records)")
    
    log_success("Backup tables created")



def copy_staging_to_main(cursor, table_name, csv_columns=None):
    """
    Copy data from staging table to main table by executing external SQL script.
    The SQL scripts handle all type conversions and transformations.
    """
    log(f"Copying data from {table_name}_staging to {table_name}")
    
    sql_file = None
    if table_name == 'enrollee':
        if csv_columns:
            # Find the correct SQL script from FIELD_CONFIG
            csv_columns_set = frozenset(csv_columns)
            sql_file_name = FIELD_CONFIG.get(csv_columns_set)
            if sql_file_name:
                sql_file = SQL_SCRIPTS_PATH / sql_file_name
            else:
                log_error(f"No matching SQL script found for the given columns in {table_name}_staging.")
                return False
        else:
            log_error("No CSV columns provided for enrollee table.")
            return False
    else:
        # For other tables, use the default script
        sql_file = SQL_SCRIPTS_PATH / f"copy_{table_name}_staging_to_main.sql"

    if not sql_file or not sql_file.exists():
        log_error(f"SQL script not found: {sql_file}")
        return False
    
    log(f"Using SQL script: {sql_file.name}")

    # Read SQL from file
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql = f.read()
    except Exception as e:
        log_error(f"Error reading SQL file {sql_file}: {e}")
        return False
    
    # Execute the SQL
    try:
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        log(f"Copied {rows_affected} rows from {table_name}_staging to {table_name}")
        return True
    except pyodbc.Error as e:
        log_error(f"Error executing SQL for {table_name}: {e}")
        return False
    
# ============================================================================
# Data cleaning
# ============================================================================
def run_data_cleaning(cursor):
    """
    Executes the data cleaning SQL script to fix swapped values and barcodes.
    """
    log("Running data cleaning script (data_cleaning.sql)...")
    sql_file = SQL_SCRIPTS_PATH / "data_cleaning.sql"
    
    if not sql_file.exists():
        log_error(f"Cleaning script not found: {sql_file}")
        return False
        
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        # Execute the script as a single batch
        cursor.execute(sql)
        log_success("Data cleaning script executed successfully")
        return True
    except pyodbc.Error as e:
        log_error(f"SQL Error during data cleaning: {e}")
        return False
    

def step2_import_to_sql():
    """Step 2: Import CSV files to SQL Server."""
    log_section("STEP 2: Importing data to SQL Server")
    
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        log("Connected to SQL Server")
        
        # Step 2a: Truncate staging tables
        log("Truncating staging tables...")
        for table_name in TABLE_NAMES:
            truncate_table(cursor, f"{table_name}_staging")
        conn.commit()
        
        # Step 2b: Import CSV files to staging tables
        log("Importing CSV files to staging tables...")
        csv_headers = {}
        for folder in DATA_FOLDERS:
            folder_path = DATA_ROOT / folder
            
            if not folder_path.exists():
                log(f"Folder does not exist: {folder_path}")
                continue
            
            log(f"Processing folder: {folder}")
            
            for table_name in TABLE_NAMES:
                csv_path = folder_path / f"{table_name}.csv"
                
                if not csv_path.exists():
                    log_error(f"CSV file not found: {csv_path}")
                    conn.rollback()
                    return False
                
                csv_columns = import_csv_to_table(cursor, csv_path, f"{table_name}_staging")
                if not csv_columns:
                    conn.rollback()
                    return False
                
                if table_name not in csv_headers:
                    csv_headers[table_name] = csv_columns

        conn.commit()
        log("All CSV files imported to staging tables")
        
        # Step 2c: Verify staging data quality
        success, message, tables_with_new_data = verify_staging_data(cursor)
        if not success:
            log_error(message)
            log_error("Aborting import - main tables remain unchanged")
            conn.rollback()
            return False
        
        # Check if there's no new data
        if message == "NO_NEW_DATA":
            log_success("Step 2 completed - no new data to import")
            cursor.close()
            conn.close()
            return "NO_NEW_DATA"  # Return special value to skip export/zip
        
        # Determine which tables to update
        # If formchanges has new data, update ALL tables
        # Otherwise, update only tables with new data
        if 'formchanges' in tables_with_new_data:
            log("Info: formchanges has new data - will update ALL tables")
            tables_to_update = TABLE_NAMES
        else:
            log(f"Info: Will update tables: {', '.join(tables_with_new_data)}")
            tables_to_update = tables_with_new_data
        
        # Step 2d: Create backup of tables to be updated
        log("Creating backup tables...")
        for table_name in tables_to_update:
            backup_table = f"{table_name}_backup"
            
            # Drop backup table if it exists
            cursor.execute(f"""
                IF OBJECT_ID('{backup_table}', 'U') IS NOT NULL
                    DROP TABLE {backup_table}
            """)
            
            # Create backup as a copy of current main table
            cursor.execute(f"""
                SELECT * INTO {backup_table}
                FROM {table_name}
            """)
            
            cursor.execute(f"SELECT COUNT(*) FROM {backup_table}")
            backup_count = cursor.fetchone()[0]
            log(f"Created backup: {backup_table} ({backup_count} records)")
        
        conn.commit()
        log_success("Backup tables created")
        
        # Step 2e: Truncate tables to be updated
        log("Truncating tables to be updated...")
        for table_name in tables_to_update:
            truncate_table(cursor, table_name)
        conn.commit()
        
        # Step 2f: Copy from staging to main using SQL transformation scripts
        log("Copying data from staging to main tables...")
        for table_name in tables_to_update:
            if not copy_staging_to_main(cursor, table_name, csv_headers.get(table_name)):
                conn.rollback()
                return False
        
        # Data cleaning - only if the 'enrollee' table was actually updated
        if 'enrollee' in tables_to_update:
            if not run_data_cleaning(cursor):
                conn.rollback()
                return False

        conn.commit()
        
        # Step 2g: Verify final record counts
        log("Verifying final record counts...")
        for table_name in tables_to_update:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            final_count = cursor.fetchone()[0]
            log(f"OK: {table_name}: {final_count} records")
        
        log_success("Step 2 completed successfully")
        
        cursor.close()
        conn.close()
        return True
        
    except pyodbc.Error as e:
        log_error(f"SQL Server error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    except Exception as e:
        log_error(f"Unexpected error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    


# ============================================================================
# STEP 3: EXPORT FUNCTIONS
# ============================================================================

def export_table_to_csv(cursor, table_name, output_path):
    """Export SQL Server table to CSV file."""
    log(f"Exporting {table_name} to {output_path.name}")
    
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    
    # Get column names
    columns = [column[0] for column in cursor.description]
    
    # Write to CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(columns)
        writer.writerows(rows)
    
    log(f"Exported {len(rows)} rows to {output_path.name}")


def step3_export_data():
    """Step 3: Export data from SQL Server to CSV files."""
    log_section("STEP 3: Exporting data to CSV files")
    
    try:
        # Ensure export folder exists
        ensure_folder_exists(EXPORT_PATH)
        
        conn = get_sql_connection()
        cursor = conn.cursor()
        log("Connected to SQL Server")
        
        # Export each table
        for table_name in TABLE_NAMES:
            output_path = EXPORT_PATH / f"{table_name}.csv"
            export_table_to_csv(cursor, table_name, output_path)
        
        log_success("Step 3 completed successfully")
        
        cursor.close()
        conn.close()
        return True
        
    except pyodbc.Error as e:
        log_error(f"SQL Server error: {e}")
        return False
    except Exception as e:
        log_error(f"Unexpected error: {e}")
        return False


# ============================================================================
# STEP 4: ZIP FILES
# ============================================================================

def step4_zip_files():
    """Step 4: Zip exported CSV files."""
    log_section("STEP 4: Zipping export files")
    
    try:
        # Ensure output directory exists
        ensure_folder_exists(ZIP_OUTPUT_PATH.parent)
        
        # Create zip file
        log(f"Creating zip file: {ZIP_OUTPUT_PATH}")
        
        with zipfile.ZipFile(ZIP_OUTPUT_PATH, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for table_name in TABLE_NAMES:
                csv_path = EXPORT_PATH / f"{table_name}.csv"
                
                if not csv_path.exists():
                    log_error(f"CSV file not found: {csv_path}")
                    return False
                
                # Add file to zip with just the filename (no path)
                zipf.write(csv_path, csv_path.name)
                log(f"Added {csv_path.name} to zip")
        
        log_success("Step 4 completed successfully")
        return True
        
    except Exception as e:
        log_error(f"Error creating zip file: {e}")
        return False


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main function to orchestrate the complete data processing pipeline."""
    start_time = datetime.now()
    
    print("\n" + "=" * 60)
    print("R21 TEST NEGATIVE - DATA PROCESSING PIPELINE")
    print("=" * 60)
    log(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Fetch data from FTP
    if not step1_fetch_data():
        log_error("Step 1: Fetch Data from FTP FAILED - Pipeline stopped")
        print("\n" + "=" * 60)
        log("PIPELINE FAILED", "ERROR")
        print("=" * 60)
        return 1
    
    # Step 2: Import to SQL Server
    step2_result = step2_import_to_sql()
    if step2_result == False:
        log_error("Step 2: Import to SQL Server FAILED - Pipeline stopped")
        print("\n" + "=" * 60)
        log("PIPELINE FAILED", "ERROR")
        print("=" * 60)
        return 1
    elif step2_result == "NO_NEW_DATA":
        # No new data - skip export and zip
        log("Info: Skipping export and zip steps (no new data)")
        end_time = datetime.now()
        duration = end_time - start_time
        print("\n" + "=" * 60)
        log_success("PIPELINE COMPLETED - NO NEW DATA PROCESSED")
        log(f"Duration: {duration}")
        print("=" * 60)
        return 0
    
    # Step 3: Export to CSV (only if there was new data)
    if not step3_export_data():
        log_error("Step 3: Export to CSV FAILED - Pipeline stopped")
        print("\n" + "=" * 60)
        log("PIPELINE FAILED", "ERROR")
        print("=" * 60)
        return 1
    
    # Step 4: Zip files (only if there was new data)
    if not step4_zip_files():
        log_error("Step 4: Zip Files FAILED - Pipeline stopped")
        print("\n" + "=" * 60)
        log("PIPELINE FAILED", "ERROR")
        print("=" * 60)
        return 1
    
    # All steps completed successfully
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    log_success("ALL STEPS COMPLETED SUCCESSFULLY")
    log(f"Duration: {duration}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())