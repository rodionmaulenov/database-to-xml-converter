import sqlite3
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List, Any
from lxml import etree

from .normalizers import (
    normalize_date,
    normalize_amount,
    validate_account,
    clean_description
)
from .config import ETLConfig

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL Pipeline for processing journal entries"""

    def __init__(self, db_path: str, output_path: str, schema_path: Optional[str] = None, 
                 config: Optional[ETLConfig] = None) -> None:
        """
        Initialize ETL Pipeline
        
        Args:
            db_path: Path to SQLite database
            output_path: Path for output XML file
            schema_path: Optional path to XSD schema for validation
            config: Optional ETL configuration
        """
        self.db_path: str = db_path
        self.output_path: str = output_path
        self.schema_path: Optional[str] = schema_path
        self.config: ETLConfig = config or ETLConfig.default()

        # Statistics
        self.total_records: int = 0
        self.valid_records: int = 0
        self.skipped_records: int = 0
        
        # Performance tracking
        self.extract_time: float = 0.0
        self.transform_time: float = 0.0
        self.load_time: float = 0.0

    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from SQLite database
        
        Returns:
            List of dictionaries containing raw data
        """
        start_time = time.time()
        logger.info(f"Connecting to database: {self.db_path}")

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Query all journal entries
            cursor.execute("SELECT Date, Account, Amount, Description FROM journal_entries")
            rows = cursor.fetchall()

            # Convert to list of dictionaries
            data = [dict(row) for row in rows]

            conn.close()

            self.extract_time = time.time() - start_time
            logger.info(f"Extracted {len(data)} records from database in {self.extract_time:.2f}s")
            return data

        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {e}")
            raise

    def transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        Transform and validate raw data
        
        Args:
            raw_data: List of raw records from database
            
        Returns:
            List of transformed and validated records
        """
        start_time = time.time()
        logger.info("Starting data transformation")

        transformed_data = []
        self.total_records = len(raw_data)

        for record in raw_data:
            # Normalize each field
            normalized_date = normalize_date(record.get('Date', ''))
            normalized_amount = normalize_amount(record.get('Amount', ''))
            validated_account = validate_account(record.get('Account', ''))
            # Use config for description max length
            desc = record.get('Description')
            if desc and len(str(desc)) > self.config.processing.max_description_length:
                desc = str(desc)[:self.config.processing.max_description_length]
            cleaned_description = clean_description(desc)

            # Check if all required fields are valid
            if normalized_date and normalized_amount and validated_account:
                transformed_record = {
                    'Date': normalized_date,
                    'Account': validated_account,
                    'Amount': normalized_amount,
                    'Description': cleaned_description
                }
                transformed_data.append(transformed_record)
                self.valid_records += 1
            else:
                self.skipped_records += 1
                logger.debug(f"Skipped invalid record: {record}")

        self.transform_time = time.time() - start_time
        logger.info(f"Transformation complete: {self.valid_records} valid, {self.skipped_records} skipped in {self.transform_time:.2f}s")
        return transformed_data

    def load(self, data: List[Dict[str, str]]) -> None:
        """
        Generate XML file from transformed data
        
        Args:
            data: List of transformed records
        """
        start_time = time.time()
        logger.info("Generating XML output")

        # Create root element
        root = etree.Element("Journal")

        # Add schema reference if schema path is provided
        if self.schema_path:
            root.set("{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation",
                     "schema.xsd")

        # Add each entry
        for record in data:
            entry = etree.SubElement(root, "Entry")

            date_elem = etree.SubElement(entry, "Date")
            date_elem.text = record['Date']

            account_elem = etree.SubElement(entry, "Account")
            account_elem.text = record['Account']

            amount_elem = etree.SubElement(entry, "Amount")
            amount_elem.text = record['Amount']

            # Only add Description if it's not empty
            if record['Description']:
                desc_elem = etree.SubElement(entry, "Description")
                desc_elem.text = record['Description']

        # Create pretty-printed XML
        tree = etree.ElementTree(root)

        # Validate against schema if provided and enabled in config
        if (self.config.output.validate_xml and 
            self.schema_path and Path(self.schema_path).exists()):
            try:
                logger.info("Validating XML against schema")
                schema_doc = etree.parse(self.schema_path)
                schema = etree.XMLSchema(schema_doc)
                schema.assertValid(tree)
                logger.info("XML validation successful")
            except etree.XMLSchemaError as e:
                logger.error(f"Schema validation failed: {e}")
                raise

        # Write to file using config settings
        try:
            tree.write(self.output_path,
                       pretty_print=self.config.output.pretty_print,
                       xml_declaration=True,
                       encoding='UTF-8')
            self.load_time = time.time() - start_time
            logger.info(f"XML file written to: {self.output_path} in {self.load_time:.2f}s")
        except IOError as e:
            logger.error(f"Failed to write XML file: {e}")
            raise

    def run(self) -> None:
        """Execute the complete ETL pipeline"""
        logger.info("Starting ETL pipeline")

        try:
            # Extract
            raw_data = self.extract()

            # Transform
            transformed_data = self.transform(raw_data)

            # Load
            self.load(transformed_data)

            # Final statistics with performance metrics
            if self.total_records > 0:
                success_rate = self.valid_records / self.total_records * 100
                total_time = self.extract_time + self.transform_time + self.load_time
                throughput = self.total_records / total_time if total_time > 0 else 0
                
                logger.info(f"""
                    ETL Pipeline completed successfully!
                    Total records processed: {self.total_records}
                    Valid records: {self.valid_records}
                    Skipped records: {self.skipped_records}
                    Success rate: {success_rate:.1f}%
                    
                    Performance:
                    Extract time: {self.extract_time:.2f}s
                    Transform time: {self.transform_time:.2f}s
                    Load time: {self.load_time:.2f}s
                    Total time: {total_time:.2f}s
                    Throughput: {throughput:.0f} records/second
                """)
            else:
                logger.info("ETL Pipeline completed successfully! No records found to process.")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
