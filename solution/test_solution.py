import pytest
import sqlite3
import tempfile
import os
from pathlib import Path
from lxml import etree
from solution.normalizers import (
    normalize_date, 
    normalize_amount, 
    validate_account, 
    clean_description
)
from solution.pipeline import ETLPipeline


class TestDateNormalization:
    """Test cases for date normalization"""
    
    def test_iso_date(self):
        assert normalize_date("2024-01-01") == "2024-01-01"
        assert normalize_date("2023-12-31") == "2023-12-31"
    
    def test_iso_datetime_with_t(self):
        assert normalize_date("2024-01-01T14:30:00") == "2024-01-01"
        assert normalize_date("2023-12-31T23:59:59") == "2023-12-31"
    
    def test_iso_datetime_with_space(self):
        assert normalize_date("2024-01-01 14:30:00") == "2024-01-01"
        assert normalize_date("2023-12-31 00:00:00") == "2023-12-31"
    
    def test_us_format(self):
        assert normalize_date("12/31/2024") == "2024-12-31"
        assert normalize_date("01/15/2024") == "2024-01-15"
        assert normalize_date("02/29/2024") == "2024-02-29"  # Leap year
    
    def test_european_format(self):
        assert normalize_date("31-12-2024") == "2024-12-31"
        assert normalize_date("15-01-2024") == "2024-01-15"
        assert normalize_date("29-02-2024") == "2024-02-29"  # Leap year
    
    def test_invalid_dates(self):
        assert normalize_date("invalid") is None
        assert normalize_date("2024-13-01") is None  # Invalid month
        assert normalize_date("2024-01-32") is None  # Invalid day
        assert normalize_date("02/30/2024") is None  # Feb 30 doesn't exist
        assert normalize_date("") is None
        assert normalize_date(None) is None
        assert normalize_date("2024/01/01") is None  # Unsupported format
    
    def test_edge_cases(self):
        assert normalize_date("  2024-01-01  ") == "2024-01-01"  # With spaces
        assert normalize_date("29-02-2023") is None  # Not a leap year


class TestAmountNormalization:
    """Test cases for amount normalization"""
    
    def test_whole_numbers(self):
        assert normalize_amount("100") == "100.00"
        assert normalize_amount("0") == "0.00"
        assert normalize_amount("999999") == "999999.00"
    
    def test_single_decimal(self):
        assert normalize_amount("100.5") == "100.50"
        assert normalize_amount("0.1") == "0.10"
        assert normalize_amount("99.9") == "99.90"
    
    def test_two_decimals(self):
        assert normalize_amount("100.50") == "100.50"
        assert normalize_amount("0.01") == "0.01"
        assert normalize_amount("99.99") == "99.99"
    
    def test_multiple_decimals_rounding(self):
        assert normalize_amount("100.567") == "100.57"
        assert normalize_amount("100.564") == "100.56"
        assert normalize_amount("100.565") == "100.57"  # Banker's rounding
        assert normalize_amount("100.999") == "101.00"
    
    def test_european_comma(self):
        assert normalize_amount("1,20") == "1.20"
        assert normalize_amount("100,5") == "100.50"
        assert normalize_amount("999,99") == "999.99"
    
    def test_negative_amounts(self):
        assert normalize_amount("-42.1") == "-42.10"
        assert normalize_amount("-100") == "-100.00"
        assert normalize_amount("-0.01") == "-0.01"
        assert normalize_amount("-999.999") == "-1000.00"
    
    def test_invalid_amounts(self):
        assert normalize_amount("invalid") is None
        assert normalize_amount("12.34.56") is None
        assert normalize_amount("") is None
        assert normalize_amount(None) is None
        assert normalize_amount("$100") is None
        assert normalize_amount("100 USD") is None
    
    def test_edge_cases(self):
        assert normalize_amount("  100.50  ") == "100.50"  # With spaces
        assert normalize_amount("-0") == "0.00"
        assert normalize_amount("00100.50") == "100.50"  # Leading zeros


class TestAccountValidation:
    """Test cases for account validation"""
    
    def test_valid_accounts(self):
        assert validate_account("123") == "123"  # Minimum length
        assert validate_account("123456789012") == "123456789012"  # Maximum length
        assert validate_account("000") == "000"
        assert validate_account("999999") == "999999"
    
    def test_invalid_accounts(self):
        assert validate_account("12") is None  # Too short
        assert validate_account("1234567890123") is None  # Too long
        assert validate_account("12A") is None  # Contains letter
        assert validate_account("12-34") is None  # Contains dash
        assert validate_account("12.34") is None  # Contains dot
        assert validate_account("") is None
        assert validate_account(None) is None
        assert validate_account("ABC") is None  # All letters
    
    def test_edge_cases(self):
        assert validate_account("  123  ") == "123"  # With spaces
        assert validate_account("001") == "001"  # Leading zeros


class TestDescriptionCleaning:
    """Test cases for description cleaning"""
    
    def test_normal_descriptions(self):
        assert clean_description("Opening balance") == "Opening balance"
        assert clean_description("Payment received") == "Payment received"
    
    def test_empty_description(self):
        assert clean_description("") == ""
        assert clean_description(None) == ""
        assert clean_description("   ") == ""
    
    def test_trimming(self):
        assert clean_description("  Test  ") == "Test"
        assert clean_description("\nTest\n") == "Test"
        assert clean_description("\tTest\t") == "Test"
    
    def test_truncation(self):
        long_text = "A" * 300
        result = clean_description(long_text)
        assert len(result) == 255
        assert result == "A" * 255
    
    def test_edge_cases(self):
        assert clean_description(123) == "123"  # Non-string input
        assert clean_description("Test\nwith\nnewlines") == "Test\nwith\nnewlines"


class TestETLPipeline:
    """Test cases for the complete ETL pipeline"""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database with test data"""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute('''
            CREATE TABLE journal_entries (
                Date TEXT,
                Account TEXT,
                Amount TEXT,
                Description TEXT
            )
        ''')
        
        # Insert test data
        test_data = [
            ("2024-01-01", "101", "100", "Opening balance"),
            ("2024-01-02T10:30:00", "202", "50.5", "Payment"),
            ("12/31/2023", "303", "-25,50", "Withdrawal"),
            ("31-12-2023", "404", "1000.567", ""),  # Empty description
            ("invalid-date", "505", "100", "Should be skipped"),  # Invalid date
            ("2024-01-03", "AB123", "100", "Should be skipped"),  # Invalid account
            ("2024-01-04", "606", "invalid", "Should be skipped"),  # Invalid amount
            ("2024-01-05", "12", "100", "Should be skipped"),  # Account too short
        ]
        
        cursor.executemany("INSERT INTO journal_entries VALUES (?, ?, ?, ?)", test_data)
        conn.commit()
        conn.close()
        
        yield path
        
        # Cleanup
        os.unlink(path)
    
    @pytest.fixture
    def temp_output(self):
        """Create a temporary output file path"""
        fd, path = tempfile.mkstemp(suffix='.xml')
        os.close(fd)
        yield path
        # Cleanup
        if os.path.exists(path):
            os.unlink(path)
    
    def test_pipeline_extraction(self, temp_db):
        """Test data extraction from database"""
        pipeline = ETLPipeline(temp_db, "dummy.xml")
        data = pipeline.extract()
        
        assert len(data) == 8
        assert data[0]['Date'] == "2024-01-01"
        assert data[0]['Account'] == "101"
    
    def test_pipeline_transformation(self, temp_db):
        """Test data transformation"""
        pipeline = ETLPipeline(temp_db, "dummy.xml")
        raw_data = pipeline.extract()
        transformed = pipeline.transform(raw_data)
        
        assert len(transformed) == 4  # Only 4 valid records
        assert pipeline.total_records == 8
        assert pipeline.valid_records == 4
        assert pipeline.skipped_records == 4
        
        # Check first transformed record
        assert transformed[0]['Date'] == "2024-01-01"
        assert transformed[0]['Account'] == "101"
        assert transformed[0]['Amount'] == "100.00"
        assert transformed[0]['Description'] == "Opening balance"
        
        # Check record with transformations
        assert transformed[2]['Date'] == "2023-12-31"  # US format converted
        assert transformed[2]['Amount'] == "-25.50"  # European comma converted
    
    def test_pipeline_xml_generation(self, temp_db, temp_output):
        """Test XML generation"""
        pipeline = ETLPipeline(temp_db, temp_output)
        pipeline.run()
        
        # Check file exists
        assert os.path.exists(temp_output)
        
        # Parse and validate XML structure
        tree = etree.parse(temp_output)
        root = tree.getroot()
        
        assert root.tag == "Journal"
        entries = root.findall("Entry")
        assert len(entries) == 4
        
        # Check first entry
        first_entry = entries[0]
        assert first_entry.find("Date").text == "2024-01-01"
        assert first_entry.find("Account").text == "101"
        assert first_entry.find("Amount").text == "100.00"
        assert first_entry.find("Description").text == "Opening balance"
        
        # Check entry with empty description
        last_entry = entries[3]
        assert last_entry.find("Description") is None  # Should not be present
    
    def test_empty_database(self):
        """Test handling of empty database"""
        # Create empty database
        fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE journal_entries (
                Date TEXT,
                Account TEXT,
                Amount TEXT,
                Description TEXT
            )
        ''')
        conn.commit()
        conn.close()
        
        fd, output_path = tempfile.mkstemp(suffix='.xml')
        os.close(fd)
        
        pipeline = ETLPipeline(db_path, output_path)
        pipeline.run()
        
        # Check empty XML
        tree = etree.parse(output_path)
        root = tree.getroot()
        assert len(root.findall("Entry")) == 0
        
        # Cleanup
        os.unlink(db_path)
        os.unlink(output_path)
    
    def test_database_error(self):
        """Test handling of database errors"""
        pipeline = ETLPipeline("non_existent.db", "dummy.xml")
        
        with pytest.raises(sqlite3.Error):
            pipeline.extract()
    
    def test_schema_validation(self, temp_db, temp_output):
        """Test XML schema validation"""
        # Get schema path relative to test file location
        test_dir = Path(__file__).parent
        project_root = test_dir.parent
        schema_path = str(project_root / "sources" / "schema.xsd")
        
        if Path(schema_path).exists():
            pipeline = ETLPipeline(temp_db, temp_output, schema_path)
            pipeline.run()
            
            # Verify schema reference in XML
            tree = etree.parse(temp_output)
            root = tree.getroot()
            
            ns = "{http://www.w3.org/2001/XMLSchema-instance}"
            assert root.get(f"{ns}noNamespaceSchemaLocation") == "schema.xsd"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])