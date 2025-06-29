"""
Main entry point for ETL pipeline
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

from solution.pipeline import ETLPipeline
from solution.config import load_config, ETLConfig

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_argument_parser() -> argparse.ArgumentParser:
    """Create simple argument parser"""
    parser = argparse.ArgumentParser(
        description='ETL Pipeline for processing accounting journal entries'
    )
    
    parser.add_argument(
        '--db-path',
        help='Path to SQLite database file'
    )
    
    parser.add_argument(
        '--output-path', 
        help='Path for output XML file'
    )
    
    return parser


def main() -> None:
    """Main entry point"""
    # Parse command line arguments
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Load configuration
    config: ETLConfig = load_config()
    
    # Configure logging from config
    logging.basicConfig(
        level=getattr(logging, config.logging.level),
        format=config.logging.format,
        force=True
    )
    
    # Get project root and paths
    project_root: Path = Path(__file__).parent
    paths = config.get_absolute_paths(project_root)
    
    # Use CLI arguments or config defaults
    db_path: str = args.db_path or paths['database']
    output_path: str = args.output_path or paths['output']
    schema_path: Optional[str] = paths['schema']
    
    # Convert relative paths to absolute
    if not Path(db_path).is_absolute():
        db_path = str(project_root / db_path)
    if not Path(output_path).is_absolute():
        output_path = str(project_root / output_path)
    
    # Check database exists
    if not Path(db_path).exists():
        logger.error(f"Database file not found: {db_path}")
        raise FileNotFoundError(f"Database file not found: {db_path}")
    
    # Check schema exists
    if not Path(schema_path).exists():
        logger.warning(f"Schema file not found: {schema_path}. Proceeding without schema validation.")
        schema_path = None

    # Create and run pipeline
    pipeline: ETLPipeline = ETLPipeline(db_path, output_path, schema_path, config)
    pipeline.run()


if __name__ == "__main__":
    main()