import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class PathsConfig:
    """Configuration for file paths"""
    database: str = "sources/journal_entries.db"
    schema: str = "sources/schema.xsd"
    output: str = "sources/output.xml"


@dataclass
class ProcessingConfig:
    """Configuration for data processing options"""
    batch_size: int = 1000
    skip_validation: bool = False
    max_description_length: int = 255


@dataclass
class LoggingConfig:
    """Configuration for logging"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@dataclass
class OutputConfig:
    """Configuration for XML output"""
    pretty_print: bool = True
    validate_xml: bool = True


@dataclass
class ETLConfig:
    """Main configuration class for ETL pipeline"""
    paths: PathsConfig
    processing: ProcessingConfig
    logging: LoggingConfig
    output: OutputConfig

    @classmethod
    def load_from_file(cls, config_path: Optional[str] = None) -> 'ETLConfig':
        """
        Load configuration from JSON file
        
        Args:
            config_path: Path to config file. If None, tries default locations.
            
        Returns:
            ETLConfig instance
        """
        # Use default config file if not specified
        if config_path is None:
            config_path = "config.json"
        
        # Load from file if exists
        if config_path and Path(config_path).exists():
            try:
                logger.info(f"Loading configuration from: {config_path}")
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                return cls.from_dict(config_data)
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.warning(f"Failed to load config from {config_path}: {e}")
                logger.info("Using default configuration")
        else:
            logger.info("No config file found, using default configuration")
        
        # Return default configuration
        return cls.default()

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ETLConfig':
        """Create ETLConfig from dictionary"""
        return cls(
            paths=PathsConfig(**config_dict.get('paths', {})),
            processing=ProcessingConfig(**config_dict.get('processing', {})),
            logging=LoggingConfig(**config_dict.get('logging', {})),
            output=OutputConfig(**config_dict.get('output', {}))
        )

    @classmethod
    def default(cls) -> 'ETLConfig':
        """Create default configuration"""
        return cls(
            paths=PathsConfig(),
            processing=ProcessingConfig(),
            logging=LoggingConfig(),
            output=OutputConfig()
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return asdict(self)

    def save_to_file(self, config_path: str) -> None:
        """Save configuration to JSON file"""
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, indent=2)
            logger.info(f"Configuration saved to: {config_path}")
        except IOError as e:
            logger.error(f"Failed to save config to {config_path}: {e}")
            raise

    def get_absolute_paths(self, project_root: Path) -> Dict[str, str]:
        """
        Get absolute paths based on project root
        
        Args:
            project_root: Path to project root directory
            
        Returns:
            Dictionary with absolute paths
        """
        return {
            'database': str(project_root / self.paths.database),
            'schema': str(project_root / self.paths.schema),
            'output': str(project_root / self.paths.output)
        }



def load_config(config_path: Optional[str] = None) -> ETLConfig:
    """
    Convenience function to load configuration
    
    Args:
        config_path: Optional path to config file
        
    Returns:
        ETLConfig instance
    """
    config = ETLConfig.load_from_file(config_path)
    return config