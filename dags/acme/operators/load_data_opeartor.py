from typing import Any, Dict, Union, List, Optional
from abc import ABC, abstractmethod
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import logging

class DataLoader(ABC):
    """Abstract base class for data loading chain"""
    
    def __init__(self):
        self._next_handler = None

    def set_next(self, handler):
        """Set the next handler in the chain"""
        self._next_handler = handler
        return handler

    @abstractmethod
    def handle(self, source: Any, **kwargs) -> Dict[str, Any]:
        """Handle the data loading request"""
        if self._next_handler:
            return self._next_handler.handle(source, **kwargs)
        return None

class DirectoryLoader(DataLoader):
    """Handler for loading data from directory"""
    
    def __init__(self):
        super().__init__()
        self.supported_extensions = {
            'image': {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.svg'},
            'document': {'.docx', '.xlsx', '.pptx', '.pdf', '.csv'}
        }

    def handle(self, source: str, **kwargs) -> Dict[str, Any]:
        if not isinstance(source, str) or not os.path.isdir(source):
            return super().handle(source, **kwargs)

        files_by_type = {'image': [], 'document': [], 'unknown': []}
        
        for file_path in Path(source).rglob('*'):
            if file_path.is_file():
                ext = file_path.suffix.lower()
                if ext in self.supported_extensions['image']:
                    files_by_type['image'].append(str(file_path))
                elif ext in self.supported_extensions['document']:
                    files_by_type['document'].append(str(file_path))
                else:
                    files_by_type['unknown'].append(str(file_path))

        return {
            'source_type': 'directory',
            'files': files_by_type,
            'metadata': {
                'total_files': sum(len(files) for files in files_by_type.values()),
                'directory_path': source
            }
        }

class SQLLoader(DataLoader):
    """Handler for loading data from SQL query"""
    
    def handle(self, source: Any, **kwargs) -> Dict[str, Any]:
        if not isinstance(source, str) or not self._is_sql_query(source):
            return super().handle(source, **kwargs)

        connection_params = kwargs.get('connection_params', {})
        if not connection_params:
            raise ValueError("Database connection parameters are required for SQL loading")

        try:
            engine = create_engine(self._build_connection_string(connection_params))
            df = pd.read_sql(source, engine)
            
            return {
                'source_type': 'sql',
                'data': df,
                'metadata': {
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'query': source
                }
            }
        except Exception as e:
            logging.error(f"Failed to execute SQL query: {str(e)}")
            raise

    def _is_sql_query(self, source: str) -> bool:
        """Simple check if the source string looks like a SQL query"""
        sql_keywords = {'select', 'from', 'where', 'join', 'group by', 'order by'}
        return any(keyword in source.lower() for keyword in sql_keywords)

    def _build_connection_string(self, params: Dict) -> str:
        """Build SQLAlchemy connection string from parameters"""
        required_params = ['dialect', 'username', 'password', 'host', 'database']
        if not all(param in params for param in required_params):
            raise ValueError(f"Missing required connection parameters. Required: {required_params}")
            
        return f"{params['dialect']}://{params['username']}:{params['password']}@{params['host']}/{params['database']}"

class LoadDataOperator(BaseOperator):
    """
    Custom Airflow operator that handles loading data from various sources
    
    :param data_source: Source of the data (directory path or SQL query)
    :param connection_params: Database connection parameters (required for SQL queries)
    """
    
    @apply_defaults
    def __init__(
        self,
        data_source: str,
        connection_params: Optional[Dict] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.data_source = data_source
        self.connection_params = connection_params or {}
        self._initialize_chain()

    def _initialize_chain(self) -> None:
        """Initialize the chain of responsibility for data loading"""
        directory_loader = DirectoryLoader()
        sql_loader = SQLLoader()
        
        # Set up the chain
        directory_loader.set_next(sql_loader)
        self.chain = directory_loader

    def execute(self, context: Dict) -> Dict[str, Any]:
        """
        Execute the data loading operation
        
        :param context: Airflow context
        :return: Dictionary containing loaded data and metadata
        """
        self.log.info(f"Starting data loading from source")
        
        try:
            result = self.chain.handle(
                self.data_source,
                connection_params=self.connection_params
            )
            
            if result is None:
                raise ValueError(f"Unable to handle data source: {self.data_source}")
                
            self.log.info(f"Data loading completed successfully")
            return result
            
        except Exception as e:
            self.log.error(f"Data loading failed: {str(e)}")
            raise