from typing import Any, List, Optional, Union, Dict 
from abc import ABC, abstractmethod
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import numpy as np
from transformers import AutoTokenizer, AutoModel, AutoImageProcessor, AutoModelForCausalLM
import torch
import re
import pandas as pd
from PIL import Image
import io

class InputProcessor(ABC):
    """Abstract base class for input processing"""
    
    @abstractmethod
    def process(self, input_data: Any) -> str:
        pass

class TextProcessor(InputProcessor):
    """Process text input"""
    
    def process(self, input_data: str) -> str:
        return input_data

class ImageProcessor(InputProcessor):
    """Process image input using vision model for caption generation"""
    
    def __init__(self):
        self.processor = AutoImageProcessor.from_pretrained("microsoft/git-base")
        self.model = AutoModelForCausalLM.from_pretrained("microsoft/git-base")
        
    def process(self, input_data: Union[str, bytes, Image.Image]) -> str:
        # Convert input to PIL Image if needed
        if isinstance(input_data, str):
            image = Image.open(input_data)
        elif isinstance(input_data, bytes):
            image = Image.open(io.BytesIO(input_data))
        elif isinstance(input_data, Image.Image):
            image = input_data
        else:
            raise ValueError("Unsupported image input type")
            
        # Process image and generate description
        inputs = self.processor(images=image, return_tensors="pt")
        outputs = self.model.generate(**inputs, max_length=50)
        description = self.processor.batch_decode(outputs, skip_special_tokens=True)[0]
        
        return description

class RDBProcessor(InputProcessor):
    """Process RDB data input"""
    
    def process(self, input_data: Union[pd.DataFrame, Dict, str]) -> str:
        if isinstance(input_data, pd.DataFrame):
            # Convert DataFrame to string representation
            return input_data.to_string()
        elif isinstance(input_data, dict):
            # Convert dictionary to DataFrame then to string
            return pd.DataFrame(input_data).to_string()
        elif isinstance(input_data, str):
            # Assume it's a SQL query result
            return input_data
        else:
            raise ValueError("Unsupported RDB data input type")

class ChunkingStrategy(ABC):
    """Abstract base class for chunking strategies."""
    
    def __init__(self) -> None:
        self._next_handler: Optional['ChunkingStrategy'] = None

    def set_next(self, handler: 'ChunkingStrategy') -> 'ChunkingStrategy':
        self._next_handler = handler
        return handler

    @abstractmethod
    def handle(self, text: str, **kwargs) -> List[str]:
        pass

class FixedSizeChunkingStrategy(ChunkingStrategy):
    """
    Strategy 1: Fixed Size Chunking
    Splits text by single character with specified chunk size and overlap
    """
    
    def handle(self, text: str, **kwargs) -> List[str]:
        chunk_size = kwargs.get('chunk_size', 1000)
        chunk_overlap = kwargs.get('chunk_overlap', 200)
        separator = kwargs.get('separator', '')
        
        if not text:
            return []
            
        # If separator is provided, split by separator first
        if separator:
            parts = text.split(separator)
        else:
            parts = [text]
            
        chunks = []
        for part in parts:
            for i in range(0, len(part), chunk_size - chunk_overlap):
                chunk = part[i:i + chunk_size]
                if chunk:
                    chunks.append(chunk)
                    
        return chunks

class RecursiveChunkingStrategy(ChunkingStrategy):
    """
    Strategy 2: Recursive Chunking
    Recursively splits text based on different separators
    """
    
    def handle(self, text: str, **kwargs) -> List[str]:
        separators = kwargs.get('separators', ['\n\n', '\n', '. ', ' '])
        min_chunk_size = kwargs.get('min_chunk_size', 100)
        
        def recursive_split(text: str, separators: List[str], depth: int = 0) -> List[str]:
            if depth >= len(separators):
                return [text]
                
            parts = text.split(separators[depth])
            chunks = []
            
            for part in parts:
                if len(part.strip()) < min_chunk_size and depth < len(separators) - 1:
                    sub_chunks = recursive_split(part, separators, depth + 1)
                    chunks.extend(sub_chunks)
                else:
                    chunks.append(part.strip())
                    
            return [c for c in chunks if c]
            
        return recursive_split(text, separators)

class DocumentBasedChunkingStrategy(ChunkingStrategy):
    """
    Strategy 3: Document-based Chunking
    Handles different document types (Markdown, Python/JS, Tables, Images)
    """
    
    def handle(self, text: str, **kwargs) -> List[str]:
        doc_type = kwargs.get('doc_type', 'text')
        
        if doc_type == 'markdown':
            # Split on markdown headers and code blocks
            chunks = re.split(r'(?=#{1,6}\s)|(?=```)', text)
        elif doc_type in ['python', 'javascript']:
            # Split on class and function definitions
            chunks = re.split(r'(?=class\s+)|(?=def\s+)|(?=function\s+)', text)
        elif doc_type == 'table':
            # Handle table content (assuming CSV format)
            chunks = [row for row in text.split('\n') if row.strip()]
        else:
            # Default text splitting
            chunks = text.split('\n\n')
            
        return [chunk.strip() for chunk in chunks if chunk.strip()]

class SemanticChunkingStrategy(ChunkingStrategy):
    """
    Strategy 4: Semantic Chunking
    Uses embedding similarity to create semantically coherent chunks
    """
    
    def __init__(self):
        super().__init__()
        self.tokenizer = AutoTokenizer.from_pretrained('sentence-transformers/all-MiniLM-L6-v2')
        self.model = AutoModel.from_pretrained('sentence-transformers/all-MiniLM-L6-v2')
        
    def handle(self, text: str, **kwargs) -> List[str]:
        buffer_size = kwargs.get('buffer_size', 512)
        breakpoint_threshold = kwargs.get('breakpoint_percentile_threshold', 0.7)
        
        # Split into sentences first
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        if len(sentences) <= 1:
            return sentences
            
        # Get embeddings for sentences
        inputs = self.tokenizer(sentences, padding=True, truncation=True, return_tensors="pt")
        with torch.no_grad():
            embeddings = self.model(**inputs).last_hidden_state.mean(dim=1)
            
        # Calculate semantic similarity between adjacent sentences
        similarities = []
        for i in range(len(embeddings) - 1):
            similarity = torch.cosine_similarity(embeddings[i], embeddings[i+1], dim=0)
            similarities.append(similarity.item())
            
        # Find break points where similarity is low
        threshold = np.percentile(similarities, breakpoint_threshold * 100)
        chunk_indices = [0] + [i + 1 for i, sim in enumerate(similarities) if sim < threshold] + [len(sentences)]
        
        # Create chunks based on break points
        chunks = []
        for i in range(len(chunk_indices) - 1):
            chunk = ' '.join(sentences[chunk_indices[i]:chunk_indices[i+1]])
            chunks.append(chunk)
            
        return chunks

class AgenticChunkingStrategy(ChunkingStrategy):
    """
    Strategy 5: Agentic Chunking
    Uses LLM to determine optimal chunk boundaries based on context
    """
    
    def handle(self, text: str, **kwargs) -> List[str]:
        # Note: This is a simplified version. In practice, you would want to use
        # a proper LLM API (e.g., OpenAI) to make chunking decisions
        
        max_chunk_size = kwargs.get('max_chunk_size', 1000)
        
        def find_natural_break(text: str, around_position: int) -> int:
            # Look for natural break points: paragraphs, sentences, or punctuation
            break_points = [
                text.rfind('\n\n', 0, around_position),
                text.rfind('. ', 0, around_position),
                text.rfind('! ', 0, around_position),
                text.rfind('? ', 0, around_position),
                text.rfind(', ', 0, around_position)
            ]
            
            # Get the closest break point that's not -1
            valid_breaks = [b for b in break_points if b != -1]
            return max(valid_breaks) if valid_breaks else around_position
        
        chunks = []
        current_pos = 0
        
        while current_pos < len(text):
            if current_pos + max_chunk_size >= len(text):
                chunks.append(text[current_pos:])
                break
                
            # Find natural break point
            break_point = find_natural_break(text, current_pos + max_chunk_size)
            
            # Add chunk and move position
            if break_point > current_pos:
                chunks.append(text[current_pos:break_point + 2])  # +2 to include the punctuation and space
                current_pos = break_point + 2
            else:
                # Fallback if no good break point found
                chunks.append(text[current_pos:current_pos + max_chunk_size])
                current_pos += max_chunk_size
                
        return chunks

class ChunkingOperator(BaseOperator):
    """
    Custom Airflow operator that implements chunking strategies for different input types
    
    :param input_data: Input data to be chunked (text, image, or RDB data)
    :param input_type: Type of input ('text', 'image', or 'rdb')
    :param strategy: Chunking strategy to use
    :param strategy_params: Additional parameters for the chunking strategy
    """
    
    @apply_defaults
    def __init__(
        self,
        input_data: Any,
        input_type: str = 'text',
        strategy: str = 'fixed',
        strategy_params: Optional[Dict] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.input_data = input_data
        self.input_type = input_type
        self.strategy = strategy
        self.strategy_params = strategy_params or {}
        self._initialize_processors()
        self._initialize_strategies()

    def _initialize_processors(self) -> None:
        """Initialize input processors"""
        self.processors = {
            'text': TextProcessor(),
            'image': ImageProcessor(),
            'rdb': RDBProcessor()
        }

    def _initialize_strategies(self) -> None:
        """Initialize chunking strategies"""
        self.strategies = {
            'fixed': FixedSizeChunkingStrategy(),
            'recursive': RecursiveChunkingStrategy(),
            'document': DocumentBasedChunkingStrategy(),
            'semantic': SemanticChunkingStrategy(),
            'agentic': AgenticChunkingStrategy()
        }

    def execute(self, context: dict) -> List[str]:
        """
        Execute the chunking operation
        
        1. Process input based on type
        2. Apply selected chunking strategy
        3. Return chunks
        
        :param context: Airflow context
        :return: List of text chunks
        """
        self.input_data = context['task_instance'].xcom_pull(task_ids=self.input_data)
        # Validate input type
        if self.input_type not in self.processors:
            raise ValueError(f"Unsupported input type: {self.input_type}")
            
        # Validate strategy
        if self.strategy not in self.strategies:
            raise ValueError(f"Unknown strategy: {self.strategy}")
            
        self.log.info(f"Processing {self.input_type} input")
        
        # Process input
        processor = self.processors[self.input_type]
        processed_text = processor.process(self.input_data)
        
        # Apply chunking strategy
        strategy = self.strategies[self.strategy]
        self.log.info(f"Applying {self.strategy} chunking strategy")
        
        # Merge strategy_params with context
        params = {**context, **self.strategy_params}
        chunks = strategy.handle(processed_text, **params)
        
        self.log.info(f"Created {len(chunks)} chunks")
        return chunks