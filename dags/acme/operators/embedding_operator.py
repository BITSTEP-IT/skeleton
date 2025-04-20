from typing import Any, List, Optional, Union, Dict
from abc import ABC, abstractmethod
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import numpy as np
from transformers import AutoTokenizer, AutoModel, AutoImageProcessor, AutoModelForImageClassification
import torch
from PIL import Image
import io

class EmbeddingModel(ABC):
    """Abstract base class for embedding models"""
    
    @abstractmethod
    def embed(self, input_data: Any) -> np.ndarray:
        pass

class SentenceTransformerModel(EmbeddingModel):
    """Text embedding model using Sentence Transformers"""
    
    def __init__(self, model_name: str = 'sentence-transformers/all-MiniLM-L6-v2'):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        
    def embed(self, input_data: Union[str, List[str]]) -> np.ndarray:
        # Handle both single strings and lists of strings
        if isinstance(input_data, str):
            texts = [input_data]
        else:
            texts = input_data
            
        # Tokenize and get embeddings
        inputs = self.tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
        with torch.no_grad():
            outputs = self.model(**inputs)
            # Use mean pooling to get sentence embeddings
            embeddings = outputs.last_hidden_state.mean(dim=1)
            
        return embeddings.numpy()

class VisionTransformerModel(EmbeddingModel):
    """Image embedding model using Vision Transformers"""
    
    def __init__(self, model_name: str = 'google/vit-base-patch16-224'):
        self.processor = AutoImageProcessor.from_pretrained(model_name)
        self.model = AutoModelForImageClassification.from_pretrained(model_name)
        
    def embed(self, input_data: Union[str, bytes, Image.Image, List[Union[str, bytes, Image.Image]]]) -> np.ndarray:
        if not isinstance(input_data, list):
            input_data = [input_data]
            
        processed_images = []
        for img in input_data:
            if isinstance(img, str):
                image = Image.open(img)
            elif isinstance(img, bytes):
                image = Image.open(io.BytesIO(img))
            elif isinstance(img, Image.Image):
                image = img
            else:
                raise ValueError("Unsupported image input type")
            processed_images.append(image)
            
        inputs = self.processor(images=processed_images, return_tensors="pt")
        with torch.no_grad():
            outputs = self.model(**inputs)
            # Use the last hidden state for embeddings
            embeddings = outputs.logits
            
        return embeddings.numpy()

class EmbeddingOperator(BaseOperator):
    """
    Custom Airflow operator that generates embeddings for different types of input data
    
    :param input_data: Input data to be embedded (text, image, or chunks)
    :param input_type: Type of input ('text' or 'image')
    :param model_name: Name of the pre-trained model to use for embeddings
    :param batch_size: Size of batches for processing large inputs
    :param normalize: Whether to L2 normalize the embeddings
    """
    
    @apply_defaults
    def __init__(
        self,
        input_data: Any,
        input_type: str = 'text',
        model_name: Optional[str] = None,
        batch_size: int = 32,
        normalize: bool = True,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.input_data = input_data
        self.input_type = input_type
        self.model_name = model_name
        self.batch_size = batch_size
        self.normalize = normalize
        self._initialize_model()

    def _initialize_model(self) -> None:
        """Initialize the appropriate embedding model based on input type"""
        if self.input_type == 'text':
            model_name = self.model_name or 'sentence-transformers/all-MiniLM-L6-v2'
            self.model = SentenceTransformerModel(model_name)
        elif self.input_type == 'image':
            model_name = self.model_name or 'google/vit-base-patch16-224'
            self.model = VisionTransformerModel(model_name)
        else:
            raise ValueError(f"Unsupported input type: {self.input_type}")

    def _batch_process(self, data: List[Any]) -> np.ndarray:
        """Process data in batches"""
        embeddings = []
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            batch_embeddings = self.model.embed(batch)
            embeddings.append(batch_embeddings)
        return np.vstack(embeddings)

    def _normalize_embeddings(self, embeddings: np.ndarray) -> np.ndarray:
        """L2 normalize embeddings"""
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        return embeddings / norms

    def execute(self, context: Dict[str, Any]) -> np.ndarray:
        """
        Execute the embedding operation
        
        1. Validate and prepare input data
        2. Generate embeddings in batches
        3. Normalize if requested
        4. Return embeddings array
        
        :param context: Airflow context
        :return: numpy array of embeddings
        """
        self.input_data = context['task_instance'].xcom_pull(task_ids=self.input_data)
        self.log.info(f"Generating embeddings for {self.input_type} input")
        
        # Ensure input is a list
        if not isinstance(self.input_data, list):
            data = [self.input_data]
        else:
            data = self.input_data
            
        # Generate embeddings
        embeddings = self._batch_process(data)
        
        # Normalize if requested
        if self.normalize:
            embeddings = self._normalize_embeddings(embeddings)
            
        self.log.info(f"Generated embeddings with shape: {embeddings.shape}")
        return embeddings
