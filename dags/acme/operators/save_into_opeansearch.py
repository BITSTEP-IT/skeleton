# from typing import Any, Dict, List, Optional, Union
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
# import numpy as np
# from opensearchpy import OpenSearch, helpers
# import json
# import uuid

# class SaveIntoOpenSearchOperator(BaseOperator):
#     """
#     Custom Airflow operator that saves vector embeddings into OpenSearch
    
#     :param embeddings: Numpy array of embeddings from EmbeddingOperator
#     :param documents: Original documents/data corresponding to the embeddings
#     :param index_name: Name of the OpenSearch index to save to
#     :param opensearch_conn: OpenSearch connection parameters
#     :param batch_size: Size of batches for bulk indexing
#     :param mapping: Optional custom mapping for the index
#     """
    
#     @apply_defaults
#     def __init__(
#         self,
#         embeddings: Union[np.ndarray, List[np.ndarray]],
#         documents: List[Any],
#         index_name: str,
#         opensearch_conn: Dict[str, Any],
#         batch_size: int = 500,
#         mapping: Optional[Dict] = None,
#         **kwargs
#     ) -> None:
#         super().__init__(**kwargs)

#         # Type check for embeddings
#         if not isinstance(embeddings, (np.ndarray, list)):
#             raise TypeError("Embeddings must be a numpy array or a list of numpy arrays")

#         self.embeddings = embeddings
#         self.documents = documents
#         self.index_name = index_name
#         self.opensearch_conn = opensearch_conn
#         self.batch_size = batch_size
#         self.mapping = mapping or self._get_default_mapping(embeddings.shape[1])
        
#     def _get_default_mapping(self, vector_dim: int) -> Dict[str, Any]:
#         """Create default mapping with knn vector field"""
#         return {
#             "mappings": {
#                 "properties": {
#                     "vector": {
#                         "type": "knn_vector",
#                         "dimension": vector_dim,
#                         "method": {
#                             "name": "hnsw",
#                             "space_type": "l2",
#                             "engine": "nmslib"
#                         }
#                     },
#                     "document": {
#                         "type": "object",
#                         "enabled": True
#                     },
#                     "metadata": {
#                         "type": "object",
#                         "enabled": True
#                     }
#                 }
#             }
#         }
        
#     def _create_index_if_not_exists(self, client: OpenSearch) -> None:
#         """Create the OpenSearch index if it doesn't exist"""
#         if not client.indices.exists(self.index_name):
#             self.log.info(f"Creating index {self.index_name}")
#             client.indices.create(
#                 index=self.index_name,
#                 body=self.mapping
#             )
            
#     def _generate_bulk_data(self) -> List[Dict[str, Any]]:
#         """Generate bulk indexing data"""
#         if len(self.embeddings) != len(self.documents):
#             raise ValueError("Number of embeddings must match number of documents")
            
#         bulk_data = []
#         for i, (embedding, document) in enumerate(zip(self.embeddings, self.documents)):
#             doc = {
#                 "_index": self.index_name,
#                 "_id": str(uuid.uuid4()),
#                 "_source": {
#                     "vector": embedding.tolist(),
#                     "document": document,
#                     "metadata": {
#                         "timestamp": self.execution_date.isoformat(),
#                         "embedding_dim": len(embedding)
#                     }
#                 }
#             }
#             bulk_data.append(doc)
            
#         return bulk_data
        
#     def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
#         """
#         Execute the save operation
        
#         1. Connect to OpenSearch
#         2. Create index if it doesn't exist
#         3. Save embeddings and documents in batches
#         4. Return statistics about the operation
        
#         :param context: Airflow context
#         :return: Dictionary containing operation statistics
#         """
#         self.log.info(f"Connecting to OpenSearch at {self.opensearch_conn['hosts']}")
        
#         # Initialize OpenSearch client
#         client = OpenSearch(
#             hosts=self.opensearch_conn['hosts'],
#             http_auth=self.opensearch_conn.get('http_auth'),
#             use_ssl=self.opensearch_conn.get('use_ssl', True),
#             verify_certs=self.opensearch_conn.get('verify_certs', True),
#             ssl_show_warn=self.opensearch_conn.get('ssl_show_warn', False)
#         )
        
#         # Create index if needed
#         self._create_index_if_not_exists(client)
        
#         # Generate bulk data
#         bulk_data = self._generate_bulk_data()
        
#         # Save in batches
#         success_count = 0
#         error_count = 0
        
#         for i in range(0, len(bulk_data), self.batch_size):
#             batch = bulk_data[i:i + self.batch_size]
#             try:
#                 success, failed = helpers.bulk(
#                     client,
#                     batch,
#                     stats_only=True
#                 )
#                 success_count += success
#                 error_count += failed
#             except Exception as e:
#                 self.log.error(f"Error in batch starting at index {i}: {str(e)}")
#                 raise
                
#         stats = {
#             "total_documents": len(self.documents),
#             "successful_saves": success_count,
#             "failed_saves": error_count,
#             "index_name": self.index_name,
#             "vector_dimension": self.embeddings.shape[1]
#         }
        
#         self.log.info(f"Save operation completed: {json.dumps(stats, indent=2)}")
#         return stats
