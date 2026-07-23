"""
Preprocess the Cohere / MS MARCO embeddings dataset for the clustering benchmark.

Keeps only the 1024-dim embedding and writes an engine-neutral `emb : array<float>`
(no null / wrong-length / NaN vectors). See common.embeddings for the shared logic.
"""

from data_preprocessing.common.embeddings import preprocess_embeddings

EMB_DIM = 1024
EMB_COL = "emb"
INPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/cohere_vectores_data/passages_parquet/"
OUTPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/cohere_vectores_data_preprocessed"


def main() -> None:
    preprocess_embeddings("Cohere_Data_Prep", INPUT_PATH, OUTPUT_PATH, EMB_COL, EMB_DIM)


if __name__ == "__main__":
    main()
