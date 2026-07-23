"""
Preprocess the MongoDB tech-news-embeddings dataset for the clustering benchmark.

Keeps only the 256-dim embedding and writes an engine-neutral `emb : array<float>`
(no null / wrong-length / NaN vectors). See common.embeddings for the shared logic.
"""

from data_preprocessing.common.embeddings import preprocess_embeddings

EMB_DIM = 256
EMB_COL = "embedding"
INPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/tech_news_data"
OUTPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/tech_news_data_processed"


def main() -> None:
    preprocess_embeddings("MongoDB_TechNews_Data_Prep", INPUT_PATH, OUTPUT_PATH, EMB_COL, EMB_DIM)


if __name__ == "__main__":
    main()
