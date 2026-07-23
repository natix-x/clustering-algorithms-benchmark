"""
Preprocess the jasperai/monet dataset for the clustering benchmark.

Keeps only the 512-dim CLIP (ViT-B/32) embedding and writes an engine-neutral
`emb : array<float>` (no null / wrong-length / NaN vectors). See common.embeddings for
the shared logic.
"""

from data_preprocessing.common.embeddings import preprocess_embeddings

EMB_DIM = 512
EMB_COL = "embedding_clip-vit-base-patch32"
INPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/monet_data"
OUTPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/monet_data_processed"


def main() -> None:
    preprocess_embeddings("Monet_Data_Prep", INPUT_PATH, OUTPUT_PATH, EMB_COL, EMB_DIM)


if __name__ == "__main__":
    main()
