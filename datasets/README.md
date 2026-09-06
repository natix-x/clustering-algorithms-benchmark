# Datasets

## Gaia Data Release 3 (GAIA DR3)
The European Space Agency (ESA) Gaia mission provides the most precise 3D map of the Milky Way. Data Release 3 (DR3) contains billions of astronomical observations. This work builds an 8-feature vector per star: 3D position (Cartesian X, Y, Z, from parallax distance and sky coordinates), proper motion (in right ascension and declination), and GSP-Phot stellar parameters (effective temperature, surface gravity, metallicity). It serves as an I/O and memory-pressure benchmark: streaming its large, low-dimensional raw release (~681 GB) tests data-streaming throughput and JVM garbage collection, even though the cleaned set is a moderate fraction of the trip records below.

Preprocessing strictly filters invalid distances (`parallax > 0`) and removes NULLs, which also drops stars lacking GSP-Phot parameters (a large fraction), performs spherical-to-Cartesian geometry transformations, and standardizes the final feature vector:
- 434,715,499 rows, ~26 GB

* Source & License: [AWS Open Data Registry](https://registry.opendata.aws/mast-gaia-dr3/) | Open Scientific Data
* Citation: Gaia Collaboration, A. Vallenari, et al., "Gaia Data Release 3: Summary of the content and survey properties," *Astronomy & Astrophysics*, vol. 674, A1, 2023, doi: 10.1051/0004-6361/202243940. Accessed via the [AWS Open Data Registry (MAST Gaia DR3)](https://registry.opendata.aws/mast-gaia-dr3/), [Accessed: 23-Jul-2026].

---

## NYC Taxi & Limousine Commission (TLC) Trip Record Data
The TLC publishes several trip-record datasets (yellow taxi, green taxi, for-hire and high-volume for-hire vehicles). This work uses the yellow taxi records for January 2011 to January 2025: one row per trip with pickup/dropoff time and zone, passenger count, trip distance and fare. With ~1.39 billion cleaned rows it is the **largest dataset in this benchmark by record count** — the main large-scale test. Its trips are heavily concentrated in Manhattan, which also makes it a natural test for spatio-temporal clustering and for data skew (unevenly distributed data that overloads individual workers).

Preprocessing filters outliers, removes NULLs, encodes time cyclically (sine/cosine), maps the pickup AND dropoff zones to geographic coordinates (the records carry zone ids, not coordinates — TLC dropped raw lat/lon), and standardizes the 10 clustering features:
- before: 1,441,816,049 rows
- after: 1,382,278,233 rows (dropped 59,537,816 = 4.1%), ~13 GB, 215 files

* Source & License: [NYC Open Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | CC BY 4.0
* Citation: New York City Taxi and Limousine Commission, "TLC Trip Record Data," NYC Open Data, 2025. [Online]. Available: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page. [Accessed: 23-Jul-2026].

---

## MongoDB Tech-News Embeddings
Dense 256-dimensional text embeddings produced by OpenAI's `text-embedding-3-small` model from the HackerNoon corpus of curated news articles about technology companies. Each row is one medium-dimensional semantic vector with associated article metadata. It is the **medium-dimensional test** of this benchmark: it fills the gap between the low-dimensional tabular sets and other embeddings data, isolating the effect of moderate dimensionality on clustering quality without the extreme volume of the other sets, and doubling as a fast-iteration dataset for tuning algorithm configurations.

Preprocessing keeps only the embedding vector (drops the article text and metadata) and casts arrays to engine-neutral vector types, so the row count is unchanged and only the physical footprint shrinks:
- after: 1,576,524 rows, ~1.6 GB

* Source & License: [Hugging Face](https://huggingface.co/datasets/MongoDB/tech-news-embeddings) | Apache 2.0
* Citation: MongoDB, "tech-news-embeddings," Hugging Face, 2024. [Online]. Available: https://huggingface.co/datasets/MongoDB/tech-news-embeddings. [Accessed: 23-Jul-2026].

---

## jasperai/monet
512-dimensional CLIP (ViT-B/32) image embeddings, published by jasperai. Each row is one semantic image vector. A mid-to-high-dimensional embedding set (512 dims) sitting between the MongoDB (256) and Cohere (1024) sets, for comparing clustering behaviour across embedding dimensionalities.

Preprocessing keeps only the embedding vector and stores it as an engine-neutral `array<float>`, dropping rows with null/malformed/NaN vectors:
- after: 103,807,773 rows, ~[TO DO] GB

* Source & License: [Hugging Face](https://huggingface.co/datasets/jasperai/monet) | see the Hugging Face dataset card
* Citation: jasperai, "monet," Hugging Face, 2024. [Online]. Available: https://huggingface.co/datasets/jasperai/monet. [Accessed: 23-Jul-2026].

---

## Cohere MS MARCO v2.1 Embeddings
Dense 1024-dimensional text embeddings produced by Cohere's `embed-english-v3.0` model from the MS MARCO corpus. Each row is one high-dimensional semantic vector. With ~113 million such vectors, it serves as a **high-dimensional, high-volume stress test**: it generates heavy CPU load (vector math) and large communication overhead (broadcasting large matrices) in the distributed setting.

Preprocessing keeps only the embedding vector (drops raw text and metadata) and casts arrays to engine-neutral vector types, so the row count is unchanged and only the physical footprint shrinks:
- after: 113,522,427 rows, ~196 GB

* Source & License: [Hugging Face](https://huggingface.co/datasets/CohereLabs/msmarco-v2.1-embed-english-v3) | Apache 2.0 (Embeddings) & MS MARCO v2.1 License (Text)
* Citation:
  1. Cohere, "MS MARCO v2.1 English Embeddings (embed-english-v3.0)," Hugging Face, 2024. [Online]. Available: https://huggingface.co/datasets/CohereLabs/msmarco-v2.1-embed-english-v3. [Accessed: 23-Jul-2026].
  2. P. Bajaj et al., "MS MARCO: A Human Generated MAchine Reading COmprehension Dataset," *arXiv preprint arXiv:1611.09268*, 2016.

---

## Running the preprocessing
Each dataset is cleaned by a single-node Spark job under
`python/data_preprocessing/<dataset>/`. All jobs share one generic SLURM script
(`sbatch_scripts/preprocess.sbatch`); a small wrapper submits it per dataset with the
right script path, walltime and Spark cores. From the repo root on Ares:

```bash
./sbatch_scripts/run.sh <dataset>   # dataset: gaia | nyc | cohere | monet | tech_news
```

