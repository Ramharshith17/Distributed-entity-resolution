# Distributed Entity Resolution and Record Linkage

## 1. Introduction
This project identifies duplicate customer records across CSV and JSON datasets using Apache Spark. It applies data preprocessing, candidate blocking, similarity scoring, and match classification.

## 2. Objectives
- Process heterogeneous customer data.
- Reduce record comparisons using blocking.
- Calculate matching confidence scores.
- Evaluate results using precision, recall, and F1-score.
- Compare all-pairs matching with blocking.
- Display results in a Streamlit dashboard.

## 3. Technologies
Python, Apache Spark 3.5.1, Pandas, Streamlit, Ubuntu WSL, CSV, and JSON.

## 4. Project Structure
- `data/`: sample CSV and JSON files
- `src/`: Spark testing, matching, evaluation, and performance scripts
- `output/`: matching results
- `results/`: performance results
- `dashboard.py`: Streamlit dashboard

## 5. Setup
Requirements: Java 11, Apache Spark 3.5.1, Python 3, and Ubuntu/WSL.

Install dashboard dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install streamlit pandas
```

## 6. Execution Instructions

Run these commands from the project root.

Test Spark:

```bash
/usr/local/spark/bin/spark-submit --master 'local[*]' src/test_spark.py
```

Run matching:

```bash
/usr/local/spark/bin/spark-submit --master 'local[*]' src/matching.py
```

Evaluate results:

```bash
/usr/local/spark/bin/spark-submit --master 'local[*]' src/evaluation.py
```

Measure performance:

```bash
/usr/local/spark/bin/spark-submit --master 'local[*]' src/performance.py
```

Launch the dashboard:

```bash
source .venv/bin/activate
python -m streamlit run dashboard.py
```

Open `http://localhost:8501` in your browser.

## 7. Proposed Architecture
Data ingestion → preprocessing → candidate blocking → similarity scoring → match classification → evaluation → dashboard visualization.

## 8. Results and Limitations
The scripts calculate matching metrics and compare candidate counts and execution times. Actual results must be obtained by running the scripts. The supplied dataset is a small demonstration, and Spark runs locally rather than on a multi-machine cluster.

## 9. Team Responsibilities
- Member 1: Data preparation and preprocessing.
- Member 2: Matching algorithm and confidence scoring.
- Member 3: Evaluation, performance testing, and dashboard.

## 10. Conclusion
The project demonstrates entity resolution across heterogeneous data sources using Spark. Blocking can reduce unnecessary comparisons, while similarity scoring supports duplicate identification. Larger datasets and broader testing are needed to assess real-world scalability.
