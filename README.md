# Airflow Web Log Processing

This repository contains an Apache Airflow DAG for processing web log files. The workflow includes tasks for extracting, compressing, transforming, encrypting, and archiving log data.

## Project Structure
airflow-web-log-processing/
├── dags/
│   └── process_web_log_dag.py
├── README.md
├── .gitignore
└── requirements.txt


- `dags/`: Contains the Airflow DAG file.
- `README.md`: This file with project information.
- `requirements.txt`: Python dependencies (if needed).

## DAG Description

- **`extract_data_task`**: Extracts data from a log file.
- **`compress_data_task`**: Compresses the extracted data.
- **`transform_data_task`**: Transforms the compressed data by filtering out specific entries.
- **`encrypt_data_task`**: Encrypts the transformed data.
- **`load_data_task`**: Archives the encrypted data.
- **`cleanup_task`**: Cleans up intermediate files.

## Setup Instructions

1. **Install Apache Airflow:**

   Follow the [official installation guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html) to set up Apache Airflow in your environment.

2. **Add DAG File:**

   Place `process_web_log_dag.py` in your Airflow `dags` directory.

3. **Install Dependencies:**

   Create a `requirements.txt` file with any additional Python dependencies. For this project, you may need `apache-airflow`:
   pip install -r requirements.txt


 4.  Configure Airflow:

Follow Airflow's setup instructions to configure your environment, including setting up connections and variables as needed for your DAG.

5.  Start Airflow:

Run the Airflow web server and scheduler. Follow the instructions in the Airflow documentation for starting these services.

6.  Verify Setup:

Once Airflow is running, navigate to the Airflow web UI to ensure that the process_web_log_dag appears and is functioning as expected.
