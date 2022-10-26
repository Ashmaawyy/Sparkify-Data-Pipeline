# Sparkify Data Pipieline

<img src="https://upload.wikimedia.org/wikipedia/commons/d/de/AirflowLogo.png">

<br>
<br>

## Pipeline Walkthrough

<br>

- First data is loaded into two staging tables (staged_events & staged_songs).
- Then data is loaded into one fact table and four dimention tables from these two staging tables.
- Then a data quality check test is run on the data to see if there is null values in the userId field.

<br>

## project Build Manual

<br>

- First you need airflow setup to build this project, to install airflow copy these commands in your terminal:
<pre><code>
 AIRFLOW_VERSION=2.4.2
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
 CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/> constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
 pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
</code></pre>