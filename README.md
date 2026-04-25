# Data_Engineering_Python_RealTime_daTa
![Architecture](https://raw.githubusercontent.com/GiorgiMegeneishvili/REPO_NAME/main/docs/architecture.png)
# 🚀 Real-Time Data Engineering Pipeline (Python + Kafka)

## 📌 Project Overview
This project demonstrates a real-time data engineering pipeline built with Python.  
It simulates streaming data ingestion, processing, and storage using modern data engineering tools and practices.

Data from CoinGecko api and send to kafka and Pyspark and PostGresql

The pipeline is designed to handle real-time event data and process it efficiently for downstream analytics.

---

## 🏗️ Architecture

**Flow:**


- Producer: Sends real-time events to Kafka  
- Kafka: Acts as message broker for streaming data  
- Consumer/Processor: Reads and transforms data  
- Sink: Stores processed data for analytics  

---

## ⚙️ Technologies Used

- Python 🐍  
- Apache Kafka 📡  
- Apache Pyspark  
- SQL / Database (PostgreSQL / MSSQL depending on config)  
- JSON event streaming  




🔄 Data Flow
Python producer generates real-time events
Events are serialized into JSON format
Kafka topic receives streaming data
Consumer processes and transforms data
Cleaned data is stored in database or file system
📊 Features
Real-time data streaming
Event-based architecture
Scalable Kafka-based ingestion
Modular Python code structure
Schema-driven event design
📌 Future Improvements
Docker support for full environment setup
Apache Spark Streaming integration
Monitoring with Prometheus / Grafana
Airflow orchestration
Cloud deployment (AWS / Azure)
👨‍💻 Author

Giorgi Megeneishvili
Data Engineer | ETL / Real-Time Systems | Kafka | Python | SQL

⭐ Support

If you like this project, give it a ⭐ on GitHub and feel free to contribute!
