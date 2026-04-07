# Intelligent Supply Chain Decision Recommendation System

## Overview
This project implements an intelligent supply chain analytics system using Big Data technologies and Machine Learning. It processes streaming order data in real time, predicts delivery delays and profitability, and generates actionable business decisions to optimize operations.

## Key Features
- Real-time data streaming using Apache Kafka
- Distributed data processing with PySpark
- Machine Learning-based delay prediction
- Profit-based risk scoring mechanism
- Automated decision engine for order optimization

## Tech Stack
- Apache Kafka (Data Streaming)
- Apache Spark / PySpark (Processing & ML Inference)
- Python (Data Processing & Model Training)
- Scikit-learn (Machine Learning)

## System Architecture
Data Source → Kafka → PySpark Streaming → ML Prediction → Risk Scoring → Decision Engine

## Workflow
1. Order data is streamed via Kafka
2. PySpark processes incoming data in real time
3. ML model predicts delivery delay
4. Profit is analyzed to compute risk score
5. Decision engine recommends actions:
   - Accept Order
   - Change Shipping Mode
   - Reduce Discount
   - 
   - Reject Order

## Output Example
Predicted Delay: 2.8 days
Predicted Profit: -5 USD
Risk Score: 78
Decision: Reduce Discount


## Key Contributions
- Combines Big Data pipeline with Machine Learning inference
- Moves beyond prediction to decision intelligence
- Simulates real-time supply chain optimization
- Provides business-oriented actionable insights

## Future Enhancements
- Real-time route optimization
- Demand forecasting using time-series models
- Reinforcement learning for automated decision-making
- Interactive dashboard for monitoring

## Author
Mit Mhatre
