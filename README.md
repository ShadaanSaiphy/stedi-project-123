# STEDI Human Balance Analytics Project

This project processes STEDI customer, accelerometer, and step trainer data using Athena and S3.

Steps completed:
- Created landing tables from JSON files in S3
- Filtered customers who consented to research sharing
- Joined accelerometer data with trusted customers
- Created curated customer dataset
- Linked step trainer data with curated customers
- Built final machine learning curated dataset

Final counts:
customer_landing: 956
accelerometer_landing: 81273
step_trainer_landing: 28680
customer_trusted: 482
accelerometer_trusted: 40981
customer_curated: 482
step_trainer_trusted: 14460
machine_learning_curated: 43681