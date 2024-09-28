1. Create an account on Google Cloud

2. Set Up Google Cloud Project
Create a new Google Cloud Project if you don’t have one already.

3. Install the Google Cloud CLI and authenticate
https://cloud.google.com/sdk/docs/install#deb

4. Initialize gcloud on the command line and select the project you want to work with


5. Enable necessary APIs:

Cloud Run API
Pub/Sub API
BigQuery API
Dataflow API (for Apache Beam)

Command: gcloud services enable run.googleapis.com pubsub.googleapis.com bigquery.googleapis.com dataflow.googleapis.com

6. Create a Bigquery Dataset
Command: bq mk --dataset my_project:binance_data

7. Create Pub/Sub Topic and Subscription
Pub/Sub will serve as the message broker between your WebSocket data and Apache Beam pipeline.

Create a Pub/Sub Topic and Subscription for Crypto data.


# Create a Pub/Sub topic
gcloud pubsub topics create binance-topic

# Create a subscription to the topic
gcloud pubsub subscriptions create binance-subscription --topic=binance-topic

