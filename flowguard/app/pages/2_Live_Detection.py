# app/pages/2_Live_Detection.py
# Real-time detection view for the demo.
# Controls: choose the CSV day (Monday benign, Wednesday or Friday attacks), replay rate in
# events per second, start and stop the replay into Kafka (calls core/kafka_client.py).
# Live part: auto-refreshing feed of new alerts, running count of scored vs flagged flows,
# and a live chart of reconstruction error against the threshold.
