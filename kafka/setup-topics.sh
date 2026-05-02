for topic in raw-trades raw-orderbook features; do
    /usr/bin/kafka-topics --create \
      --bootstrap-server kafka:9092 \
      --replication-factor 1 \
      --partitions 2 \
      --if-not-exists \
      --topic $topic
done