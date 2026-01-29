FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/openskynetwork/opensky-api.git

RUN pip install --no-cache-dir ./opensky-api/python

RUN pip install --no-cache-dir kafka-python

COPY ./producer/scripts/ /app/scripts/

CMD ["sleep", "infinity"]
