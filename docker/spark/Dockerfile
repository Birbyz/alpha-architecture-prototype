FROM apache/spark:4.0.1

USER root

# Install Python deps inside the Spark image so spark-submit has them
COPY requirements.txt /opt/spark/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /opt/spark/requirements.txt

# Create workspace folder
RUN mkdir -p /workspace && chown -R 185:0 /workspace

USER 185
WORKDIR /workspace
