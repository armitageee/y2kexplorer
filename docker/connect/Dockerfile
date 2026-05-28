# Confluent Connect image ships Mirror* connectors only; add Apache file connectors for the demo pipeline.
FROM confluentinc/cp-kafka-connect:7.7.1

USER root
ARG CONNECT_FILE_VERSION=3.7.1
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/kafka/connect-file/${CONNECT_FILE_VERSION}/connect-file-${CONNECT_FILE_VERSION}.jar" \
    -o "/usr/share/java/kafka/connect-file-${CONNECT_FILE_VERSION}.jar" \
    && chmod 644 "/usr/share/java/kafka/connect-file-${CONNECT_FILE_VERSION}.jar"

USER appuser
