#
# Copyright 2018 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM confluentinc/cp-server-connect-base:7.5.0

# 1. Instalar datagen mediante confluent-hub (que sí funciona)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest

# 2. Descargar e instalar Debezium para Oracle, más su driver JDBC (ojdbc8)
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-oracle && \
    curl -sS https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/2.4.0.Final/debezium-connector-oracle-2.4.0.Final-plugin.tar.gz \
    | tar -xz -C /usr/share/confluent-hub-components/debezium-connector-oracle --strip-components=1 && \
    curl -sS -o /usr/share/confluent-hub-components/debezium-connector-oracle/ojdbc8.jar \
    https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar

# 3. Descargar e instalar el conector sink de Couchbase
RUN mkdir -p /usr/share/confluent-hub-components/couchbase-kafka-connect-couchbase /tmp/couchbase-extract && \
    curl -sS -o /tmp/couchbase-connector.zip \
    https://packages.couchbase.com/clients/kafka/4.3.5/couchbase-kafka-connect-couchbase-4.3.5.zip && \
    cd /tmp/couchbase-extract && jar -xf /tmp/couchbase-connector.zip && \
    cp -r /tmp/couchbase-extract/couchbase-kafka-connect-couchbase-4.3.5/* /usr/share/confluent-hub-components/couchbase-kafka-connect-couchbase/ && \
    rm -rf /tmp/couchbase-connector.zip /tmp/couchbase-extract