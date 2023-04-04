/*
 *  Copyright (c) Telicent Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.jena.fuseki.kafka;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class MockKafka {

    private final KafkaContainer kafkaContainer;
    private final String bootstrap;
    private final AdminClient admin;

    public MockKafka() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.3"));
        kafkaContainer.start();
        bootstrap = kafkaContainer.getBootstrapServers();
        admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokers()));
    }

    private String getKafkaBrokers() {
        Integer mappedPort = kafkaContainer.getFirstMappedPort();
        return String.format("%s:%d", "localhost", mappedPort);
      }

    public String getServer() { return bootstrap; }

    public void createTopic(String topic) {
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

        admin.createTopics(List.of(newTopic));
    }

    public void deleteTopic(String topic) {
        admin.deleteTopics(List.of(topic));
    }

    public void stop() {
        kafkaContainer.stop();
    }

    public Collection<String> listTopics() {
        try {
            return admin.listTopics().names().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
