package com.tests.main;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.type.AtlasType;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AtlasKafkaConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasKafkaConsumer.class);

    private static KafkaConsumer<String, String> CONSUMER = null;
    static {

        String bootstrapServers = "localhost:9092";
        String groupId = "atlas";

        // Set consumer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        // Create consumer
        CONSUMER = new KafkaConsumer<>(props);

        // Subscribe to topic
        CONSUMER.subscribe(Collections.singletonList("ATLAS_ENTITIES"));

        //CONSUMER.assign(Collections.singletonList(partitionToSeek));

        LOG.info("Created AtlasKafkaConsumer");
    }

/*    public static void poll(int numberOfRecords) {
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            pollMessages(numberOfRecords);
            return true;
        });
    }*/

    public static Map<String, KafkaMessage> pollMessages(int numberOfRecords) {
        Map<String, KafkaMessage> messages = new HashMap<>(numberOfRecords);

        ConsumerRecords<String, String> records = CONSUMER.poll(Duration.ofMillis(10000));

        // Convert ConsumerRecords to a List
        List<ConsumerRecord<String, String>> recordList = new ArrayList<>();
        records.forEach(recordList::add);

        // Get the last 7 records
        int size = recordList.size();
        List<ConsumerRecord<String, String>> chunk = recordList.subList(Math.max(0, size - numberOfRecords), size);

        chunk.forEach(x -> {
            KafkaMessage message = deSerialise(x);
            messages.put(message.getEntity().getGuid(), message);
        });

        return messages;
    }

    @PreDestroy
    public void preDestroy() {
        LOG.info("Closing AtlasKafkaConsumer");
        CONSUMER.close();
    }

    private static KafkaMessage deSerialise(ConsumerRecord<String, String> record) {
        KafkaMessage message = new KafkaMessage();

        Map rawMessage = (Map) AtlasType.fromJson(record.value(), Map.class).get("message");

        message.setEventTime((Long) rawMessage.get("eventTime"));
        message.setHeaders((Map<String, String>) rawMessage.get("headers"));

        message.setEntity(AtlasType.fromJson(AtlasType.toJson(rawMessage.get("entity")), AtlasEntityHeader.class));
        message.setMutatedDetails(AtlasType.fromJson(AtlasType.toJson(rawMessage.get("mutatedDetails")), AtlasEntity.class));

        message.setOperationType(EntityNotification.EntityNotificationV2.OperationType.valueOf(rawMessage.get("operationType").toString()));

        return message;
    }
}
