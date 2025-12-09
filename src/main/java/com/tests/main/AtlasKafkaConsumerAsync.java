package com.tests.main;

import com.tests.main.utils.TestUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.type.AtlasType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AtlasKafkaConsumerAsync implements Runnable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasKafkaConsumerAsync.class);

    private KafkaConsumer<String, String> consumer = null;

    private final Map<String, List<KafkaMessage>> messages = new HashMap<>();

    public AtlasKafkaConsumerAsync() {
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
        this.consumer = new KafkaConsumer<>(props);

        // Subscribe to topic
        consumer.subscribe(Collections.singletonList("ATLAS_ENTITIES"));

        LOG.info("Created AtlasKafkaConsumerAsync");
    }

    @Override
    public void run() {
        // Poll in a loop until partitions are assigned.
        // This is necessary before we can manually seek.
        while (consumer.assignment().isEmpty()) {
            LOG.info("Waiting for partition assignment...");
            consumer.poll(Duration.ofMillis(1000));
        }

        // Now that we have partitions, seek to the end of all of them
        LOG.info("Partitions assigned. Seeking to end of all assigned partitions.");
        consumer.seekToEnd(consumer.assignment());
        while (true) {
            LOG.info("Polling for new Kafka messages...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            // Convert ConsumerRecords to a List
            List<ConsumerRecord<String, String>> recordList = new ArrayList<>();
            records.forEach(recordList::add);

            LOG.info("Found {} messages...", recordList.size());
            recordList.forEach(x -> {
                KafkaMessage message = deSerialise(x);
                List<KafkaMessage> currentMessages = null;

                if (messages.containsKey(message.getEntity().getGuid())) {
                    currentMessages = messages.get(message.getEntity().getGuid());
                } else {
                    currentMessages = new ArrayList<>(1);
                }
                currentMessages.add(0, message);
                messages.put(message.getEntity().getGuid(), currentMessages);
            });

            TestUtil.sleep(5000);
        }
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

    public List<KafkaMessage> getMessages(String guid) {
        return messages.get(guid);
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing AtlasKafkaConsumerAsync");
        consumer.close();
    }
}
