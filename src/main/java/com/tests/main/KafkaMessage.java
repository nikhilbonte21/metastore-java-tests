package com.tests.main;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.notification.EntityNotification;

import java.util.Map;

public class KafkaMessage {

    private AtlasEntityHeader entity;
    private long              eventTime;
    private AtlasEntity mutatedDetails;
    private Map<String, String> headers;

    private EntityNotification.EntityNotificationV2.OperationType operationType;

    public AtlasEntityHeader getEntity() {
        return entity;
    }

    public void setEntity(AtlasEntityHeader entity) {
        this.entity = entity;
    }

    public EntityNotification.EntityNotificationV2.OperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(EntityNotification.EntityNotificationV2.OperationType operationType) {
        this.operationType = operationType;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public AtlasEntity getMutatedDetails() {
        return mutatedDetails;
    }

    public void setMutatedDetails(AtlasEntity mutatedDetails) {
        this.mutatedDetails = mutatedDetails;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }
}
