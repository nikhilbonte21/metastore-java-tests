package com.tests.main.client;

import com.sun.jersey.api.client.ClientResponse;

public class AtlasServiceException extends Exception {
    private ClientResponse.Status status;

    public AtlasServiceException(AtlasBaseClient.API api, Exception e) {
        super("Metadata service API " + api.getMethod() + " : " + api.getNormalizedPath() + " failed", e);
    }

    private AtlasServiceException(AtlasBaseClient.API api, ClientResponse.Status status, String response) {
        super("Metadata service API " + api + " failed with status " + (status != null ? status.getStatusCode() : -1)
                + " (" + status + ") Response Body (" + response + ")");
        this.status = status;
    }

    public AtlasServiceException(AtlasBaseClient.API api, ClientResponse response) {
        this(api, ClientResponse.Status.fromStatusCode(response.getStatus()), response.getEntity(String.class));
    }

    public AtlasServiceException(Exception e) {
        super(e);
    }

    public AtlasServiceException(AtlasServiceException e) {
        super(e);
        this.status = e.status;
    }

    public ClientResponse.Status getStatus() {
        return status;
    }
}
