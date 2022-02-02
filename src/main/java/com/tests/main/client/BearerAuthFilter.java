package com.tests.main.client;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.atlas.AtlasException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BearerAuthFilter extends ClientFilter {

    private static final Logger LOG = LoggerFactory.getLogger(BearerAuthFilter.class);
    @Override
    public ClientResponse handle(ClientRequest cr) throws ClientHandlerException {
        String token = null;
        try {
            token = ClientBuilder.getProperties().getString("beta.client.auth.token");
        } catch (AtlasException e) {
            LOG.error("Failed to get properties");
            e.printStackTrace();
        }
        if (StringUtils.isNotEmpty(token)) {
            cr.getHeaders().add("Authorization", "Bearer " + token);
        }
        return getNext().handle(cr);
    }
}
