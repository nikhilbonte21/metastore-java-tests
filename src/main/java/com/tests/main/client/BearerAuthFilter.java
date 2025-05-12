package com.tests.main.client;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.tests.main.client.okhttp3.ConfigReader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtil.isRunAsAdmin;
import static com.tests.main.utils.TestUtil.isRunAsGod;
import static com.tests.main.utils.TestUtil.isRunAsGuest;
import static com.tests.main.utils.TestUtil.isRunAsMember;


public class BearerAuthFilter extends ClientFilter {

    private static final Logger LOG = LoggerFactory.getLogger(BearerAuthFilter.class);

    private static String GOD, ADMIN, MEMBER, GUEST;

    static {
        GOD = ConfigReader.getString("beta.client.auth.token");
        ADMIN = ConfigReader.getString("beta.client.admin.auth.token");

        MEMBER = ConfigReader.getString("beta.client.member.auth.token");
        GUEST = ConfigReader.getString("beta.client.guest.auth.token");;
    }

    @Override
    public ClientResponse handle(ClientRequest cr) throws ClientHandlerException {
        String token = null;

        if (isRunAsGod()) {
            token = GOD;
        } else if (isRunAsAdmin()) {
            token = ADMIN;
        } else if (isRunAsMember()) {
            token = MEMBER;
        } else if (isRunAsGuest()) {
            token = GUEST;
        }

        if (StringUtils.isNotEmpty(token)) {
            cr.getHeaders().add("Authorization", "Bearer " + token);
        }

        return getNext().handle(cr);
    }
}
