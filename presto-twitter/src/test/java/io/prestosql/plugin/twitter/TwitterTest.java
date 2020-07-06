/*
 * $Id: $
 * $Revision: $
 * $Author: $
 * $Date: $
 * Copyright (c) 2020 Trustwave Holdings, Inc.
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information
 * of Trustwave Holdings, Inc.  Use of this software is governed by
 * the terms and conditions of the license statement and limited
 * warranty furnished with the software.
 *
 * IN PARTICULAR, YOU WILL INDEMNIFY AND HOLD TRUSTWAVE HOLDINGS INC.,
 * ITS RELATED COMPANIES AND ITS SUPPLIERS, HARMLESS FROM AND AGAINST
 * ANY CLAIMS OR LIABILITIES ARISING OUT OF OR RESULTING FROM THE USE,
 * MODIFICATION, OR DISTRIBUTION OF PROGRAMS OR FILES CREATED FROM,
 * BASED ON, AND/OR DERIVED FROM THIS SOURCE CODE FILE.
 */
package io.prestosql.plugin.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;

import org.glassfish.jersey.client.internal.HttpUrlConnector;
import org.testng.annotations.Test;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;

/**
 * --- TODO: Class comments go here ---
 *
 * <b><pre>
 * Copyright (c) 2020 Trustwave Holdings, Inc.
 * All rights reserved.
 * </pre></b>
 *
 * @author bolsen
 * @version $Revision: $
 */
public class TwitterTest {

    private JsonCodecFactory jsonCodecFactory = new JsonCodecFactory();


    @Test
    public void test() throws IOException {
        Form form = new Form();
        form.param("grant_type", "client_credentials");

        String accessToken = "";

        {

            Client client = ClientBuilder.newClient();
            String response = "";
            try {
                response = client.target("https://api.twitter.com/oauth2/token")
                        .request(MediaType.APPLICATION_FORM_URLENCODED)
                        .acceptEncoding("UTF-8")
                        .header("Authorization", getBasicAuthentication("my-twitter-key",
                                "my-twitter-secret"))
                        .post(Entity.form(form), String.class);
            }
            catch (Exception e) {
                System.out.println(e);
            }

            JsonCodec<Map<String, String>> codec = jsonCodecFactory.mapJsonCodec(String.class, String.class);

            Map<String, String> responseMap = codec.fromJson(response);

            accessToken = responseMap.get("access_token");

            System.out.println(accessToken);

            client.close();
        }


        {
            Client client = ClientBuilder.newClient();
            Response response = null;


            try {
                response = client.target("https://api.twitter.com/labs/1/tweets/stream/sample")
                        .request(MediaType.APPLICATION_JSON)
                        .header("Authorization", getBearerAuthentication(accessToken))
                        .get();
            }
            catch (Exception e) {
                System.out.println(e);
            }

            HttpUrlConnector entity = (HttpUrlConnector)response.getEntity();

         /*   if (null != entity) {
                BufferedReader reader = new BufferedReader(new InputStreamReader());
                String line = reader.readLine();
                while (line != null) {
                    System.out.println(line);
                    line = reader.readLine();
                }
            }*/

            client.close();
        }

    }

    private String getBasicAuthentication(String user, String password) {
        String token = user + ":" + password;
        try {
            return "Basic " + DatatypeConverter.printBase64Binary(token.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException("Cannot encode with UTF-8", ex);
        }
    }

    private String getBearerAuthentication(String token) {
            return "Bearer " + token;
    }
}
