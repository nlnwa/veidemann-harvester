/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.broprox.frontier.worker;

import java.util.Optional;

import com.google.gson.reflect.TypeToken;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import no.nb.nna.broprox.db.DbObjectFactory;
import no.nb.nna.broprox.db.model.QueuedUri;

/**
 *
 */
public class HarvesterClient implements AutoCloseable {

    final static Client CLIENT = ClientBuilder.newClient();

    final WebTarget harvesterTarget;

    private static final TypeToken URI_ARRAY_TYPE = new TypeToken<QueuedUri[]>() {
    };

    public HarvesterClient(final String host, final int port) {
        harvesterTarget = CLIENT.target(UriBuilder.fromPath("/").host(host).port(port).scheme("http").build());
        System.out.println("Target: " + harvesterTarget);
    }

    public QueuedUri[] fetchPage(String executionId, QueuedUri qUri) {
        try {
            String request = qUri.toJson();

            Response outlinkResponse = harvesterTarget.path("fetch")
                    .queryParam("executionId", executionId)
                    .request()
                    .accept(MediaType.APPLICATION_JSON)
                    .post(Entity.entity(request, MediaType.APPLICATION_JSON), Response.class);

            if (outlinkResponse.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
                throw new WebApplicationException(outlinkResponse);
            }

            String json = outlinkResponse.readEntity(String.class);
            Optional<QueuedUri[]> outlinks = DbObjectFactory.of(URI_ARRAY_TYPE, json);
            return outlinks.get();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        CLIENT.close();
    }

}
