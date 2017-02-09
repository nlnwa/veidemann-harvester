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
package no.nb.nna.broprox.frontier.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.RethinkDbAdapter;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 *
 */
@Path("/")
public class StatsResource {

    static final byte[] CRLF = {CR, LF};

    static final RethinkDB r = RethinkDB.r;

    @Context
    DbAdapter db;

    public StatsResource() {
    }

    @Path("status")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getStatus() {
        RethinkDbAdapter rethink = (RethinkDbAdapter) db;
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        Map<String, Object> json = new HashMap<>();
        json.put("queue_size", rethink.executeRequest(r.table("uri_queue").count()));
        json.put("screenshot_size", rethink.executeRequest(r.table("screenshot").count()));

        List<String> uris = new ArrayList<>();
        try (Cursor<Map<String, String>> cursor = rethink.executeRequest(r.table("uri_queue").pluck("uri"));) {
            for (Map<String, String> doc : cursor) {
                uris.add(doc.get("uri"));
            }
            json.put("queued_uris", uris);
        }

        return gson.toJson(json);
    }

    @DELETE
    public void deleteDb() {
        System.out.println("DELETING DB");
        RethinkDbAdapter rethink = (RethinkDbAdapter) db;
        rethink.executeRequest(r.table("crawl_log").delete());
        rethink.executeRequest(r.table("screenshot").delete());
        rethink.executeRequest(r.table("uri_queue").delete());
        rethink.executeRequest(r.table("crawled_content").delete());
        rethink.executeRequest(r.table("extracted_text").delete());
    }
}
