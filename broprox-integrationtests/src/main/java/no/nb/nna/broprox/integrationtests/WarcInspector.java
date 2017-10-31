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
package no.nb.nna.broprox.integrationtests;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 *
 */
public class WarcInspector {

    final static OkHttpClient CLIENT = new OkHttpClient.Builder().followRedirects(true).build();

    final static Gson GSON = new Gson();

    final static HttpUrl WARC_SERVER_URL;

    static {
        String warcServerHost = System.getProperty("contentexplorer.host");
        int warcServerPort = Integer.parseInt(System.getProperty("contentexplorer.port"));

        WARC_SERVER_URL = new HttpUrl.Builder()
                .scheme("http")
                .host(warcServerHost)
                .port(warcServerPort)
                .build();
    }

    private WarcInspector() {
    }

    public static WarcFileSet getWarcFiles() throws UncheckedIOException {
        HttpUrl url = WARC_SERVER_URL.resolve("warcs");

        Request request = new Request.Builder()
                .url(url)
                .header(HttpHeaders.ACCEPT, "application/json")
                .build();

        try (Response response = CLIENT.newCall(request).execute();) {
            if (response.isSuccessful()) {
                return new WarcFileSet(GSON.fromJson(response.body().charStream(), List.class)
                        .stream().map(m -> new WarcFile(m)));
            } else {
                throw new IOException("Unexpected code " + response);
            }
        } catch(IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static void deleteWarcFiles() {
        HttpUrl url = WARC_SERVER_URL.resolve("warcs");

        Request request = new Request.Builder()
                .delete()
                .url(url)
                .build();

        try (Response response = CLIENT.newCall(request).execute();) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
        } catch(IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static String getWarcHeadersForStorageRef(String storageRef) {
        HttpUrl url = WARC_SERVER_URL.resolve("storageref/" + storageRef + "/warcheader");

        Request request = new Request.Builder()
                .url(url)
                .build();

        try (Response response = CLIENT.newCall(request).execute();) {
            if (response.isSuccessful()) {
                return response.body().string();
            } else {
                throw new IOException("Unexpected code " + response);
            }
        } catch(IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static byte[] getWarcContentForStorageRef(String storageRef) {
        HttpUrl url = WARC_SERVER_URL.resolve("storageref/" + storageRef);

        Request request = new Request.Builder()
                .url(url)
                .build();

        try (Response response = CLIENT.newCall(request).execute();) {
            if (response.isSuccessful()) {
                return response.body().bytes();
            } else {
                throw new IOException("Unexpected code " + response);
            }
        } catch(IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
