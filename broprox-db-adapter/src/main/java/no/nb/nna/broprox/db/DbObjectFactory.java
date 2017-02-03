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
package no.nb.nna.broprox.db;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 *
 */
public final class DbObjectFactory {

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapterFactory(new TypeAdapterFactory() {
                @Override
                public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                    final Class<T> rawType = (Class<T>) type.getRawType();

                    if (!rawType.isInterface()) {
                        return null;
                    }

                    Map<String, TypeAdapter> fieldTypes = new LinkedHashMap<>();
                    for (Method m : rawType.getMethods()) {
                        if (m.getName().startsWith("with") && m.getParameterTypes().length == 1) {
                            String fieldName = uncapFieldName(m.getName(), 4);
                            fieldTypes.put(fieldName, gson.getAdapter(m.getParameterTypes()[0]));
                        }
                    }

                    return (TypeAdapter<T>) new TypeAdapter<Object>() {
                        public void write(JsonWriter out, Object value) throws IOException {
                            if (value == null) {
                                out.nullValue();
                            } else {
                                System.out.println("WRITE VAL: " + value);
                            }
                        }

                        public Object read(JsonReader in) throws IOException {
                            switch (in.peek()) {
                                case BEGIN_OBJECT:

                                    DbObject result = (DbObject) DbObjectFactory.create(rawType);
                                    in.beginObject();
                                    while (in.hasNext()) {
                                        String fieldName = in.nextName();
                                        result.getMap().put(fieldName, fieldTypes.get(fieldName).read(in));
                                    }
                                    in.endObject();
                                    return result;

                                case STRING:
                                    return in.nextString();

                                case NUMBER:
                                    return in.nextDouble();

                                case BOOLEAN:
                                    return in.nextBoolean();

                                case NULL:
                                    in.nextNull();
                                    return null;

                                default:
                                    throw new IllegalStateException();

                            }
                        }

                    };
                }

            }).create();

    public static <T> T create(Class<T> type) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(),
                new Class<?>[]{type, DbObject.class},
                new DbMapInvocationHandler(type, new HashMap<>()));
    }

    public static <T> Optional<T> of(Class<T> type, Map<String, Object> src) {
        if (src == null) {
            return Optional.empty();
        } else {
            return Optional.of((T) Proxy.newProxyInstance(type.getClassLoader(),
                    new Class<?>[]{type, DbObject.class},
                    new DbMapInvocationHandler(type, src)));
        }
    }

    public static <T> Optional<T> of(Class<T> type, String jsonString) {
        if (jsonString == null) {
            return Optional.empty();
        } else {
            return Optional.of(gson.fromJson(jsonString, type));
        }
    }


    private static class DbMapInvocationHandler implements InvocationHandler {

        private final Class type;

        private final Map<String, Object> src;

        public DbMapInvocationHandler(Class type, Map<String, Object> src) {
            this.type = type;
            this.src = src;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "getMap":
                    return src;
                case "toJson":
                    return gson.toJson(src);
                case "toString":
                    return getString();

                default:
                    return getOrSetValue(proxy, method, args);
            }
        }

        private Object getOrSetValue(Object proxy, Method method, Object[] args) {
            String mName = method.getName();

            if (mName.startsWith("get")) {
                return src.get(uncapFieldName(mName, 3));
            }
            if (mName.startsWith("with") && args.length == 1) {
                src.put(uncapFieldName(mName, 4), args[0]);
                return proxy;
            }
            if (mName.startsWith("is")) {
                return src.get(uncapFieldName(mName, 2));
            }
            return null;
        }

        private String getString() {
            StringBuilder response = new StringBuilder(type.getSimpleName() + "{");
            boolean notFirst = false;
            for (Method m : type.getMethods()) {
                if (m.getName().startsWith("get")) {
                    if (notFirst) {
                        response.append(", ");
                    }
                    String fieldName = Character.toLowerCase(m.getName().charAt(3)) + m.getName().substring(4);
                    response.append(fieldName).append("=").append(getOrSetValue(null, m, null));
                    notFirst = true;
                }
            }
            response.append("}");
            return response.toString();
        }

    }

    private static String uncapFieldName(String methodName, int prefixLen) {
        String name = Character.toLowerCase(methodName.charAt(prefixLen)) + methodName.substring(prefixLen + 1);
        return name;
    }

}
