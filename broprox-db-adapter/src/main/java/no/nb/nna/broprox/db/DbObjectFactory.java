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

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public final class DbObjectFactory {

    public static <T> T create(Class<T> type) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(),
                new Class<?>[]{type, DbBasics.class},
                new DbMapInvocationHandler(type, new HashMap<>()));
    }

    public static <T> Optional<T> of(Class<T> type, Map<String, Object> src) {
        if (src == null) {
            return Optional.empty();
        } else {
            return Optional.of((T) Proxy.newProxyInstance(type.getClassLoader(),
                    new Class<?>[]{type, DbBasics.class},
                    new DbMapInvocationHandler(type, src)));
        }
    }

    public interface DbBasics {

        Map<String, Object> getMap();

        Object getKey();

    }

    private static class DbMapInvocationHandler implements InvocationHandler {

        private final Class type;

        private final Map<String, Object> src;

        private final String keyField;

        public DbMapInvocationHandler(Class type, Map<String, Object> src) {
            this.type = type;
            this.src = src;
            Annotation ann = type.getAnnotation(Key.class);
            if (ann != null) {
                keyField = ((Key) ann).value();
            } else {
                keyField = null;
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "getMap":
                    return src;
                case "getKey":
                    return src.get("id");
                case "toString":
                    return getString();

                default:
                    return getOrSetValue(proxy, method, args);
            }
        }

        private Object getOrSetValue(Object proxy, Method method, Object[] args) {
            String mName = method.getName();

            if (mName.startsWith("get")) {
                return src.get(extractFieldName(mName, 3));
            }
            if (mName.startsWith("with") && args.length == 1) {
                src.put(extractFieldName(mName, 4), args[0]);
                return proxy;
            }
            if (mName.startsWith("is")) {
                return src.get(extractFieldName(mName, 2));
            }
            return null;
        }

        private String extractFieldName(String methodName, int prefixLen) {
            String name = Character.toLowerCase(methodName.charAt(prefixLen)) + methodName.substring(prefixLen + 1);
            if (keyField != null && keyField.equals(name)) {
                name = "id";
            }
            return name;
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
}
