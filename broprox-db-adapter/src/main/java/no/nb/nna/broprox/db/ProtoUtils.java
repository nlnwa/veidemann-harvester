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

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import static no.nb.nna.broprox.db.RethinkDbAdapter.r;

/**
 * Static methods for converting between Protobuf messages and RethinkDB objects.
 */
public class ProtoUtils {

    /**
     * Utility class should not be instanciated.
     */
    private ProtoUtils() {
    }

    /**
     * Convert from ProtoBuf Message to Map suitable for RethinkDb.
     *
     * @param msg the ProtoBuf Message
     * @return a map suitable for RethinkDb
     */
    public static Map<String, Object> protoToRethink(MessageOrBuilder msg) {
        Map rMap = r.hashMap();
        msg.getAllFields().forEach((f, v) -> {
            if (f.isRepeated()) {
                List l = r.array();
                ((List) v).forEach((value) -> {
                    if (f.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                        l.add(protoToRethink((MessageOrBuilder) value));
                    } else {
                        l.add(value);
                    }
                });
                rMap.put(f.getJsonName(), l);
            } else {
                switch (f.getType()) {
                    case MESSAGE:
                        switch (f.getMessageType().getFullName()) {
                            case "google.protobuf.Timestamp":
                                rMap.put(f.getJsonName(), tsToOdt((Timestamp) v));
                                break;
                            default:
                                rMap.put(f.getJavaType(), protoToRethink((MessageOrBuilder) v));
                                break;
                        }
                        break;
                    default:
                        rMap.put(f.getJsonName(), v);
                        break;
                }
            }
        });
        return rMap;
    }

    /**
     * Convert a Map response from RethinkDb to a ProtoBuf Message.
     *
     * @param <T> The ProtoBuf message type
     * @param msg a Map from a RethinkDb response
     * @param type The Class of the ProtoBuf message type
     * @return the generated ProtoBuf Message
     */
    public static <T extends Message> T rethinkToProto(Map<String, Object> msg, Class<T> type) {
        try {
            Message.Builder protoBuilder = (Message.Builder) type.getMethod("newBuilder").invoke(null);
            return rethinkToProto(msg, protoBuilder);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Convert a Map response from RethinkDb to a ProtoBuf Message.
     *
     * @param <T> The ProtoBuf message type
     * @param msg a Map from a RethinkDb response
     * @param protoBuilder a builder for the ProtoBuf Message type
     * @return the generated ProtoBuf Message
     */
    public static <T extends Message> T rethinkToProto(Map<String, Object> msg, Message.Builder protoBuilder) {
        protoBuilder.getDescriptorForType().getFields().forEach(fd -> {
            Object value = msg.get(fd.getJsonName());
            if (value != null) {
                if (fd.isRepeated()) {
                    ((List) value).forEach((v) -> {
                        if (fd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                            protoBuilder.addRepeatedField(fd, rethinkToProto((Map<String, Object>) v, protoBuilder
                                    .newBuilderForField(fd)));
                        } else {
                            protoBuilder.addRepeatedField(fd, v);
                        }
                    });
                } else {
                    switch (fd.getType()) {
                        case MESSAGE:
                            switch (fd.getMessageType().getFullName()) {
                                case "google.protobuf.Timestamp":
                                    protoBuilder.setField(fd, odtToTs((OffsetDateTime) value));
                                    break;
                                default:
                                    protoBuilder.setField(fd, rethinkToProto((Map<String, Object>) value, protoBuilder
                                            .newBuilderForField(fd)));
                                    break;
                            }
                            break;
                        default:
                            protoBuilder.setField(fd, value);
                            break;
                    }
                }
            }
        });
        return (T) protoBuilder.build();
    }

    /**
     * Convert ProtoBuf {@link Timestamp} to {@link OffsetDateTime} used by RethinkDb.
     *
     * @param timestamp the Timestamp to convert
     * @return an OffsetDateTime representing the same time instant as the Timestamp
     */
    public static OffsetDateTime tsToOdt(Timestamp timestamp) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(Timestamps.toMillis(timestamp)), ZoneOffset.UTC);
    }

    /**
     * Convert {@link OffsetDateTime} used by RethinkDb to ProtoBuf {@link Timestamp}.
     *
     * @param timestamp the OffsetDateTime to convert
     * @return a Timestamp representing the same time instant as the OffsetDateTime
     */
    public static Timestamp odtToTs(OffsetDateTime timestamp) {
        return Timestamps.fromMillis(timestamp.toInstant().toEpochMilli());
    }

}
