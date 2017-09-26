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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.nb.nna.broprox.db.RethinkDbAdapter.r;

/**
 * Static methods for converting between Protobuf messages and RethinkDB objects.
 */
public class ProtoUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoUtils.class);

    private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser();

    private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

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
    public static Map protoToRethink(MessageOrBuilder msg) {
        Objects.requireNonNull(msg, "The msg cannot be null");

        Map rMap = r.hashMap();
        msg.getAllFields().forEach((f, v) -> {
            if (f.isRepeated()) {
                List l = r.array();
                ((List) v).forEach((entry) -> {
                    if (f.isMapField()) {
                        Object mapKey = ((MapEntry) entry).getKey();
                        Object mapValue = ((MapEntry) entry).getValue();

                        if (f.getMessageType().findFieldByName("value").getType()
                                == Descriptors.FieldDescriptor.Type.MESSAGE) {
                            mapValue = protoToRethink((MessageOrBuilder) mapValue);
                        }

                        Map map = r.hashMap(mapKey, mapValue);
                        l.add(map);
                    } else if (f.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                        l.add(protoToRethink((MessageOrBuilder) entry));
                    } else {
                        l.add(entry);
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
                                rMap.put(f.getJsonName(), protoToRethink((MessageOrBuilder) v));
                                break;
                        }
                        break;
                    case ENUM:
                        rMap.put(f.getJsonName(), v.toString());
                        break;
                    case BYTES:
                        v = r.binary(((ByteString) v).toByteArray());
                        rMap.put(f.getJsonName(), v);
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
    public static <T extends Message> T rethinkToProto(Map msg, Class<T> type) {
        try {
            Message.Builder protoBuilder = (Message.Builder) type.getMethod("newBuilder").invoke(null);
            return rethinkToProto(msg, protoBuilder);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
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
    public static <T extends Message> T rethinkToProto(Map msg, Message.Builder protoBuilder) {
        Objects.requireNonNull(msg, "The msg cannot be null");

        protoBuilder.getDescriptorForType().getFields().forEach(fd -> {
            Object value = msg.get(fd.getJsonName());
            if (value != null) {
                if (fd.isRepeated()) {
                    ((List) value).forEach((v) -> {
                        if (fd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                            Map valueMap = (Map) v;

                            if (fd.isMapField()) {
                                Object key = valueMap.keySet().iterator().next();
                                valueMap = ImmutableMap.of("key", key, "value", valueMap.get(key));
                            }

                            protoBuilder.addRepeatedField(fd, rethinkToProto(valueMap, protoBuilder
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
                                    protoBuilder.setField(fd, rethinkToProto((Map) value, protoBuilder
                                            .newBuilderForField(fd)));
                                    break;
                            }
                            break;
                        case ENUM:
                            protoBuilder.setField(fd, fd.getEnumType().findValueByName((String) value));
                            break;
                        case INT32:
                            if (value instanceof Long) {
                                protoBuilder.setField(fd, ((Long) value).intValue());
                            } else {
                                protoBuilder.setField(fd, (int) value);
                            }
                            break;
                        case FLOAT:
                            if (value instanceof Double) {
                                protoBuilder.setField(fd, ((Double) value).floatValue());
                            } else if (value instanceof Long) {
                                protoBuilder.setField(fd, ((Long) value).floatValue());
                            } else if (value instanceof Integer) {
                                protoBuilder.setField(fd, ((Integer) value).floatValue());
                            } else {
                                protoBuilder.setField(fd, (float) value);
                            }
                            break;
                        case BYTES:
                            protoBuilder.setField(fd, ByteString.copyFrom((byte[]) value));
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

    public static String protoToJson(MessageOrBuilder msg) {
        try {
            return JSON_PRINTER.print(msg);
        } catch (InvalidProtocolBufferException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String protoListToJson(List<? extends MessageOrBuilder> msgs) {
        return msgs.stream()
                .map(msg -> protoToJson(msg))
                .collect(Collectors.joining(",", "[", "]"));
    }

    public static <T extends Message> T jsonToProto(String msg, Class<T> type) {
        try {
            T.Builder protoBuilder = (T.Builder) type.getMethod("newBuilder").invoke(null);
            try {
                JSON_PARSER.merge(msg, protoBuilder);
            } catch (InvalidProtocolBufferException ex) {
                throw new IllegalArgumentException("Could not construct '" + type + "' from json: '" + msg + "'", ex);
            }
            return (T) protoBuilder.build();
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static <T extends Message> List<T> jsonListToProto(String msg, Class<T> type) {
        if (msg.startsWith("[") && msg.endsWith("]")) {
            String[] msgs = msg.substring(1, msg.length() - 1).split(",");
            try {
                return Arrays.stream(msgs)
                        .map(s -> jsonToProto(s, type))
                        .collect(Collectors.toList());
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Input is not an JSON array: '" + msg + "'", ex);
            }
        } else {
            throw new IllegalArgumentException("Input is not an JSON array: '" + msg + "'");
        }
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

    public static Timestamp getNowTs() {
        return Timestamps.fromMillis(System.currentTimeMillis());
    }

    public static OffsetDateTime getNowOdt() {
        return OffsetDateTime.now(ZoneOffset.UTC);
    }

}
