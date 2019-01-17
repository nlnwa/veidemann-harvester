/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.model.MapObject;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigObjectOrBuilder;
import no.nb.nna.veidemann.api.config.v1.FieldMask;
import no.nb.nna.veidemann.api.config.v1.Kind;
import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.rethinkdb.RethinkDB.r;

public class FieldMasks {
    private static Map<String, String> SORTABLES = new HashMap<>();
    protected static FieldMasks CONFIG_OBJECT_DEF = createForConfigObject();

    static {
        SORTABLES.put("meta.name", "name");
        SORTABLES.put("meta.label", "label");
        SORTABLES.put("meta.lastModified", "lastModified");
        SORTABLES.put("meta.lastModifiedBy", "lastModifiedBy");
    }

    private PathElem masks = new PathElem("", "");
    private Map<String, PathElem> paths = new HashMap<>();

    private FieldMasks() {
    }

    private static FieldMasks createForConfigObject() {
        FieldMasks fm = new FieldMasks();
        fm.createPaths();
        return fm;
    }

    public static FieldMasks createForFieldMaskProto(FieldMask mask) {
        FieldMasks result = new FieldMasks();
        for (String p : mask.getPathsList()) {
            parseMask(p, result);
        }
        return result;
    }

    private static void parseMask(String mask, FieldMasks result) {
        UpdateType updateType = UpdateType.REPLACE;
        if (mask.endsWith("+")) {
            mask = mask.substring(0, mask.length() - 1);
            updateType = UpdateType.APPEND;
        } else if (mask.endsWith("-")) {
            mask = mask.substring(0, mask.length() - 1);
            updateType = UpdateType.DELETE;
        }

        if (!CONFIG_OBJECT_DEF.paths.containsKey(mask)) {
            throw new IllegalArgumentException("Illegal fieldmask path: " + mask);
        }

        String tokens[] = mask.split("\\.");
        String fullName = "";
        PathElem e = result.masks;
        for (String token : tokens) {
            fullName += token;
            e = e.getOrCreateChild(fullName, token);
            e.descriptor = getDescriptorForPath(fullName);
            fullName += ".";
        }
        if (e != null) {
            e.isTarget = true;
            if (e.descriptor.isRepeated()) {
                e.updateType = updateType;
            }
            result.paths.put(e.fullName, e);
        }
    }

    public static FieldMasks createForSingleMask(String mask) {
        FieldMasks result = new FieldMasks();
        parseMask(mask, result);
        return result;
    }

    public static Object getValue(String path, ConfigObjectOrBuilder object) {
        return getValue(getElementForPath(path), object);
    }

    public static Object getValue(PathElem path, ConfigObjectOrBuilder object) {
        return innerGetValue(path, object);
    }

    private static Object innerGetValue(PathElem p, MessageOrBuilder object) {
        if (!p.parent.name.isEmpty()) {
            object = (MessageOrBuilder) innerGetValue(p.parent, object);
        }
        return object.getField(p.descriptor);
    }

    public static ConfigObjectOrBuilder setValue(String path, ConfigObjectOrBuilder object, Object value) {
        return setValue(getElementForPath(path), object, value);
    }

    public static ConfigObjectOrBuilder setValue(PathElem path, ConfigObjectOrBuilder object, Object value) {
        if (object instanceof Builder) {
            innerSetValue(path, path, (Builder) object, value);
            return object;
        } else {
            ConfigObject.Builder b = ((ConfigObject) object).toBuilder();
            innerSetValue(path, path, b, value);
            return b.build();
        }
    }

    private static MessageOrBuilder innerSetValue(PathElem destination, PathElem p, Builder object, Object value) {
        if (!p.parent.name.isEmpty()) {
            object = (Builder) innerSetValue(destination, p.parent, object, value);
        }

        if (p == destination) {
            if (p.descriptor.isRepeated()) {
                return object.addRepeatedField(p.descriptor, value);
            } else {
                return object.setField(p.descriptor, value);
            }
        } else {
            return object.getFieldBuilder(p.descriptor);
        }
    }

    public static FieldDescriptor getDescriptorForPath(String path) {
        return getElementForPath(path).descriptor;
    }

    public static PathElem getElementForPath(String path) {
        PathElem p = CONFIG_OBJECT_DEF.paths.get(path);
        if (p == null) {
            throw new IllegalArgumentException("Illegal path: " + path);
        }
        return p;
    }

    public static String getSortIndexForPath(String path) {
        return SORTABLES.get(path);
    }

    public List createPluckQuery() {
        List p = r.array();
        p.add("apiVersion");
        p.add("kind");
        p.add("id");
        masks.children.forEach(e -> {
            innerCreatePluckQuery(p, e);
        });
        return p;
    }

    private void innerCreatePluckQuery(List p, PathElem e) {
        if (e.isTarget) {
            p.add(e.name);
        } else {
            List cp = r.array();
            p.add(r.hashMap(e.name, cp));
            e.children.forEach(c -> innerCreatePluckQuery(cp, c));
        }
    }

    public ReqlFunction1 buildFilterQuery(ConfigObject queryTemplate) {
        return row -> {
            ReqlExpr e = row;
            boolean first = true;
            for (PathElem p : paths.values()) {
                if (first) {
                    e = innerBuildFilterQuery(row, p, queryTemplate);
                } else {
                    e = e.and(innerBuildFilterQuery(row, p, queryTemplate));
                }
                first = false;
            }
            return e;
        };
    }

    private ReqlExpr innerBuildFilterQuery(ReqlExpr exp, PathElem p, ConfigObject queryTemplate) {
        exp = buildGetFieldExpression(p, exp);
        Object val = FieldMasks.getValue(p, queryTemplate);

        if (FieldMasks.getDescriptorForPath(p.fullName).isRepeated()) {
            List values = r.array();
            for (Object v : (List) ProtoUtils.protoFieldToRethink(p.descriptor, val)) {
                values.add(v);
            }
            exp = exp.contains(r.args(values));
        } else {
            exp = exp.eq(ProtoUtils.protoFieldToRethink(p.descriptor, val));
        }
        return exp;
    }

    private ReqlExpr buildGetFieldExpression(PathElem p, ReqlExpr parentExpr) {
        if (!p.parent.name.isEmpty()) {
            parentExpr = buildGetFieldExpression(p.parent, parentExpr);
        }
        parentExpr = parentExpr.g(p.name);
        return parentExpr;
    }

    public ReqlFunction1 buildUpdateQuery(Kind kind, ConfigObject object) {
        return row -> {
            MapObject p = r.hashMap();
            for (PathElem e : masks.children) {
                innerBuildUpdateQuery(row, p, e, kind, object);
            }
            return p;
        };
    }

    private void innerBuildUpdateQuery(ReqlExpr row, Map p, PathElem e, Kind kind, ConfigObject object) {
        switch (e.fullName) {
            case "id":
            case "apiVersion":
            case "kind":
            case "meta.created":
            case "meta.createdBy":
                return;
        }

        if (e.descriptor.getType() == Type.MESSAGE) {
            if (e.descriptor.isRepeated()) {
                PathElem e2 = paths.get(e.fullName);
                if (e2 == null) {
                    p.put(e.name, ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object)));
                } else if (e2.updateType == UpdateType.REPLACE) {
                    p.put(e.name, ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object)));
                } else if (e2.updateType == UpdateType.APPEND) {
                    p.put(e.name, buildGetFieldExpression(e2, row).default_(r.array())
                            .setUnion(ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object))));
                } else {
                    p.put(e.name, buildGetFieldExpression(e2, row).default_(r.array())
                            .setDifference(ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object))));
                }
            } else if (e.descriptor.getMessageType() == Timestamp.getDescriptor()) {
                p.put(e.name, ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object)));
            } else {
                Map cp = r.hashMap();
                p.put(e.name, cp);
                if (paths.containsKey(e.fullName)) {
                    e = FieldMasks.getElementForPath(e.fullName);
                }
                e.children.forEach(c -> innerBuildUpdateQuery(row, cp, c, kind, object));
            }
        } else {
            p.put(e.name, ProtoUtils.protoFieldToRethink(e.descriptor, e.getValue(object)));
        }
    }

    void createPaths() {
        for (FieldDescriptor d : ConfigObject.getDescriptor().getFields()) {
            createFieldPath(d, masks, "");
        }
    }

    void createFieldPath(FieldDescriptor field, PathElem parent, String prefix) {
        if (field.getType() == Type.MESSAGE && field.getMessageType().getFields().isEmpty()) {
            return;
        }

        String fullName = parent.fullName;
        if (!fullName.isEmpty()) {
            fullName += ".";
        }
        fullName += field.getJsonName();

        PathElem e = parent.getOrCreateChild(fullName, field.getJsonName());
        e.descriptor = field;

        paths.put(e.fullName, e);
        if (field.isRepeated()) {
            e.isRepeated = true;
        } else if (field.getType() == Type.MESSAGE) {
            if (field.getMessageType().getFields().isEmpty()) {
                return;
            }
            if (field.getMessageType() == Timestamp.getDescriptor()) {
                return;
            }
            for (FieldDescriptor m : field.getMessageType().getFields()) {
                createFieldPath(m, e, m.getJsonName());
            }
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("FieldMasks{\n");
        sb.append("  masks:\n");
        masks.children.forEach(e -> sb.append("    ").append(e.regex()).append("\n"));
        sb.append("  paths:\n");
        paths.forEach((k, v) -> sb.append("    ").append(k).append("\n"));
        sb.append('}');
        return sb.toString();
    }

    enum UpdateType {
        REPLACE,
        APPEND,
        DELETE
    }

    private class PathElem {
        private String name;
        private String fullName;
        private boolean isRepeated;
        private boolean isTarget;
        private PathElem parent;
        private final List<PathElem> children = new ArrayList<>();
        private FieldDescriptor descriptor;
        private UpdateType updateType;

        private PathElem(String fullName, String name) {
            this.fullName = fullName;
            this.name = name;
        }

        private void addChild(PathElem e) {
            e.parent = this;
            children.add(e);
        }

        private PathElem getOrCreateChild(String fullName, String name) {
            PathElem e = null;
            for (PathElem p : children) {
                if (p.name.equals(name)) {
                    e = p;
                    break;
                }
            }
            if (e == null) {
                e = new PathElem(fullName, name);
                addChild(e);
            }
            return e;
        }

        public Object getValue(ConfigObjectOrBuilder object) {
            return FieldMasks.innerGetValue(this, object);
        }

        public void setValue(ConfigObjectOrBuilder object, Object value) {
            FieldMasks.setValue(this, object, value);
        }

        @Override
        public String toString() {
            return name;
        }

        public String regex() {
            final StringBuffer sb = new StringBuffer(name);
            if (isRepeated) {
                sb.append("+");
            }
            if (!children.isEmpty()) {
                sb.append(".(");
                sb.append(Strings.join(children, '|'));
                sb.append(")");
            }
            return sb.toString();
        }
    }
}
