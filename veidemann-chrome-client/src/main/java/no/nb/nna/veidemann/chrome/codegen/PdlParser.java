package no.nb.nna.veidemann.chrome.codegen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Parser for the format used by Google Chrome for describing the debug protocol.
 * Best documentation found is Googles parser implemented in Python:
 * https://cs.chromium.org/chromium/src/third_party/inspector_protocol/pdl.py
 */
public class PdlParser {
    private static final Logger LOG = LoggerFactory.getLogger(PdlParser.class);
    private static final List<String> primitiveTypes = Arrays.asList(new String[]{"integer", "number", "boolean", "string", "object", "any", "array"});

    public static Map<String, Object> parse(InputStream in) throws IOException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));

        boolean nukeDescription = false;
        String description = "";
        Map<String, Object> protocol = new HashMap<>();

        List<Map<String, List>> domains = new ArrayList<>();
        protocol.put("domains", domains);

        Map<String, String> version = new HashMap<>();
        protocol.put("version", version);

        Map<String, List> domain = new HashMap<>();
        List enumliterals = null;
        Map item = null;
        List subitems = null;

        List<String> lines = reader.lines().collect(Collectors.toList());
        for (int i = 0; i < lines.size(); i++) {
            if (nukeDescription) {
                description = "";
                nukeDescription = false;
            }
            String line = lines.get(i);
            String trimLine = line.trim();

            if (trimLine.startsWith("#")) {
                if (!description.isEmpty()) {
                    description += '\n';
                }
                description += trimLine.substring(1);
                continue;
            } else {
                nukeDescription = true;
            }

            if (trimLine.isEmpty()) {
                continue;
            }

            Matcher m;

            m = Pattern.compile("^(experimental )?(deprecated )?domain (.*)").matcher(line);
            if (m.matches()) {
                domain = createItem("domain", m.group(3), m.group(1), m.group(2), null, description);
                domains.add(domain);
                continue;
            }

            m = Pattern.compile("^  depends on ([^\\s]+)").matcher(line);
            if (m.matches()) {
                getOrCreateList(domain, "dependencies").add(m.group(1));
                continue;
            }

            m = Pattern.compile("^  (experimental )?(deprecated )?type (.*) extends (array of )?([^\\s]+)").matcher(line);
            if (m.matches()) {
                List types = getOrCreateList(domain, "types");
                item = createItem("id", m.group(3), m.group(1), m.group(2), null, description);
                assignType(item, m.group(5), m.group(4));
                types.add(item);
                continue;
            }

            m = Pattern.compile("^  (experimental )?(deprecated )?(command|event) (.*)").matcher(line);
            if (m.matches()) {
                List list;
                if ("command".equals(m.group(3))) {
                    list = getOrCreateList(domain, "commands");
                } else {
                    list = getOrCreateList(domain, "events");
                }

                item = createItem(m.group(1), m.group(2), m.group(4), description);
                list.add(item);
                continue;
            }

            m = Pattern.compile("^      (experimental )?(deprecated )?(optional )?(array of )?([^\\s]+) ([^\\s]+)").matcher(line);
            if (m.matches()) {
                Map param = createItem(m.group(1), m.group(2), m.group(6), description);
                if (m.group(3) != null) {
                    param.put("optional", true);
                }
                assignType(param, m.group(5), m.group(4));
                if ("enum".equals(m.group(5))) {
                    enumliterals = getOrCreateList(param, "enum");
                }
                subitems.add(param);
                continue;
            }

            m = Pattern.compile("^    (parameters|returns|properties)").matcher(line);
            if (m.matches()) {
                subitems = getOrCreateList(item, m.group(1));
                continue;
            }

            m = Pattern.compile("^    enum").matcher(line);
            if (m.matches()) {
                enumliterals = getOrCreateList(item, "enum");
                continue;
            }

            m = Pattern.compile("^version").matcher(line);
            if (m.matches()) {
                continue;
            }

            m = Pattern.compile("^  major (\\d+)").matcher(line);
            if (m.matches()) {
                version.put("major", m.group(1));
                continue;
            }

            m = Pattern.compile("^  minor (\\d+)").matcher(line);
            if (m.matches()) {
                version.put("minor", m.group(1));
                continue;
            }

            m = Pattern.compile("^    redirect ([^\\s]+)").matcher(line);
            if (m.matches()) {
                item.put("redirect", m.group(1));
                continue;
            }

            m = Pattern.compile("^      (  )?[^\\s]+$").matcher(line);
            if (m.matches()) {
                // enum literal
                enumliterals.add(trimLine);
                continue;
            }

            LOG.error("Error in {}:{}, illegal token: \t{}", "file_name", i, line);
            System.exit(1);
        }
        in.close();
        return protocol;
    }

    private static List getOrCreateList(Map<String, List> container, String name) {
        List l = container.get(name);
        if (l == null) {
            l = new ArrayList<>();
            container.put(name, l);
        }
        return l;
    }

    private static void assignType(Map item, String type, String isArray) {
        if (isArray != null) {
            item.put("type", "array");
            Map items = new HashMap();
            item.put("items", items);
            assignType(items, type, null);
            return;
        }
        if ("enum".equals(type)) {
            type = "string";
        }
        if (primitiveTypes.contains(type)) {
            item.put("type", type);
        } else {
            item.put("$ref", type);
        }
    }

    private static Map createItem(String experimental, String deprecated, String name, String description) {
        Map d = new HashMap();
        createItem(d, experimental, deprecated, name, description);
        return d;
    }

    private static Map createItem(String key, String val, String experimental, String deprecated, String name, String description) {
        Map d = new HashMap();
        d.put(key, val);
        createItem(d, experimental, deprecated, name, description);
        return d;
    }

    private static void createItem(Map d, String experimental, String deprecated, String name, String description) {
        if (name != null) {
            d.put("name", name);
        }
        if (description != null) {
            d.put("description", description.trim());
        }
        if (experimental != null) {
            d.put("experimental", true);
        }
        if (deprecated != null) {
            d.put("deprecated", true);
        }
    }

}
