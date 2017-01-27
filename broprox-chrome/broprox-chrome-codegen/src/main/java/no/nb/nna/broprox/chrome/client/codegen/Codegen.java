package no.nb.nna.broprox.chrome.client.codegen;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import static javax.lang.model.element.Modifier.*;

public class Codegen {

    static Gson gson = new Gson();

    static String PACKAGE = "no.nb.nna.broprox.chrome.client";

    static ClassName CLIENT_CLASS = ClassName.get(PACKAGE + ".ws", "Cdp");

    static String CHROME_VERSION = "56.0.2924.76";

    public static void main(String args[]) throws IOException {
        String browserProtocol = "https://chromium.googlesource.com/chromium/src/+/"
                + CHROME_VERSION + "/third_party/WebKit/Source/core/inspector/browser_protocol.json?format=text";

        String jsProtocol = "https://chromium.googlesource.com/v8/v8/+/chromium/"
                + CHROME_VERSION.split("\\.")[2] + "/src/inspector/js_protocol.json?format=text";

        Protocol protocol = loadProtocol(browserProtocol);
        protocol.merge(loadProtocol(jsProtocol));

        File outdir = args.length > 0 ? new File(args[0]) : null;
        protocol.gencode(outdir);
    }

    static Protocol loadProtocol(String url) throws IOException {
        try (InputStream stream = Base64.getDecoder().wrap(new URL(url).openStream());
                InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            return gson.fromJson(reader, Protocol.class);
        }
    }

    static ClassName buildStruct(TypeSpec.Builder b, String name, String description, List<Parameter> members, Protocol protocol, Domain domain) {
        String typeName = name.substring(0, 1).toUpperCase() + name.substring(1);
        TypeSpec.Builder typeSpec = TypeSpec.classBuilder(typeName)
                .addModifiers(PUBLIC, STATIC);

        StringBuilder fieldStrings = new StringBuilder();

        if (description != null) {
            typeSpec.addJavadoc(description.replace("$", "$$") + "\n");
        }

        for (Parameter member : members) {
            if (Objects.equals(member.name, "this")) {
                member.name = "this_";
            }
            FieldSpec.Builder field = FieldSpec.builder(member.typeName(protocol, domain), member.name, PUBLIC);
            if (member.name.equals("this_")) {
                field.addAnnotation(AnnotationSpec.builder(SerializedName.class)
                        .addMember("value", "$S", "this").build());
            }
            if (member.description != null) {
                field.addJavadoc(member.description.replace("$", "$$") + "\n");
            }
            typeSpec.addField(field.build());

            if (fieldStrings.length() > 0) {
                fieldStrings.append(", ");
            }
            fieldStrings.append(member.name + "=\" + " + member.name + " + \"");
        }

        typeSpec.addMethod(MethodSpec.methodBuilder("toString")
                .addModifiers(PUBLIC)
                .returns(String.class)
                .addStatement("return \"" + typeName + "{" + fieldStrings + "}\"").build());

        TypeSpec spec = typeSpec.build();
        b.addType(typeSpec.build());
        return ClassName.get(PACKAGE, domain.javaName, typeName);
    }

    public static String coalesce(String... strs) {
        for (String s : strs) {
            if (s != null) {
                return s;
            }
        }
        return null;
    }

    static String cap(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

}
