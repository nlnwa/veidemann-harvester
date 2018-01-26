package no.nb.nna.veidemann.chrome.client.codegen;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import static javax.lang.model.element.Modifier.*;

public class Codegen {

    static Gson gson = new Gson();

    static String PACKAGE = "no.nb.nna.veidemann.chrome.client";

    static ClassName CLIENT_CLASS = ClassName.get(PACKAGE + ".ws", "Cdp");

    static String CHROME_VERSION;

    public static void main(String args[]) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing required arguments. Usage: Codegen <chrome_version> <generated_code_dir>");
        }

        CHROME_VERSION = args[0];
        File outdir = new File(args[1]);

        System.out.println("Generating client for Chrome version: " + CHROME_VERSION);
        System.out.println("Sources generated in: " + outdir);

        String browserProtocol = "https://chromium.googlesource.com/chromium/src/+/"
                + CHROME_VERSION + "/third_party/WebKit/Source/core/inspector/browser_protocol.json?format=text";

        String jsProtocol = "https://chromium.googlesource.com/v8/v8/+/chromium/"
                + CHROME_VERSION.split("\\.")[2] + "/src/inspector/js_protocol.json?format=text";

        Protocol protocol = loadProtocol(browserProtocol);
        protocol.merge(loadProtocol(jsProtocol));

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
            FieldSpec.Builder field = FieldSpec.builder(member.typeName(protocol, domain), member.name, PRIVATE);
            if (member.name.equals("this_")) {
                field.addAnnotation(AnnotationSpec.builder(SerializedName.class)
                        .addMember("value", "$S", "this").build());
            }
            if (member.description != null) {
                field.addJavadoc(member.description.replace("$", "$$") + "\n");
            }

            FieldSpec fieldSpec = field.build();
            typeSpec.addField(fieldSpec);

            if (fieldStrings.length() > 0) {
                fieldStrings.append(", ");
            }
            fieldStrings.append(member.name + "=\" + " + member.name + " + \"");

            typeSpec.addMethod(MethodSpec.methodBuilder("get" + cap(member.name))
                    .addModifiers(PUBLIC)
                    .returns(fieldSpec.type)
                    .addStatement("return $N", fieldSpec)
                    .addJavadoc(member.description == null ? "" : member.description.replace("$", "$$") + "\n")
                    .build());
            if (member.optional) {
                typeSpec.addMethod(MethodSpec.methodBuilder("set" + cap(member.name))
                        .addModifiers(PUBLIC)
                        .returns(TypeName.VOID)
                        .addParameter(fieldSpec.type, fieldSpec.name, Modifier.FINAL)
                        .addStatement("this.$N = $N", fieldSpec, fieldSpec)
                        .addJavadoc(member.description == null ? "" : member.description.replace("$", "$$") + "\n")
                        .build());
            }
        }

        typeSpec.addMethod(MethodSpec.methodBuilder("toString")
                .addModifiers(PUBLIC)
                .returns(String.class)
                .addStatement("return \"" + typeName + "{" + fieldStrings + "}\"").build());

        TypeSpec spec = typeSpec.build();
        b.addType(typeSpec.build());
        return ClassName.get(PACKAGE, domain.javaName, typeName);
    }


    static ClassName buildImmutableResponse(TypeSpec.Builder b, String name, String description, List<Parameter> members, Protocol protocol, Domain domain) {
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
            FieldSpec.Builder field = FieldSpec.builder(member.typeName(protocol, domain), member.name, PRIVATE);
            if (member.name.equals("this_")) {
                field.addAnnotation(AnnotationSpec.builder(SerializedName.class)
                        .addMember("value", "$S", "this").build());
            }
            if (member.description != null) {
                field.addJavadoc(member.description.replace("$", "$$") + "\n");
            }

            FieldSpec fieldSpec = field.build();
            typeSpec.addField(fieldSpec);

            if (fieldStrings.length() > 0) {
                fieldStrings.append(", ");
            }
            fieldStrings.append(member.name + "=\" + " + member.name + " + \"");

            typeSpec.addMethod(MethodSpec.methodBuilder("get" + cap(member.name))
                    .addModifiers(PUBLIC)
                    .returns(fieldSpec.type)
                    .addStatement("return $N", fieldSpec)
                    .addJavadoc(member.description == null ? "" : member.description.replace("$", "$$") + "\n")
                    .build());
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
