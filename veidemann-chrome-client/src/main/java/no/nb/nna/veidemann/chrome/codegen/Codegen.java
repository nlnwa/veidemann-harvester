package no.nb.nna.veidemann.chrome.codegen;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.annotations.SerializedName;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import no.nb.nna.veidemann.chrome.client.BrowserClientBase.CreateTargetReply;
import no.nb.nna.veidemann.chrome.client.ws.TargetInfo;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

public class Codegen {

    static final Pattern UNCAP_PATTERN = Pattern.compile("([A-Z]+)(.*)");

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
                + CHROME_VERSION + "/third_party/blink/renderer/core/inspector/browser_protocol.pdl?format=text";

        String jsProtocol = "https://chromium.googlesource.com/v8/v8/+/chromium/"
                + CHROME_VERSION.split("\\.")[2] + "/include/js_protocol.pdl?format=text";

        System.out.println("Using protocol definitions from:");
        System.out.println("   " + browserProtocol);
        System.out.println("   " + jsProtocol);

        Protocol protocol = loadProtocol(browserProtocol);
        protocol.merge(loadProtocol(jsProtocol));

        protocol.gencode(outdir);
    }

    static Protocol loadProtocol(String url) throws IOException {
        try (InputStream stream = Base64.getDecoder().wrap(new URL(url).openStream())) {
            Map proto = PdlParser.parse(stream, true);
            JsonElement json = gson.toJsonTree(proto);
            return gson.fromJson(json, Protocol.class);
        }
    }

    static ClassName buildStruct(TypeSpec.Builder b, String name, String description, List<Parameter> members, Protocol protocol, Domain domain) {
        String typeName = cap(name);
        TypeSpec.Builder typeSpec = TypeSpec.classBuilder(typeName)
                .addModifiers(PUBLIC, STATIC);

        if ("TargetInfo".equals(name)) {
            typeSpec.addSuperinterface(TargetInfo.class);
        }

        StringBuilder fieldStrings = new StringBuilder();

        if (description != null) {
            typeSpec.addJavadoc(description.replace("$", "$$") + "\n");
        }

        // Constructor
        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC);

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

            // Add accessor methods
            typeSpec.addMethod(MethodSpec.methodBuilder(uncap(member.name))
                    .addModifiers(PUBLIC)
                    .returns(fieldSpec.type)
                    .addStatement("return $N", fieldSpec)
                    .addJavadoc(member.description == null ? "" : member.description.replace("$", "$$") + "\n")
                    .build());

            if (member.optional) {
                // Add fluent withNNN methods for optional arguments
                typeSpec.addMethod(MethodSpec.methodBuilder("with" + cap(member.name))
                        .addModifiers(PUBLIC)
                        .returns(ClassName.get("", typeName))
                        .addParameter(fieldSpec.type, fieldSpec.name, Modifier.FINAL)
                        .addStatement("this.$N = $N", fieldSpec, fieldSpec)
                        .addStatement("return this")
                        .addJavadoc(member.description == null ? "" : member.description.replace("$", "$$") + "\n")
                        .build());
            } else {
                // Add required arguments to constructor
                constructor.addParameter(fieldSpec.type, fieldSpec.name, Modifier.FINAL)
                        .addStatement("this.$1N = $1N", fieldSpec);
            }
        }

        typeSpec.addMethod(constructor.build());

        typeSpec.addMethod(MethodSpec.methodBuilder("toString")
                .addModifiers(PUBLIC)
                .returns(String.class)
                .addStatement("return \"" + typeName + "{" + fieldStrings + "}\"").build());

        b.addType(typeSpec.build());
        return ClassName.get(PACKAGE, domain.javaName, typeName);
    }


    static ClassName buildImmutableResponse(TypeSpec.Builder b, String name, String description, List<Parameter> members, Protocol protocol, Domain domain) {
        String typeName = name.substring(0, 1).toUpperCase() + name.substring(1);
        TypeSpec.Builder typeSpec = TypeSpec.classBuilder(typeName)
                .addModifiers(PUBLIC, STATIC);

        if ("CreateTargetResponse".equals(typeName)) {
            typeSpec.addSuperinterface(CreateTargetReply.class);
        }

        // Private constructor to avoid instatiation
        typeSpec.addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE).build());

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

            typeSpec.addMethod(MethodSpec.methodBuilder(uncap(member.name))
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

    public static String uncap(String name) {
        Matcher m = UNCAP_PATTERN.matcher(name);
        if (m.matches()) {
            return m.group(1).toLowerCase() + m.group(2);
        } else {
            return name;
        }
    }

}
