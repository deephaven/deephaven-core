package io.deephaven.demo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.io.Files;
import com.google.gson.JsonPrimitive;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Yaml;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.deephaven.demo.NameConstants.*;

/**
 * HelmGenerator:
 * <p>
 * <p> For a quick-cut at a working, but inefficient demo site,
 * <p> we'll simply generate a helm deployment with N (default 30) instances.
 * <p>
 * <p> This will be done using random, but stable names,
 * <p> so the cluster size can be increased by rerunning the generator,
 * <p> and all generated subdomains (routes to an individual's worker) will have stable names.
 * <p>
 */
public class HelmGenerator {

    private final File helmRoot, helmTarget;

    public HelmGenerator(final File helmRoot, final File helmTarget) {
        this.helmRoot = helmRoot;
        this.helmTarget = helmTarget;
        if (!helmRoot.exists()) {
            throw new IllegalArgumentException("System property " + PROP_HELM_ROOT + " points to non-existent directory: " + helmRoot);
        }
        File templates = getTemplates();
        if (!templates.isDirectory()) {
            throw new IllegalArgumentException("System property " + PROP_HELM_ROOT + " directory exists, but does not contain a templates directory: " + helmRoot);
        }

        if (!helmTarget.isDirectory()) {
            if (!helmTarget.mkdirs()) {
                throw new IllegalArgumentException("Unable to create directory backed by " + PROP_HELM_TARGET + ": " + helmTarget + "; check permissions and disk space.");
            }
        }
    }

    private File getTemplates() {
        return new File(helmRoot, "templates");
    }

    public void generate() throws IOException {
        // copy Chart.yaml, values.yaml and any yaml templates we don't want to modify.
        copyFile("Chart.yaml");
        copyFile("values.yaml");
        copyTemplate("_helpers.tpl");

        // in order to modify certain yaml files, we need to do a helm --dry-run, and capture output.
        // this will expand all properties, and create documents that we can parse as yaml WITHOUT go template language in it.
        Map<String, String> dryRun = helmDryRun();

        List<String> subdomains = getSubdomains();

        final File[] files = getTemplates().listFiles();
        if (files == null) {
            throw new IllegalArgumentException("Empty template directory " + getTemplates());
        }
        for (File file : files) {
            System.out.println("Processing " + file);
            switch (file.getName()) {
                case "envoy-service.yaml":
                    alterService(file, dryRun, subdomains);
                    break;
                case "ingress.yaml":
                    alterIngress(file, dryRun, subdomains);
                    break;
                case "worker-deployment.yaml":
                    alterDeployment(file, dryRun, subdomains);
                    break;
                // junk files we should probably delete, after scraping for useful notes / code
                case "cert-wildcard-job.yaml":
                case "cert-wildcard-secret.yaml":
                case "hpa.yaml":
                    continue;
                default:
                    copyTemplate(file.getName());
                    break;
            }
            System.out.println("Done processing " + file);
        }

        System.out.println("Done generating helm chart into file://" + helmTarget);

    }

    private Map<String, String> helmDryRun() throws IOException {
        final Map<String, String> resolvedFiles = new HashMap<>();
        File upgradeOutput = new File(helmTarget, "helm.resolved");
        ProcessBuilder p = new ProcessBuilder("helm", "upgrade", "dh-demo", helmRoot + File.separator, "--dry-run");
        p.directory(helmRoot);
        p.redirectError(ProcessBuilder.Redirect.PIPE);
        p.redirectOutput(upgradeOutput);
        System.out.println("Saving output of `helm upgrade dh-demo " + helmRoot + File.separator + "` to " + upgradeOutput);
        final Process proc = p.start();
        boolean failed = false;
        try {
            proc.waitFor(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while running helm upgrade; bailing");
            failed = true;
        }
        String failMsg = "Failed to run helm upgrade dh-demo " + helmRoot + File.separator + "; have you ran helm install dh-demo ./ from " + helmRoot + "?";
        if (!failed) {
            int code = proc.exitValue();
            if (code != 0) {
                failed = true;
                failMsg += " Exit code: " + code;
            }
        }

        if (failed) {
            throw new IllegalStateException(failMsg);
        }

        // yay, we finished.  Read the file. We'll process it by lines.
        int mode = 0, line = 0;
        final int modeBegin = 0, modeFileName = 1, modeFileBody = 2, modeDone = 3;
        String filePath = "";
        for (String readLine : Files.readLines(upgradeOutput, StandardCharsets.UTF_8)) {
            line++;
            if ("---".equals(readLine)) {
                mode = modeFileName;
                continue;
            }
            switch (mode) {
                case modeBegin:
                    // haven't seen a file yet, continue until we see ---
                    break;
                case modeFileName:
                    if (!readLine.startsWith("# Source: ")) {
                        throw new IllegalStateException("Invalid yaml filename line " + readLine + " on line " + line + " of " + upgradeOutput);
                    }
                    // Hm... check if windows hates this /
                    String[] names = readLine.split("templates/");
                    filePath = names[names.length - 1];
                    if (filePath.contains("# Source:")) {
                        throw new IllegalStateException("Invalid filepath " + filePath + " found on line " + line + " of " + upgradeOutput);
                    }
                    mode = modeFileBody;
                    break;
                case modeFileBody:
                    if (readLine.startsWith("NOTES:")) {
                        mode = modeDone;
                        break;
                    }
                    if (filePath.isEmpty()) {
                        throw new IllegalStateException("Got to file body mode with invalid filename " + filePath + " (on line " + line + " of " + upgradeOutput + ")");
                    }
                    resolvedFiles.merge(filePath, readLine, (a, b) -> a + "\n" + b);
                    break;
                case modeDone:
                    break;
                default:
                    throw new IllegalStateException("Illegal mode " + mode + " (found when on line " + line +" of " + upgradeOutput + ")");
            }

        }

        return resolvedFiles;
    }

    private void alterDeployment(final File file, final Map<String, String> dryRun, final List<String> subdomains) throws IOException {
        // Alter the deployment to have N (default 30) replicas defined.
        // These replicas will be assigned routing labels by the controller once it boots up.
        String resolvedYaml = dryRun.get(file.getName());
        final V1Deployment deployment = Yaml.loadAs(resolvedYaml, V1Deployment.class);

        // we are just going to create 30 replicas with labels already correct,
        // so we don't have to rely on controller to boot up and fix labels.
        // In the future, simply controlling the deployment size should suffice.
        deployment.getSpec().setReplicas(1);

        String sep = "";
        final File outputFile = new File(helmTarget, "templates/" + file.getName());
        try (
                final Writer output = new FileWriter(outputFile);
        ) {
            for (String subdomain : subdomains) {
                deployment.getMetadata().setName("wrk-" + subdomain);
                deployment.getSpec().getTemplate().getMetadata().getLabels().put(LABEL_USER, subdomain);
                deployment.getSpec().getTemplate().getMetadata().getLabels().put(LABEL_PURPOSE, PURPOSE_WORKER);
                deployment.getSpec().getSelector().putMatchLabelsItem(LABEL_USER, subdomain);
                deployment.getSpec().getSelector().putMatchLabelsItem(LABEL_PURPOSE, PURPOSE_WORKER);
                String yaml = asYaml(deployment);
                // stupid yaml marshalling gets weird escaping sometimes...
                yaml = yaml.replaceAll("[\\\\]\\s+", " ");
                if (sep.isEmpty()) {
                    sep = "\n---\n";
                } else {
                    output.write(sep);
                }
                output.write(yaml);
            }
        }
    }

    private void alterIngress(final File file, final Map<String, String> dryRun, final List<String> subdomains) throws IOException {
        // Alter the ingress rule to have N (default 30) ingress routes defined.
        String resolvedYaml = dryRun.get(file.getName());
        final V1Ingress ingress = Yaml.loadAs(resolvedYaml, V1Ingress.class);
        final List<V1IngressRule> rules = ingress.getSpec().getRules();
        final List<V1IngressRule> newRules = new ArrayList<>();

        for (V1IngressRule rule : rules) {
            if (rule.getHost().startsWith("default.")) {
                // the default.domain.name rule! Expand it to N rules matching the supplied domains
                newRules.addAll(expandDomains(rule, subdomains));
            } else {
                // a non-default rule, just copy it
                newRules.add(rule);
            }
        }
        ingress.getSpec().setRules(newRules);

        String yaml = asYaml(ingress);
        final File outputFile = new File(helmTarget, "templates/" + file.getName());
        FileUtils.write(outputFile, yaml, StandardCharsets.UTF_8);

    }

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory()
                    .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
    );
    static {
        YAML_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        YAML_MAPPER.getSerializationConfig().introspectClassAnnotations(IntOrString.class);
        final JsonSerializer<? super IntOrString> serialIntStr = new JsonSerializer<>() {
            @Override
            public void serialize(final IntOrString src, final JsonGenerator gen, final SerializerProvider serializers) throws IOException {
                if (src.isInteger()) {
                    gen.writeNumber(src.getIntValue());
                } else {
                    gen.writeString(src.getStrValue());
                }
            }
        };
        final JsonSerializer<? super Quantity> serialQuantity = new JsonSerializer<>() {
            @Override
            public void serialize(final Quantity src, final JsonGenerator gen, final SerializerProvider serializers) throws IOException {
                gen.writeString(src.toSuffixedString());
            }
        };
        SimpleModule intStrModule = new SimpleModule(
                "intStringMod", new Version(1, 0, 0, null, null, null))
                .addSerializer(IntOrString.class, serialIntStr)
                .addSerializer(Quantity.class, serialQuantity);

        YAML_MAPPER.registerModule(intStrModule);
    }

    private static String asYaml(Object o) {

        try {
            return new String(YAML_MAPPER.writeValueAsBytes(o));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to process object into yaml: " + o, e);
        }
    }

    private List<V1IngressRule> expandDomains(final V1IngressRule rule, final List<String> subdomains) {
        final List<V1IngressRule> results = new ArrayList<>();

        String ruleYaml = asYaml(rule.getHttp());
        for (String subdomain : subdomains) {
            final V1IngressRule newRule = new V1IngressRule();
            newRule.setHost(rule.getHost().replace("default.", subdomain + "."));
            // just copy the interesting glue
            final V1HTTPIngressRuleValue newHttp = Yaml.loadAs(ruleYaml, V1HTTPIngressRuleValue.class) ;
            newRule.setHttp(newHttp);

            for (V1HTTPIngressPath path : newHttp.getPaths()) {
                path.getBackend().getService().setName("svc-" + subdomain);
            }

            results.add(newRule);
        }

        return results;
    }

    private void alterService(final File file, final Map<String, String> dryRun, final List<String> subdomains) throws IOException {
        // We are going to create N (default 30) services in one yaml file.
        // We can write all these services to one file, using yaml file separator: ---
        String resolvedYaml = dryRun.get(file.getName());

        final V1Service service = Yaml.loadAs(resolvedYaml, V1Service.class);

        final File outputFile = new File(helmTarget, "templates/" + file.getName());
        try (
            final Writer output = new FileWriter(outputFile);
        ) {
            // set the labels we want, and reserialize each subdomain
            String sep = "";
            for (String subdomain : subdomains) {
                service.getMetadata().setName("svc-" + subdomain);
                service.getMetadata().getLabels().put(LABEL_USER, subdomain);
                service.getMetadata().getLabels().put(LABEL_PURPOSE, PURPOSE_WORKER);
                service.getSpec().getSelector().put(LABEL_USER, subdomain);
                String yaml = asYaml(service);
                // stupid yaml marshalling gets weird escaping sometimes...
                yaml = yaml.replaceAll("[\\\\]\\s+", " ");
                if (sep.isEmpty()) {
                    sep = "\n---\n";
                } else {
                    output.write(sep);
                }
                output.write(yaml);
            }
        }
    }


    private List<String> getSubdomains() {

        final List<String> list = new ArrayList<>();
        // allow user to manually specify the subdomains they want
        if (!DH_HELM_SUBDOMAINS.isEmpty()) {
            return Arrays.asList(DH_HELM_SUBDOMAINS.split(","));
        }

        long seed = 0xcafebabe;
        Random rnd = new Random(seed);
        for (int numInstances = Integer.parseInt(DH_HELM_INSTANCES);
             numInstances --> 0;) {
            String newName = NameGen.newName(
                    rnd.nextInt(NameGen.ADJECTIVES.length), rnd.nextInt(NameGen.NOUNS.length)
            );
            list.add(newName);
        }

        return list;
    }

    private void copyTemplate(final String file) throws IOException {
        copyFile("templates" + File.separator + file);
    }
    private void copyFile(final String file) throws IOException {
        final File source = new File(helmRoot, file);
        final File target = new File(helmTarget, file);
        FileUtils.copyFile(source, target);
    }
}
