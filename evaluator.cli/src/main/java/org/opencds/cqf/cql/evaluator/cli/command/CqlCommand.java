package org.opencds.cqf.cql.evaluator.cli.command;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.io.FilenameUtils;
import org.cqframework.cql.cql2elm.CqlTranslatorOptions;
import org.cqframework.cql.cql2elm.CqlTranslatorOptionsMapper;
import org.cqframework.cql.cql2elm.LibrarySourceProvider;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseDatatype;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.opencds.cqf.cql.engine.execution.EvaluationResult;
import org.opencds.cqf.cql.engine.execution.ExpressionResult;
import org.opencds.cqf.cql.engine.runtime.Interval;
import org.opencds.cqf.cql.engine.terminology.TerminologyProvider;
import org.opencds.cqf.cql.evaluator.CqlEvaluator;
import org.opencds.cqf.cql.evaluator.CqlOptions;
import org.opencds.cqf.cql.evaluator.builder.Constants;
import org.opencds.cqf.cql.evaluator.builder.CqlEvaluatorBuilder;
import org.opencds.cqf.cql.evaluator.builder.DataProviderComponents;
import org.opencds.cqf.cql.evaluator.builder.DataProviderFactory;
import org.opencds.cqf.cql.evaluator.builder.EndpointInfo;
import org.opencds.cqf.cql.evaluator.dagger.CqlEvaluatorComponent;
import org.opencds.cqf.cql.evaluator.dagger.DaggerCqlEvaluatorComponent;
import org.json.JSONArray;
import org.json.JSONObject;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "cql", mixinStandardHelpOptions = true)
public class CqlCommand implements Callable<Integer> {
    @Option(names = { "-fv", "--fhir-version" }, required = true)
    public String fhirVersion;

    @Option(names = { "-op", "--options-path" })
    public String optionsPath;

    @ArgGroup(multiplicity = "1..*", exclusive = false)
    List<LibraryParameter> libraries;

    static class LibraryParameter {
        @Option(names = { "-lu", "--library-url" }, required = true)
        public String libraryUrl;

        @Option(names = { "-ln", "--library-name" }, required = true)
        public String libraryName;

        @Option(names = { "-lv", "--library-version" })
        public String libraryVersion;

        @Option(names = { "-t", "--terminology-url" })
        public String terminologyUrl;

        @ArgGroup(multiplicity = "0..1", exclusive = false)
        public ModelParameter model;

        @ArgGroup(multiplicity = "0..*", exclusive = false)
        public List<ParameterParameter> parameters;

        @Option(names = { "-e", "--expression" })
        public String[] expression;

        @ArgGroup(multiplicity = "0..1", exclusive = false)
        public ContextParameter context;

        static class ContextParameter {
            @Option(names = { "-c", "--context" })
            public String contextName;

            @Option(names = { "-cv", "--context-value" })
            public String contextValue;
        }

        static class ModelParameter {
            @Option(names = { "-m", "--model" })
            public String modelName;

            @Option(names = { "-mu", "--model-url" })
            public String modelUrl;
        }

        static class ParameterParameter {
            @Option(names = { "-p", "--parameter" })
            public String parameterName;

            @Option(names = { "-pv", "--parameter-value" })
            public String parameterValue;
        }
    }

    private Map<String, LibrarySourceProvider> librarySourceProviderIndex = new HashMap<>();
    private Map<String, TerminologyProvider> terminologyProviderIndex = new HashMap<>();

    private FhirContext context = FhirContext.forR4();
    private IParser parser = context.newJsonParser().setPrettyPrint(true);

    public static Properties loadProperties() throws IOException {
        Properties config = new Properties();
        InputStream is = CqlCommand.class.getClassLoader().getResourceAsStream("filter.properties");
        config.load(is);
        is.close();
        return config;

    }

    @Override
    public Integer call() throws Exception {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp.toString());

        FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(fhirVersion);

        CqlEvaluatorComponent cqlEvaluatorComponent = DaggerCqlEvaluatorComponent.builder()
                .fhirContext(fhirVersionEnum.newContext()).build();

        CqlOptions cqlOptions = CqlOptions.defaultOptions();

        if (optionsPath != null) {
            CqlTranslatorOptions options = CqlTranslatorOptionsMapper.fromFile(optionsPath);
            cqlOptions.setCqlTranslatorOptions(options);
        }

        Properties filter = loadProperties();

        ArrayList<String> filteredFields = new ArrayList<>(Arrays.asList(
                filter.getProperty(libraries.get(0).libraryName).split(",")));
        for (LibraryParameter library : libraries) {
            File membersFolder = new File(library.model.modelUrl);
            File[] memberFiles = membersFolder.listFiles();

            LibrarySourceProvider librarySourceProvider = librarySourceProviderIndex.get(library.libraryUrl);

            if (librarySourceProvider == null) {
                librarySourceProvider = cqlEvaluatorComponent.createLibrarySourceProviderFactory()
                        .create(new EndpointInfo().setAddress(library.libraryUrl));
                this.librarySourceProviderIndex.put(library.libraryUrl, librarySourceProvider);
            }

            for (File memberFile : memberFiles) {
                CqlEvaluatorBuilder cqlEvaluatorBuilder = cqlEvaluatorComponent.createBuilder()
                        .withCqlOptions(cqlOptions);

                cqlEvaluatorBuilder.withLibrarySourceProvider(librarySourceProvider);

                if (library.terminologyUrl != null) {
                    TerminologyProvider terminologyProvider = this.terminologyProviderIndex.get(library.terminologyUrl);
                    if (terminologyProvider == null) {
                        terminologyProvider = cqlEvaluatorComponent.createTerminologyProviderFactory()
                                .create(new EndpointInfo().setAddress(library.terminologyUrl));
                        this.terminologyProviderIndex.put(library.terminologyUrl, terminologyProvider);
                    }

                    cqlEvaluatorBuilder.withTerminologyProvider(terminologyProvider);
                }

                DataProviderComponents dataProvider = null;
                DataProviderFactory dataProviderFactory = cqlEvaluatorComponent.createDataProviderFactory();

                if (library.model != null) {
                    dataProvider = dataProviderFactory.create(new EndpointInfo().setAddress(memberFile.toString()));
                }
                // default to FHIR
                else {
                    dataProvider = dataProviderFactory
                            .create(new EndpointInfo().setType(Constants.HL7_FHIR_FILES_CODE));
                }

                cqlEvaluatorBuilder.withModelResolverAndRetrieveProvider(dataProvider.getModelUri(),
                        dataProvider.getModelResolver(),
                        dataProvider.getRetrieveProvider());

                CqlEvaluator evaluator = cqlEvaluatorBuilder.build();

                VersionedIdentifier identifier = new VersionedIdentifier().withId(library.libraryName);

                Pair<String, Object> contextParameter = null;

                if (library.context != null) {
                    contextParameter = Pair.of(library.context.contextName, library.context.contextValue);
                }

                EvaluationResult result = evaluator.evaluate(identifier, contextParameter);

                JSONObject json = new JSONObject();
                for (Map.Entry<String, ExpressionResult> libraryEntry : result.expressionResults.entrySet()) {
                    if (filteredFields.contains(libraryEntry.getKey())) {
                        continue;
                    }
                    String value = tempConvert(libraryEntry.getValue().value());
                    if (value.startsWith("{")) {
                        json.put(libraryEntry.getKey(), new JSONObject(value));
                    } else if (value.startsWith("[") || value.equals("[]") || value.equals("[\"")) {
                        json.put(libraryEntry.getKey(), new JSONArray(value));
                    } else {
                        json.put(libraryEntry.getKey(), value);
                    }
                }

                String memberId = FilenameUtils.getBaseName(memberFile.toString());
                String filename = "measure-output\\member-" + memberId + ".json";
                try (FileWriter jsonFile = new FileWriter(filename)) {
                    jsonFile.write(json.toString(4));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp.toString());

        return 0;
    }

    private boolean checkIBase(Object value) {
        if (value instanceof IBaseResource || value instanceof IBaseDatatype || value instanceof IBase) {
            return true;
        }
        return false;
    }

    private String tempConvert(Object value) {
        if (value == null) {
            return "null";
        }

        String result = "";
        if (value instanceof Iterable) {
            result += "[";
            Iterable<?> values = (Iterable<?>) value;
            for (Object o : values) {
                result += (tempConvert(o) + ", ");
            }

            if (result.length() > 1) {
                result = result.substring(0, result.length() - 2);
            }

            result += "]";
        } else if (checkIBase(value)) {
            result = parser.encodeResourceToString((IBaseResource) value);
        } else if (value.toString().startsWith("Interval")) {
            Interval interval = (Interval) value;
            result = "{ \"low\":\"" + interval.getLow() + "\", \"high\":\"" + interval.getHigh() +
                    "\", \"lowClosed\":" + interval.getLowClosed() + ", \"highClosed\": " + interval.getHighClosed()
                    + " }";
        } else {
            result = value.toString();
        }

        return result;
    }

}
