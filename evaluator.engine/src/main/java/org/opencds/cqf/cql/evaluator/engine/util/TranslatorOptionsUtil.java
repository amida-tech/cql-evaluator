package org.opencds.cqf.cql.evaluator.engine.util;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;

import static java.util.Objects.requireNonNull;

import org.cqframework.cql.cql2elm.CqlTranslator;
import org.cqframework.cql.elm.execution.Library;

/**
 * This class provides functions for extracting and parsing CQL Translator Options from
 * a Library
 */
public class TranslatorOptionsUtil {

    /**
     * Gets the translator options used to generate an elm Library.
     * 
     * Returns null if the translator options could not be determined.
     * (for example, the Library was translated without annotations)
     * @param library The library to extracts the options from.
     * @return The set of options used to translate the library.
     */
    public static EnumSet<CqlTranslator.Options> getTranslatorOptions(Library library) {
        requireNonNull(library, "library can not be null");
        if (library.getAnnotation() == null || library.getAnnotation().isEmpty()) {
            return null;
        }

        String translatorOptions = getTranslatorOptions(library.getAnnotation());
        return parseTranslatorOptions(translatorOptions);
    }

    @SuppressWarnings("unchecked")
    private static String getTranslatorOptions(List<Object> annotations){
        for (Object o : annotations) {
            LinkedHashMap<String, String> lhm;
            if (o instanceof LinkedHashMap) {
                try {
                    lhm = (LinkedHashMap<String, String>)o;
                    String options = lhm.get("translatorOptions");
                    if (options != null) {
                        return options;
                    }
                }
                catch(Exception e) {
                    continue;
                }

            }
        }

        return null;
    }

    /**
     * Parses a string representing CQL Translator Options into an EnumSet. The string is expected
     * to be a comma delimited list of values from the CqlTranslator.Options enumeration.
     * For example "EnableListPromotion, EnableListDemotion".
     * @param translatorOptions the string to parse
     * @return the set of options
     */
    public static EnumSet<CqlTranslator.Options> parseTranslatorOptions(String translatorOptions) {
        if (translatorOptions == null) {
            return null;
        }

        EnumSet<CqlTranslator.Options> optionSet = EnumSet.noneOf(CqlTranslator.Options.class);
        String[] options = translatorOptions.trim().split(",");

        for (String option : options) {
            optionSet.add(CqlTranslator.Options.valueOf(option));
        }

        return optionSet;
    }
}
