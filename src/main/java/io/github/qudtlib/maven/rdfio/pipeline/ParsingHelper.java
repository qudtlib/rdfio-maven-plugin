package io.github.qudtlib.maven.rdfio.pipeline;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class ParsingHelper {
    public static String getNonBlankChildString(Xpp3Dom config, String name) {
        if (config == null) {
            return null;
        }
        Xpp3Dom child = config.getChild(name);
        return getNonBlankString(child);
    }

    public static String getNonBlankString(Xpp3Dom config) {
        if (config == null) {
            return null;
        }
        if (config.getValue() != null && !config.getValue().trim().isBlank()) {
            return config.getValue().trim();
        }
        return null;
    }

    public static <T> void domChildren(
            Xpp3Dom config,
            String name,
            Function<Xpp3Dom, T> childParser,
            Consumer<T> childSetter,
            Supplier<String> usageSupplier,
            int minCount,
            int maxCount)
            throws ConfigurationParseException {
        int count =
                handleDomChildernInternal(config, name, childParser, childSetter, usageSupplier);
        throwCountExceptions(name, usageSupplier, minCount, maxCount, count);
    }

    public static <T> void requiredDomChildren(
            Xpp3Dom config,
            String name,
            Function<Xpp3Dom, T> childParser,
            Consumer<T> childSetter,
            Supplier<String> usageSupplier)
            throws ConfigurationParseException {
        domChildren(config, name, childParser, childSetter, usageSupplier, 1, -1);
    }

    public static <T> void optionalDomChildren(
            Xpp3Dom config,
            String name,
            Function<Xpp3Dom, T> childParser,
            Consumer<T> childSetter,
            Supplier<String> usageSupplier)
            throws ConfigurationParseException {
        domChildren(config, name, childParser, childSetter, usageSupplier, 0, -1);
    }

    public static <T> void requiredDomChild(
            Xpp3Dom config,
            String name,
            Function<Xpp3Dom, T> childParser,
            Consumer<T> childSetter,
            Supplier<String> usageSupplier)
            throws ConfigurationParseException {
        domChildren(config, name, childParser, childSetter, usageSupplier, 1, 1);
    }

    public static <T> void optionalDomChild(
            Xpp3Dom config,
            String name,
            Function<Xpp3Dom, T> childParser,
            Consumer<T> childSetter,
            Supplier<String> usageSupplier)
            throws ConfigurationParseException {
        domChildren(config, name, childParser, childSetter, usageSupplier, 0, 1);
    }

    public static void stringChildren(
            Xpp3Dom config,
            String name,
            Consumer<String> childHandler,
            Supplier<String> usageSupplier,
            int minCount,
            int maxCount)
            throws ConfigurationParseException {
        int count = handleStringChildernInternal(config, name, childHandler, usageSupplier);
        throwCountExceptions(name, usageSupplier, minCount, maxCount, count);
    }

    public static void optionalStringChildren(
            Xpp3Dom config,
            String name,
            Consumer<String> valueSetter,
            Supplier<String> usageSupplier) {
        stringChildren(config, name, valueSetter, usageSupplier, 0, -1);
    }

    public static void requiredStringChildren(
            Xpp3Dom config,
            String name,
            Consumer<String> valueSetter,
            Supplier<String> usageSupplier) {
        stringChildren(config, name, valueSetter, usageSupplier, 1, -1);
    }

    public static void requiredStringChild(
            Xpp3Dom config,
            String name,
            Consumer<String> valueSetter,
            Supplier<String> usageSupplier) {
        stringChildren(config, name, valueSetter, usageSupplier, 1, 1);
    }

    public static void optionalStringChild(
            Xpp3Dom config,
            String name,
            Consumer<String> valueSetter,
            Supplier<String> usageSupplier) {
        stringChildren(config, name, valueSetter, usageSupplier, 0, 1);
    }

    private static void throwCountExceptions(
            String name, Supplier<String> usageSupplier, int minCount, int maxCount, int count)
            throws ConfigurationParseException {
        if (minCount > 0 && count < minCount) {
            throw new ConfigurationParseException(
                    String.format(
                            "At least %d <%s> subelements are allowed, but encountered %d.\n%s",
                            minCount, name, count, usageSupplier.get()));
        }
        if (maxCount > 0 && count > maxCount) {
            throw new ConfigurationParseException(
                    String.format(
                            "At most %d <%s> subelements are allowed, but encountered %d.\n%s",
                            maxCount, name, count, usageSupplier.get()));
        }
        if (maxCount == 0 && count > maxCount) {
            throw new ConfigurationParseException(
                    String.format(
                            "No <%s> subelements are allowed, but encountered %d.\n%s",
                            maxCount, name, count, usageSupplier.get()));
        }
    }

    private static int handleStringChildernInternal(
            Xpp3Dom config,
            String name,
            Consumer<String> childHandler,
            Supplier<String> usageSupplier)
            throws ConfigurationParseException {
        int count = 0;
        for (Xpp3Dom child : config.getChildren(name)) {
            String childString = ParsingHelper.getNonBlankString(child);
            if (childString != null) {
                childHandler.accept(childString);
            } else {
                throw new ConfigurationParseException(
                        String.format(
                                "<%s> subelement must not be empty.\n%s",
                                name, usageSupplier.get()));
            }
            count++;
        }
        return count;
    }

    private static <T> int handleDomChildernInternal(
            Xpp3Dom config,
            String name,
            Function<Xpp3Dom, T> childParser,
            Consumer<T> childSetter,
            Supplier<String> usageSupplier)
            throws ConfigurationParseException {
        int count = 0;
        for (Xpp3Dom child : config.getChildren(name)) {
            T parsedChild = childParser.apply(child);
            childSetter.accept(parsedChild);
            count++;
        }
        return count;
    }
}
