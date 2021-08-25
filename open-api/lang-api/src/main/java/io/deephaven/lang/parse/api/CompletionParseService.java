package io.deephaven.lang.parse.api;

import io.deephaven.io.logger.Logger;

import java.util.List;

/**
 * A specialized parser for autocompletion; maybe better to call it a chunker than a parser...
 */
public interface CompletionParseService<ResultType extends ParsedResult, ChangeType, ParseErrorType extends Throwable> {

    ResultType parse(String document) throws ParseErrorType;

    void open(String text, String uri, String version);

    void update(
        String uri,
        String version,
        List<ChangeType> changes);

    void remove(String uri);

    ResultType finish(String uri);

    void close(final String uri);

}
