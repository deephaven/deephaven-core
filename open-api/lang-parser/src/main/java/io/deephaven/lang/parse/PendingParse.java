package io.deephaven.lang.parse;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.api.ParseCancelled;
import io.deephaven.lang.generated.Chunker;
import io.deephaven.lang.generated.ChunkerDocument;
import io.deephaven.lang.generated.ParseException;
import io.deephaven.lang.generated.TokenMgrException;
import io.deephaven.lang.shared.lsp.CompletionCancelled;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

/**
 * An object to represent the operation of parsing documents from users.
 *
 * Because we update the server with new document state as the user types,
 * we don't want to get a deluge of stale parse operations sucking down CPU.
 *
 * Only one version (the newest) of a given document will be parsed;
 * all others will be cancelled.
 */
public class PendingParse {

    /**
     * A logger. For logging things.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(PendingParse.class);

    /**
     * A ParseJob represents a unit of work for our parser.
     * Note that we only ever want to parse / use the newest ParseJob,
     * and only when there is no new parse job incoming, as completion requests
     * must only ever be issued when the document is stable (if the document is changing,
     * then any completion request is stale, as the user is typing new code).
     */
    private final class ParseJob {

        /**
         * monaco-supplied version string.
         * We use this to sanity check that we're not parsing an old document.
         * We _should_ do some testing that Ctrl+Z does not re-issue an old version identifier.
         */
        private final String version;
        /**
         * The text of the document.  Monaco only sends us diffs of changes, and our parser
         * is not (yet) incremental, so we rebuild the whole document from the text of the previous state of the document.
         */
        private final String text;

        /**
         * The language of this parse job. Should be one of the fields in {@link io.deephaven.lang.parse.api.Languages}
         */
        private final String language;

        /**
         * The final parsed result.  Only non-null after a parse operation has completed.
         * Note that we only ever read this field inside a synchronized() block, so no need for volatile here.
         */
        private ParsedDocument result;

        private ParseJob(@NotNull final String version, @NotNull final String text, final String language) {
            this.text = text;
            this.version = version;
            this.language = language;
        }

        /**
         * Attempt to perform a document parse.
         *
         * @return true if parsing was successful, false if it failed with a parse exception, and null if we were interrupted.
         */
        private Boolean parse() {
            try {
                final Chunker chunker = new Chunker(text);
                final ChunkerDocument chunkerDocument = chunker.Document();
                result = new ParsedDocument(chunkerDocument, text);
                return true;
            } catch (ParseCancelled expected) {
                // Cancelled, nothing to do here
                return null;
            } catch (TokenMgrException | ParseException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn()
                            .append("Parser for ").append(uri)
                            .append(": Encountered parse exception ").append(e)
                            .append(" while parsing ").append(text)
                            .endl();
                }
                return false;
            }
        }
    }

    /**
     * The unique identifier for the document we are responsible for parsing.
     */
    private final String uri;

    /**
     * The thread doing the parse work.
     * We (over)use this field to interrupt the parser, and as an object monitor to un/pause our threads.
     */
    private final Thread parseThread;

    /**
     * Set whenever the document is mutated and we need to reparse.
     * This is volatile so we can read the `String text` inside it without acquiring any locks.
     */
    private volatile ParseJob targetState;
    /**
     * Set when a parse has completed either successfully or unsuccessfully.
     * We use this to prevent re-parsing a document so invalid that the parser blows up.
     */
    private ParseJob lastParseState;
    /**
     * Set when the parse has completed successfully.
     * We only ever return a valid result when completedState == lastParseState == targetState,
     * as this means "parse has completed successfully, and document has not been mutated.
     */
    private ParseJob completedState;

    /**
     * Always true while user is connected to IDE session.  False once user disconnects and we need to stop working.
     */
    private volatile boolean alive;
    /**
     * Set to false when we know a document change is incoming, but a new parse has not been requested.
     * Used to keep the parser thread cleared while we are waiting.
     */
    private volatile boolean valid;

    PendingParse(String uri) {
        this.uri = uri;
        alive = true;
        parseThread = new Thread(this::parse, "ParserFor" + uri);
        parseThread.setDaemon(true);
        parseThread.start();
    }

    private ParseJob awaitNext() {
        while (alive) {
            synchronized (parseThread) {
                if (valid && targetState != null && targetState != lastParseState) {
                    // we'll set lastParseState if a parse completes, whether with success or failure.
                    // targetState _might_ get processed more than once, if a superfluous interrupt is issued;
                    // note that we can probably replace lastParseState w/ a `boolean finished` field in the ParseJob
                    return targetState;
                }
                try {
                    // wait until there is a new targetState to process.
                    parseThread.wait();
                } catch (InterruptedException expected) {
                    // Let the loop come back around - interrupt() is used as well as notify().
                }
            }
        }
        return null;
    }

    private void parse(){
        ParseJob pending;
        // awaitNext() will return the targetState so long as it has never completed a parse attempt
        while ((pending = awaitNext()) != null) {
            // Perform the parsing.  This is a potentially expensive operation.
            final Boolean success = pending.parse();
            synchronized (parseThread) {
                // we'll return true if parse succeeded, false if failed, and null if interrupted.
                if (Boolean.TRUE.equals(success)) {
                    // pending state was successfully parsed, signal that it has a completed document, and should not be run again
                    completedState = lastParseState = pending;
                } else if (Boolean.FALSE.equals(success)){ // failed
                    // signal that the pending state cannot be parsed, so don't try again.
                    // In this state, we will allow fallback to V1 parser if the document has not changed since we failed,
                    // otherwise, we will cancel all parse attempts until the document no longer causes the parser to blow up.
                    lastParseState = pending;
                }
                // wake up anyone waiting on us (in finishParse())
                // note that if the parse was cancelled, we still want to wake up pending completion requests,
                // since they are no longer valid and should be cancelled.
                parseThread.notifyAll();
            }
        }
    }

    /**
     * Called when the document has been updated and we need to reparse the latest document text.
     * @param version The monaco-supplied version string.  Monatomically increasing (TODO: test editor undo operations)
     * @param text The full text of the document
     * @param language
     * @param force A boolean to signal if we should forcibly parse, ignoring current state.
     */
    void requestParse(@NotNull final String version, @NotNull final String text, final String language, final boolean force) {
        final ParseJob localTargetState;
        // hm, if we fail the version check, we should actually send a message to the client, and tell them
        // to send us the full document text so that we can get into a good state.  Failing the check means our
        // document diff application has likely resulted in a malformed document.
        // TODO: test if we _can_ even get into a bad state
        if (force || (localTargetState = targetState) == null || localTargetState.version.compareTo(version) < 0) {

            // first, interrupt, in case the parser is running a stale job, we want it to die (it checks interrupt state).
            // it's unlikely this will do much, since we already interrupted in invalidate(), but it's possible to need
            // this if there's three edits in flight at once; invalidate() cancels the first, we cancel the second,
            // and then we queue up the third, current request, below.
            parseThread.interrupt();

            synchronized (parseThread) {
                // next, set the targetState so the parse thread can pick up the work we want.
                // we do this inside this synchronized block, so the logic in finishParse() can consider targetState atomic
                // (we only *read* targetState field outside of a synchronized block).
                targetState = new ParseJob(version, text, language);
                // allows parse attempts to occur.  We set valid=false when we invalidate() a parse upon receiving document updates.
                valid = true;
                // now, just in case this thread was paused in this method call, we want to be sure that the job is picked up,
                // WITHOUT telling the parser to cancel (if it has already started working on targetState).
                // i.e. if we interrupted the thread and then the parse thread woke up, it would have noticed valid=false,
                // and gone back to sleep; so, we issue an extra wake up here.  It's very unlikely to occur, but better to be safe...
                parseThread.notifyAll();
            }
        }
    }

    /**
     * Called inside CompletionQuery to request / block on a parsed document.
     * @return Optional.empty() if we have either failed a parse, or never parsed a document (tells calling code to fallback to V1 autocomplete).
     *         Optional.of(validDocument) iff we have successfully parsed a document and no new changes have arrived.
     * @throws CompletionCancelled if the document was mutate while we were blocking.
     */
    Optional<ParsedDocument> finishParse() {
        final ParseJob localTargetState = targetState;
        // I don't like big synchronized blocks, but the only "expensive" thing we do in here is some logging.
        synchronized (parseThread) {
            // loop while alive, valid, and not finished parsing.
            while (alive && valid && localTargetState != lastParseState) {
                if (localTargetState != targetState) {
                    // if the document changed while we were waiting on a parse, the current
                    // completion request is no longer valid, even though parsing _has_ completed.
                    // This can happen if a very large document has issued a completion request,
                    // but the user kept typing; we want to fail the current request quickly.
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info()
                                .append("Document changed while awaiting parsing; failing current request ")
                                .append(localTargetState.version)
                                .endl();
                    }
                    // this exception tells calling code that it should immediately fail,
                    // rather than fall back to the V1 parser.
                    throw new CompletionCancelled();
                }
                try {
                    parseThread.wait();
                } catch (InterruptedException failure) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn()
                                .append("Unexpected interruption of document parser")
                                .append(failure)
                                .endl();
                    }
                    Thread.currentThread().interrupt(); // percolate interruption
                    throw new CompletionCancelled(); // calling code will catch this to fail-fast (does not allow fallback).
                }
            } // end while(){} loop

            // we have been woken up.  Either parsing has finished, or the document has been mutated under us.
            if (!alive) {
                // user closed tab while we were working.  give up.
                throw new CompletionCancelled();
            }
            if (!valid) {
                // there is a document edit incoming. Do not finish this completion request.
                throw new CompletionCancelled();
            }
            if (completedState == null) {
                // We have never successfully parsed the document; let calling code fallback to V1 parser
                return Optional.empty();
            }
            if (lastParseState == targetState) {
                // The parse operation completed w/ success or failure, and the document has not been mutated under us.
                if (completedState == lastParseState) {
                    // The parse completed successfully, and the document is uptodate.  Use the result to perform completion.
                    return Optional.of(completedState.result);
                }
                // The parse failed, but the document is uptodate.  Allow fallback to V1 parser.
                return Optional.empty();
            }
            // The parse completed successfully, but the document is no longer uptodate.
            // Throw a cancellation so we don't even finish the autocomplete query
            throw new CompletionCancelled();
        }
    }

    /**
     * Called when the user session is ended, and we need to shutdown.
     */
    public void cancel() {
        alive = false;
        parseThread.interrupt();
        synchronized (parseThread) {
            parseThread.notifyAll();
        }
    }

    /**
     * @return The full document text of the last-received document edit.
     */
    public String getText() {
        final ParseJob localTargetState = targetState;
        return localTargetState == null ? "" : localTargetState.text;
    }

    /**
     * Called when the document has been updated,
     * but before we are ready to submit new text to be parsed.
     *
     * We'll just interrupt the parser thread, so it has time to cancel any work
     * before our caller submits the new work.  Note that we don't update the targetState field,
     * since we use it to return the current text.
     *
     * Our caller must call getText() above and then apply diffs to create new document text
     * before it can call .requestParse().  Calling invalidate() gives the parser thread a little
     * extra time to realize that it is processing stale input and throw ParseCancelled(),
     * so the parser thread can be ready to immediately start work when .requestParse() is called.
     */
    public void invalidate() {
        valid = false;
        parseThread.interrupt();
    }
}
