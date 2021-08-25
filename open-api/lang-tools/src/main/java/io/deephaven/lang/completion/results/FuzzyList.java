package io.deephaven.lang.completion.results;

import io.deephaven.web.shared.fu.MappedIterable;
import org.apache.commons.text.similarity.FuzzyScore;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Locale;
import java.util.TreeSet;

/**
 * A list-like object that performs fuzzy sorting on the edit distance of strings.
 *
 * The constructor takes the pattern String (user input), then you can add-by-string-key any number of objects of type
 * T. The score for each String is used to sort entries which contain source object and string key.
 *
 * This can allow you to, for example, sort methods based on distance to a user-entered search query.
 */
public class FuzzyList<T> implements Iterable<T> {

    private static final FuzzyScore scorer = new FuzzyScore(Locale.US);

    private class FuzzyEntry implements Comparable<FuzzyEntry> {
        final String result;
        final T value;
        final double score;

        private FuzzyEntry(String key, T value, double score) {
            this.result = key;
            this.value = value;
            this.score = score;
        }

        @Override
        public int compareTo(@NotNull FuzzyEntry o) {
            if (score == o.score) {
                int myDiff = Math.abs(result.length() - query.length());
                int yourDiff = Math.abs(o.result.length() - query.length());
                if (myDiff == yourDiff) {
                    // length and score is the same, use string comparisons.
                    // If the strings are equal, then we are quite fine w/ discarding duplicates.
                    return result.compareTo(o.result);
                }
                // lowest difference in length wins
                return myDiff < yourDiff ? -1 : 1;
            }
            // highest score wins
            return score > o.score ? -1 : 1;
        }

        private T getValue() {
            return value;
        }
    }

    private final TreeSet<FuzzyEntry> entries;
    private final String query;

    public FuzzyList(String key) {
        this.query = key;
        entries = new TreeSet<>();
    }

    public void add(String key, T item) {
        final Integer score = scorer.fuzzyScore(this.query, key);
        // Later: add extra points for matches that have been used before.
        FuzzyEntry entry = new FuzzyEntry(key, item, score);
        entries.add(entry);
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return MappedIterable.of(entries).mapped(FuzzyEntry::getValue).iterator();
    }
}
