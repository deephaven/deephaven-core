package io.deephaven.csv.util;

import java.util.function.Function;


/**
 * Utility class for rendering Iterables as a string. The methods can intersperse a comma (or other separator), and can
 * take a custom function to render the item as a string.
 */
public class Renderer {
    /**
     * Render the items in {@code items} using the separator ", " and renderer {@link Object#toString}.
     * 
     * @param items The items.
     * @return The items rendered as a {@link String}, separated by {@code separator}.
     */
    public static <T> String renderList(Iterable<T> items) {
        return renderList(items, ", ", Object::toString);
    }

    /**
     * Render the items in {@code items} using a custom separator and renderer {@link Object#toString}.
     * 
     * @param items The items.
     * @param separator The separator.
     * @return The items rendered as a {@link String}, separated by {@code separator}.
     */
    public static <T> String renderList(Iterable<T> items, String separator) {
        return renderList(items, separator, Object::toString);
    }

    /**
     * Render the items in {@code items} using a custom separator and custom renderer.
     * 
     * @param items The items.
     * @param separator The separator.
     * @param renderer The renderer.
     * @return The items rendered as a {@link String}, separated by {@code separator}.
     */
    public static <T> String renderList(Iterable<T> items, final String separator, Function<T, String> renderer) {
        return renderList(new StringBuilder(), items, separator, renderer).toString();
    }

    /**
     * Render the items in {@code items} to the {@link StringBuilder} sb, using the separator {@code separator}, and the
     * custom rendering function {@code renderer}.
     * 
     * @param sb The destination where the text is written to.
     * @param items The items to render.
     * @param separator THe separator to use.
     * @param renderer A function that renders an individual item as a string.
     * @param <T> The element type of {@code items}.
     * @return The passed in {@link StringBuilder} sb.
     */
    public static <T> StringBuilder renderList(StringBuilder sb, Iterable<T> items, final String separator,
            Function<T, String> renderer) {
        String separatorToUse = "";
        for (T item : items) {
            sb.append(separatorToUse);
            sb.append(renderer.apply(item));
            separatorToUse = separator;
        }
        return sb;
    }
}
