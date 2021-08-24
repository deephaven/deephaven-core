package io.deephaven.base.testing;

import io.deephaven.base.verify.Require;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class Matchers {
    // ----------------------------------------------------------------
    public static <T> Matcher<T> thumbprint(Class<T> type, final Thumbprinter<T> thumbprinter,
        String thumbprint) {
        return new ThumbprintMatcher<T>(type, thumbprint) {
            @Override
            public String getThumbprint(T t) {
                return thumbprinter.getThumbprint(t);
            }
        };
    }

    // ----------------------------------------------------------------
    public static abstract class ThumbprintMatcher<T> extends BaseMatcher<T>
        implements Thumbprinter<T> {

        private final Class<T> m_type;
        private final String m_thumbprint;

        protected ThumbprintMatcher(Class<T> type, String thumbprint) {
            Require.neqNull(type, "type");
            m_type = type;
            m_thumbprint = thumbprint;
        }

        @Override
        public boolean matches(Object item) {
            // noinspection unchecked
            return m_type.isInstance(item)
                && m_thumbprint.equals(getThumbprint((T) item));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(m_type.getName()).appendText(" has thumbprint \"")
                .appendText(m_thumbprint).appendText("\"");
        }
    }
}
