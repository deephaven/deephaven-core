//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.string.cache;

import org.assertj.core.api.AssumptionExceptionFactory;
import org.assertj.core.api.Assumptions;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCharSequenceUtils {
    @Test
    public void ascii() {
        assertEqualsIgnoreCase("HELLO, world!", "hello, WORLD!");
        assertLessThanIgnoreCase("a", "B");
        assertLessThanIgnoreCase("A", "b");
        assertLessThanIgnoreCase("Hela", "hello");
        assertLessThanIgnoreCase("Hela.....", "hello");
        assertLessThanIgnoreCase("", "anything");
    }

    @Test
    public void germanSharp() {
        // Demonstrates that you can't use String.toUpperCase / String.toLowerCase for impl; *even though* Java
        // upperCase on ß is SS, the behavior of String.equalsIgnoreCase is explicitly around code points.
        final String germanSharpLower = "ß";
        final String germanSharpUpper = "ẞ";
        assertThat(germanSharpLower.toUpperCase()).isEqualTo("SS");

        assertLessThanIgnoreCase("ss", germanSharpLower);
        assertLessThanIgnoreCase("SS", germanSharpLower);
        assertLessThanIgnoreCase("ss", germanSharpUpper);
        assertLessThanIgnoreCase("SS", germanSharpUpper);
        assertEqualsIgnoreCase(germanSharpLower, germanSharpUpper);
    }

    @Test
    public void kelvin() {
        // This explicitly demonstrates that you _need_ to do a Character.toLowerCase, and can't get away with just a
        // Character.toUpperCase.
        // Explicitly typing this out as unicode value; most fonts render this the exact same as upper-case "K"
        // noinspection UnnecessaryUnicodeEscape
        final String kelvin = "\u212A"; // K <-- see, this looks like "K", right?
        final String lowerCaseK = "k";
        final String upperCaseK = "K";
        assertEqualsIgnoreCase(kelvin, lowerCaseK);
        assertEqualsIgnoreCase(kelvin, upperCaseK);
    }

    @Test
    public void microSign() {
        // Demonstrates that even if the character is already lower case, you still need to do Character.toUpperCase +
        // Character.toLowerCase
        final String microSignLower1 = "" + (char) 181;
        final String microSignLower2 = "" + (char) 956;
        final String capitalMicro = "" + (char) 924;
        assertThat(Character.isLowerCase(181)).isTrue();
        assertThat(Character.toUpperCase(181)).isEqualTo(924);
        assertThat(Character.toLowerCase(924)).isEqualTo(956);

        assertEqualsIgnoreCase(microSignLower1, microSignLower2);
        assertEqualsIgnoreCase(microSignLower1, capitalMicro);
        assertEqualsIgnoreCase(microSignLower2, capitalMicro);
    }

    @Test
    public void deseretScript() {
        // This case is not handled in 11, works on 17+. (Don't care about anything in-between.)
        Assumptions.assumeThat(Runtime.version()).isGreaterThanOrEqualTo(Runtime.Version.parse("17"));

        final String deseretSmallA = "\uD801\uDC28"; // 𐐨 (Deseret Small Letter Long I)
        final String deseretCapitalA = "\uD801\uDC00"; // 𐐀 (Deseret Capital Letter Long I)
        assertEqualsIgnoreCase(deseretSmallA, deseretCapitalA);
    }

    @Test
    public void adlamScript() {
        // This case is not handled in 11, works on 17+. (Don't care about anything in-between.)
        Assumptions.assumeThat(Runtime.version()).isGreaterThanOrEqualTo(Runtime.Version.parse("17"));

        final String adlamLower = "\uD83A\uDD00\uD83A\uDD01\uD83A\uDD02";
        final String adlamUpper = "\uD83A\uDD22\uD83A\uDD23\uD83A\uDD24";
        assertEqualsIgnoreCase(adlamLower, adlamUpper);
    }

    @Test
    public void mathStyled() {
        // Math styled characters are *not* equals ignoring case
        final String styledLower = "\uD835\uDC1A\uD835\uDC1B\uD835\uDC1C"; // 𝐚𝐛𝐜
        final String styledUpper = "\uD835\uDC00\uD835\uDC01\uD835\uDC02"; // 𝐀𝐁𝐂
        assertLessThanIgnoreCase(styledUpper, styledLower);
    }

    private static void assertEqualsIgnoreCase(final CharSequence s1, final CharSequence s2) {
        // This is more of a pre-condition assertion for the test (making sure this test is consistent with
        // String.equalsIgnoreCase).
        assertThat(s1.toString().equalsIgnoreCase(s2.toString())).isTrue();

        assertThat(CharSequenceUtils.contentEqualsIgnoreCase(s1, s2)).isTrue();
        assertThat(CharSequenceUtils.contentEqualsIgnoreCase(s2, s1)).isTrue();
        assertThat(CharSequenceUtils.compareToIgnoreCase(s1, s2)).isZero();
        assertThat(CharSequenceUtils.compareToIgnoreCase(s2, s1)).isZero();
        assertThat(CharSequenceUtils.caseInsensitiveHashCode(s1))
                .isEqualTo(CharSequenceUtils.caseInsensitiveHashCode(s2));
    }

    private static void assertLessThanIgnoreCase(final CharSequence s1, final CharSequence s2) {
        // This is more of a pre-condition assertion for the test (making sure this test is consistent with
        // String.compareToIgnoreCase).
        assertThat(s1.toString().compareToIgnoreCase(s2.toString())).isNegative();

        assertThat(CharSequenceUtils.contentEqualsIgnoreCase(s1, s2)).isFalse();
        assertThat(CharSequenceUtils.contentEqualsIgnoreCase(s2, s1)).isFalse();
        assertThat(CharSequenceUtils.compareToIgnoreCase(s1, s2)).isNegative();
        assertThat(CharSequenceUtils.compareToIgnoreCase(s2, s1)).isPositive();
        // can't say anything about hashCode for non-equal items
    }
}
