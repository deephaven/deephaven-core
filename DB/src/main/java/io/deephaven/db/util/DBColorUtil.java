/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util;


import io.deephaven.gui.color.Color;

/**
 * Colors moved to {@link Color}. They are left in here for backwards compatibility.
 * <p>
 * Colors are encoded into longs, the 32 least significant bits representing the foreground color
 * and the 32 most significant bits the background color. The 24 least significant bits of each
 * chunk hold the color's RGB values.
 * </p>
 * The 25th bit is flipped. This distinguishes between no formatting (0L) and black.<br>
 * For foreground colors, one of the remaining 7 bits denotes no foreground color overriding when
 * the cell is highlighted in the table. This means the foreground color will stay the same when the
 * cell is highlighted. TODO (deephaven/deephaven-core/issues/175): Move this to a new module and
 * package
 */
@SuppressWarnings({"UnusedDeclaration", "WeakerAccess"})
public class DBColorUtil extends DBColorUtilImpl {

    ////////////////////////////////////// Deprecated Colors //////////////////////////////////////
    @Deprecated
    public static final long VIVID_RED = toLong(Color.VIVID_RED);
    @Deprecated
    public static final long VIVID_YELLOWRED = toLong(Color.VIVID_YELLOWRED);
    @Deprecated
    public static final long VIVID_YELLOW = toLong(Color.VIVID_YELLOW);
    @Deprecated
    public static final long VIVID_GREENYELLOW = toLong(Color.VIVID_GREENYELLOW);
    @Deprecated
    public static final long VIVID_GREEN = toLong(Color.VIVID_GREEN);
    @Deprecated
    public static final long VIVID_BLUEGREEN = toLong(Color.VIVID_BLUEGREEN);
    @Deprecated
    public static final long VIVID_BLUE = toLong(Color.VIVID_BLUE);
    @Deprecated
    public static final long VIVID_PURPLEBLUE = toLong(Color.VIVID_PURPLEBLUE);
    @Deprecated
    public static final long VIVID_PURPLE = toLong(Color.VIVID_PURPLE);
    @Deprecated
    public static final long VIVID_REDPURPLE = toLong(Color.VIVID_REDPURPLE);
    @Deprecated
    public static final long STRONG_RED = toLong(Color.STRONG_RED);
    @Deprecated
    public static final long STRONG_YELLOWRED = toLong(Color.STRONG_YELLOWRED);
    @Deprecated
    public static final long STRONG_YELLOW = toLong(Color.STRONG_YELLOW);
    @Deprecated
    public static final long STRONG_GREENYELLOW = toLong(Color.STRONG_GREENYELLOW);
    @Deprecated
    public static final long STRONG_GREEN = toLong(Color.STRONG_GREEN);
    @Deprecated
    public static final long STRONG_BLUEGREEN = toLong(Color.STRONG_BLUEGREEN);
    @Deprecated
    public static final long STRONG_BLUE = toLong(Color.STRONG_BLUE);
    @Deprecated
    public static final long STRONG_PURPLEBLUE = toLong(Color.STRONG_PURPLEBLUE);
    @Deprecated
    public static final long STRONG_PURPLE = toLong(Color.STRONG_PURPLE);
    @Deprecated
    public static final long STRONG_REDPURPLE = toLong(Color.STRONG_REDPURPLE);
    @Deprecated
    public static final long BRIGHT_RED = toLong(Color.BRIGHT_RED);
    @Deprecated
    public static final long BRIGHT_YELLOWRED = toLong(Color.BRIGHT_YELLOWRED);
    @Deprecated
    public static final long BRIGHT_YELLOW = toLong(Color.BRIGHT_YELLOW);
    @Deprecated
    public static final long BRIGHT_GREENYELLOW = toLong(Color.BRIGHT_GREENYELLOW);
    @Deprecated
    public static final long BRIGHT_GREEN = toLong(Color.BRIGHT_GREEN);
    @Deprecated
    public static final long BRIGHT_BLUEGREEN = toLong(Color.BRIGHT_BLUEGREEN);
    @Deprecated
    public static final long BRIGHT_BLUE = toLong(Color.BRIGHT_BLUE);
    @Deprecated
    public static final long BRIGHT_PURPLEBLUE = toLong(Color.BRIGHT_PURPLEBLUE);
    @Deprecated
    public static final long BRIGHT_PURPLE = toLong(Color.BRIGHT_PURPLE);
    @Deprecated
    public static final long BRIGHT_REDPURPLE = toLong(Color.BRIGHT_REDPURPLE);
    @Deprecated
    public static final long PALE_RED = toLong(Color.PALE_RED);
    @Deprecated
    public static final long PALE_YELLOWRED = toLong(Color.PALE_YELLOWRED);
    @Deprecated
    public static final long PALE_YELLOW = toLong(Color.PALE_YELLOW);
    @Deprecated
    public static final long PALE_GREENYELLOW = toLong(Color.PALE_GREENYELLOW);
    @Deprecated
    public static final long PALE_GREEN = toLong(Color.PALE_GREEN);
    @Deprecated
    public static final long PALE_BLUEGREEN = toLong(Color.PALE_BLUEGREEN);
    @Deprecated
    public static final long PALE_BLUE = toLong(Color.PALE_BLUE);
    @Deprecated
    public static final long PALE_PURPLEBLUE = toLong(Color.PALE_PURPLEBLUE);
    @Deprecated
    public static final long PALE_PURPLE = toLong(Color.PALE_PURPLE);
    @Deprecated
    public static final long PALE_REDPURPLE = toLong(Color.PALE_REDPURPLE);
    @Deprecated
    public static final long VERYPALE_RED = toLong(Color.VERYPALE_RED);
    @Deprecated
    public static final long VERYPALE_YELLOWRED = toLong(Color.VERYPALE_YELLOWRED);
    @Deprecated
    public static final long VERYPALE_YELLOW = toLong(Color.VERYPALE_YELLOW);
    @Deprecated
    public static final long VERYPALE_GREENYELLOW = toLong(Color.VERYPALE_GREENYELLOW);
    @Deprecated
    public static final long VERYPALE_GREEN = toLong(Color.VERYPALE_GREEN);
    @Deprecated
    public static final long VERYPALE_BLUEGREEN = toLong(Color.VERYPALE_BLUEGREEN);
    @Deprecated
    public static final long VERYPALE_BLUE = toLong(Color.VERYPALE_BLUE);
    @Deprecated
    public static final long VERYPALE_PURPLEBLUE = toLong(Color.VERYPALE_PURPLEBLUE);
    @Deprecated
    public static final long VERYPALE_PURPLE = toLong(Color.VERYPALE_PURPLE);
    @Deprecated
    public static final long VERYPALE_REDPURPLE = toLong(Color.VERYPALE_REDPURPLE);
    @Deprecated
    public static final long LIGHTGRAYISH_RED = toLong(Color.LIGHTGRAYISH_RED);
    @Deprecated
    public static final long LIGHTGRAYISH_YELLOWRED = toLong(Color.LIGHTGRAYISH_YELLOWRED);
    @Deprecated
    public static final long LIGHTGRAYISH_YELLOW = toLong(Color.LIGHTGRAYISH_YELLOW);
    @Deprecated
    public static final long LIGHTGRAYISH_GREENYELLOW = toLong(Color.LIGHTGRAYISH_GREENYELLOW);
    @Deprecated
    public static final long LIGHTGRAYISH_GREEN = toLong(Color.LIGHTGRAYISH_GREEN);
    @Deprecated
    public static final long LIGHTGRAYISH_BLUEGREEN = toLong(Color.LIGHTGRAYISH_BLUEGREEN);
    @Deprecated
    public static final long LIGHTGRAYISH_BLUE = toLong(Color.LIGHTGRAYISH_BLUE);
    @Deprecated
    public static final long LIGHTGRAYISH_PURPLEBLUE = toLong(Color.LIGHTGRAYISH_PURPLEBLUE);
    @Deprecated
    public static final long LIGHTGRAYISH_PURPLE = toLong(Color.LIGHTGRAYISH_PURPLE);
    @Deprecated
    public static final long LIGHTGRAYISH_REDPURPLE = toLong(Color.LIGHTGRAYISH_REDPURPLE);
    @Deprecated
    public static final long LIGHT_RED = toLong(Color.LIGHT_RED);
    @Deprecated
    public static final long LIGHT_YELLOWRED = toLong(Color.LIGHT_YELLOWRED);
    @Deprecated
    public static final long LIGHT_YELLOW = toLong(Color.LIGHT_YELLOW);
    @Deprecated
    public static final long LIGHT_GREENYELLOW = toLong(Color.LIGHT_GREENYELLOW);
    @Deprecated
    public static final long LIGHT_GREEN = toLong(Color.LIGHT_GREEN);
    @Deprecated
    public static final long LIGHT_BLUEGREEN = toLong(Color.LIGHT_BLUEGREEN);
    @Deprecated
    public static final long LIGHT_BLUE = toLong(Color.LIGHT_BLUE);
    @Deprecated
    public static final long LIGHT_PURPLEBLUE = toLong(Color.LIGHT_PURPLEBLUE);
    @Deprecated
    public static final long LIGHT_PURPLE = toLong(Color.LIGHT_PURPLE);
    @Deprecated
    public static final long LIGHT_REDPURPLE = toLong(Color.LIGHT_REDPURPLE);
    @Deprecated
    public static final long GRAYISH_RED = toLong(Color.GRAYISH_RED);
    @Deprecated
    public static final long GRAYISH_YELLOWRED = toLong(Color.GRAYISH_YELLOWRED);
    @Deprecated
    public static final long GRAYISH_YELLOW = toLong(Color.GRAYISH_YELLOW);
    @Deprecated
    public static final long GRAYISH_GREENYELLOW = toLong(Color.GRAYISH_GREENYELLOW);
    @Deprecated
    public static final long GRAYISH_GREEN = toLong(Color.GRAYISH_GREEN);
    @Deprecated
    public static final long GRAYISH_BLUEGREEN = toLong(Color.GRAYISH_BLUEGREEN);
    @Deprecated
    public static final long GRAYISH_BLUE = toLong(Color.GRAYISH_BLUE);
    @Deprecated
    public static final long GRAYISH_PURPLEBLUE = toLong(Color.GRAYISH_PURPLEBLUE);
    @Deprecated
    public static final long GRAYISH_PURPLE = toLong(Color.GRAYISH_PURPLE);
    @Deprecated
    public static final long GRAYISH_REDPURPLE = toLong(Color.GRAYISH_REDPURPLE);
    @Deprecated
    public static final long DULL_RED = toLong(Color.DULL_RED);
    @Deprecated
    public static final long DULL_YELLOWRED = toLong(Color.DULL_YELLOWRED);
    @Deprecated
    public static final long DULL_YELLOW = toLong(Color.DULL_YELLOW);
    @Deprecated
    public static final long DULL_GREENYELLOW = toLong(Color.DULL_GREENYELLOW);
    @Deprecated
    public static final long DULL_GREEN = toLong(Color.DULL_GREEN);
    @Deprecated
    public static final long DULL_BLUEGREEN = toLong(Color.DULL_BLUEGREEN);
    @Deprecated
    public static final long DULL_BLUE = toLong(Color.DULL_BLUE);
    @Deprecated
    public static final long DULL_PURPLEBLUE = toLong(Color.DULL_PURPLEBLUE);
    @Deprecated
    public static final long DULL_PURPLE = toLong(Color.DULL_PURPLE);
    @Deprecated
    public static final long DULL_REDPURPLE = toLong(Color.DULL_REDPURPLE);
    @Deprecated
    public static final long DEEP_RED = toLong(Color.DEEP_RED);
    @Deprecated
    public static final long DEEP_YELLOWRED = toLong(Color.DEEP_YELLOWRED);
    @Deprecated
    public static final long DEEP_YELLOW = toLong(Color.DEEP_YELLOW);
    @Deprecated
    public static final long DEEP_GREENYELLOW = toLong(Color.DEEP_GREENYELLOW);
    @Deprecated
    public static final long DEEP_GREEN = toLong(Color.DEEP_GREEN);
    @Deprecated
    public static final long DEEP_BLUEGREEN = toLong(Color.DEEP_BLUEGREEN);
    @Deprecated
    public static final long DEEP_BLUE = toLong(Color.DEEP_BLUE);
    @Deprecated
    public static final long DEEP_PURPLEBLUE = toLong(Color.DEEP_PURPLEBLUE);
    @Deprecated
    public static final long DEEP_PURPLE = toLong(Color.DEEP_PURPLE);
    @Deprecated
    public static final long DEEP_REDPURPLE = toLong(Color.DEEP_REDPURPLE);
    @Deprecated
    public static final long DARK_RED = toLong(Color.DARK_RED);
    @Deprecated
    public static final long DARK_YELLOWRED = toLong(Color.DARK_YELLOWRED);
    @Deprecated
    public static final long DARK_YELLOW = toLong(Color.DARK_YELLOW);
    @Deprecated
    public static final long DARK_GREENYELLOW = toLong(Color.DARK_GREENYELLOW);
    @Deprecated
    public static final long DARK_GREEN = toLong(Color.DARK_GREEN);
    @Deprecated
    public static final long DARK_BLUEGREEN = toLong(Color.DARK_BLUEGREEN);
    @Deprecated
    public static final long DARK_BLUE = toLong(Color.DARK_BLUE);
    @Deprecated
    public static final long DARK_PURPLEBLUE = toLong(Color.DARK_PURPLEBLUE);
    @Deprecated
    public static final long DARK_PURPLE = toLong(Color.DARK_PURPLE);
    @Deprecated
    public static final long DARK_REDPURPLE = toLong(Color.DARK_REDPURPLE);
    @Deprecated
    public static final long DARKGRAYISH_RED = toLong(Color.DARKGRAYISH_RED);
    @Deprecated
    public static final long DARKGRAYISH_YELLOWRED = toLong(Color.DARKGRAYISH_YELLOWRED);
    @Deprecated
    public static final long DARKGRAYISH_YELLOW = toLong(Color.DARKGRAYISH_YELLOW);
    @Deprecated
    public static final long DARKGRAYISH_GREENYELLOW = toLong(Color.DARKGRAYISH_GREENYELLOW);
    @Deprecated
    public static final long DARKGRAYISH_GREEN = toLong(Color.DARKGRAYISH_GREEN);
    @Deprecated
    public static final long DARKGRAYISH_BLUEGREEN = toLong(Color.DARKGRAYISH_BLUEGREEN);
    @Deprecated
    public static final long DARKGRAYISH_BLUE = toLong(Color.DARKGRAYISH_BLUE);
    @Deprecated
    public static final long DARKGRAYISH_PURPLEBLUE = toLong(Color.DARKGRAYISH_PURPLEBLUE);
    @Deprecated
    public static final long DARKGRAYISH_PURPLE = toLong(Color.DARKGRAYISH_PURPLE);
    @Deprecated
    public static final long DARKGRAYISH_REDPURPLE = toLong(Color.DARKGRAYISH_REDPURPLE);
    @Deprecated
    public static final long BLACK = toLong(Color.BLACK);
    @Deprecated
    public static final long GRAY1 = toLong(Color.GRAY1);
    @Deprecated
    public static final long GRAY2 = toLong(Color.GRAY2);
    @Deprecated
    public static final long GRAY3 = toLong(Color.GRAY3);
    @Deprecated
    public static final long GRAY4 = toLong(Color.GRAY4);
    @Deprecated
    public static final long GRAY5 = toLong(Color.GRAY5);
    @Deprecated
    public static final long GRAY6 = toLong(Color.GRAY6);
    @Deprecated
    public static final long GRAY7 = toLong(Color.GRAY7);
    @Deprecated
    public static final long GRAY8 = toLong(Color.GRAY8);
    @Deprecated
    public static final long WHITE = toLong(Color.WHITE);
    @Deprecated
    public static final long RED = toLong(Color.RED);
    @Deprecated
    public static final long PINK = toLong(Color.DB_PINK);
    @Deprecated
    public static final long ORANGE = toLong(Color.DB_ORANGE);
    @Deprecated
    public static final long YELLOW = toLong(Color.YELLOW);
    @Deprecated
    public static final long GREEN = toLong(Color.DB_GREEN);
    @Deprecated
    public static final long MAGENTA = toLong(Color.MAGENTA);
    @Deprecated
    public static final long CYAN = toLong(Color.CYAN);
    @Deprecated
    public static final long BLUE = toLong(Color.BLUE);
    @Deprecated
    public static final long NO_FORMATTING = toLong(Color.NO_FORMATTING);

    @Deprecated
    public static final class DistinctFormatter extends DBColorUtilImpl.DistinctFormatter {
    }
}
