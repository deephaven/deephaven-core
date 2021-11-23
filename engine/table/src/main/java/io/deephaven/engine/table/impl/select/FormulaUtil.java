/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;

public class FormulaUtil {

    public static List<String> getFormulaTokens(String formula, Collection<String> names) {
        List<String> result = new ArrayList<String>();
        for (String name : names) {
            int lastIndex = 0;
            while ((lastIndex = formula.indexOf(name, lastIndex)) != -1) {
                if (lastIndex > 0 && (Character.isLetter(formula.charAt(lastIndex - 1))
                        || formula.charAt(lastIndex - 1) == '_')) {
                    lastIndex++;
                    continue;
                }
                int nextChar = lastIndex + name.length();
                if (nextChar < formula.length() && (Character.isLetter(formula.charAt(nextChar))
                        || formula.charAt(nextChar) == '_' || Character.isDigit(formula.charAt(nextChar)))) {
                    lastIndex++;
                    continue;
                }
                result.add(name);
                break;
            }
        }
        return result;
    }

    public static String replaceFormulaTokens(String formula, String sourceToken, String destToken) {
        int lastIndex = 0;
        while (lastIndex < formula.length() && (lastIndex = formula.indexOf(sourceToken, lastIndex)) != -1) {
            if (lastIndex > 0
                    && (Character.isLetter(formula.charAt(lastIndex - 1)) || formula.charAt(lastIndex - 1) == '_')) {
                lastIndex++;
                continue;
            }
            int nextChar = lastIndex + sourceToken.length();
            if (nextChar < formula.length() && (Character.isLetter(formula.charAt(nextChar))
                    || formula.charAt(nextChar) == '_' || Character.isDigit(formula.charAt(nextChar)))) {
                lastIndex++;
                continue;
            }
            formula = formula.substring(0, lastIndex) + destToken + formula.substring(lastIndex + sourceToken.length());
            lastIndex += destToken.length();
        }
        return formula;
    }

}
