package io.deephaven.web.client.api.i18n;

import com.google.gwt.core.client.GWT;
import com.google.gwt.i18n.client.ConstantsWithLookup;
import com.google.gwt.i18n.client.TimeZone;
import com.google.gwt.i18n.client.constants.TimeZoneConstants;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;

@JsType(name = "TimeZone", namespace = "dh.i18n")
public class JsTimeZone {
    // ConstantsWithLookup is an explicit opt-in "keep every possible option compiled in, and I'll do runtime lookups, prevent the compiler from helping me"
    interface TimeZoneLookup extends TimeZoneConstants, ConstantsWithLookup {}

    private static final TimeZoneLookup constants = GWT.create(TimeZoneLookup.class);

    // Cache the time zones that are parsed so we don't need to recalculate them
    private static final Map<String, JsTimeZone> timeZoneCache = new HashMap<>();

    public static JsTimeZone getTimeZone(String tzCode) {
        return timeZoneCache.computeIfAbsent(tzCode, ignored -> createTimeZone(tzCode));
    }

    private static JsTimeZone createTimeZone(String tzCode) {
        if (tzCode.equals("UTC") || tzCode.equals("GMT") || tzCode.equals("Etc/GMT") || tzCode.equals("Z")) {
            return new JsTimeZone(TimeZone.createTimeZone(0));
        }
        return new JsTimeZone(TimeZone.createTimeZone(getJsonForCode(tzCode)));
    }

    private static String getJsonForCode(String tzCode) {
        if (tzCode.contains("/")) {
            String[] parts = tzCode.split("/");
            String key = parts[0].toLowerCase() + parts[1];
            try {
                return constants.getString(key);
            } catch (MissingResourceException e) {
                // ignore and continue
            }
        }
        switch (tzCode) {
            case "JP":
            case "JST":
                return constants.asiaTokyo();
            case "KR":
            case "KST":
                return constants.asiaSeoul();
            case "HK":
            case "HKT":
                return constants.asiaHongKong();
            case "SG":
            case "SGT":
                return constants.asiaSingapore();
            case "Asia/Kolkata":
            case "IN":
            case "IST":
                return constants.asiaCalcutta();
            case "TW":
                return constants.asiaTaipei();
            case "NL":
                return constants.europeAmsterdam();
            case "CE":
            case "CET":
            case "CEST":
                return constants.europeBerlin();
            case "LON":
            case "BST":
                return constants.europeLondon();
            case "CH":
                return constants.europeZurich();
            case "BT":
            case "BRST":
            case "BRT":
                return constants.americaSaoPaulo();
            case "NF":
            case "NST":
            case "NDT":
                return constants.americaStJohns();
            case "AT":
            case "AST":
            case "ADT":
                return constants.americaHalifax();
            case "NY":
            case "ET":
            case "EST":
            case "EDT":
                return constants.americaNewYork();
            case "MN":
            case "CT":
            case "CST":
            case "CDT":
                return constants.americaChicago();
            case "MT":
            case "MST":
            case "MDT":
                return constants.americaDenver();
            case "PT":
            case "PST":
            case "PDT":
                return constants.americaLosAngeles();
            case "AL":
            case "AKST":
            case "AKDT":
                return constants.americaAnchorage();
            case "HI":
            case "HST":
            case "HDT":
                return constants.pacificHonolulu();
            case "SYD":
            case "AEST":
            case "AEDT":
                return constants.australiaSydney();
            case "MOS":
                return constants.europeMoscow();
            case "SHG":
                return constants.asiaShanghai();
            default:
                throw new IllegalArgumentException("Unsupported time zone " + tzCode);
        }
    }

    @JsIgnore
    public static boolean needsDstAdjustment(String timeZoneAbbreviation) {
        if (timeZoneAbbreviation == null) {
            return true;
        }

        // If the abbreviation is in Standard Time or Daylight Time, then no adjustment is needed
        final String upper = timeZoneAbbreviation.toUpperCase();
        return !(upper.endsWith("ST") || upper.endsWith("DT"));
    }

    private final TimeZone tz;

    @JsIgnore
    public JsTimeZone(TimeZone tz) {
        this.tz = tz;
    }

    @JsIgnore
    public TimeZone unwrap() {
        return tz;
    }

    @JsProperty(name = "id")
    public String getID() {
        return tz.getID();
    }

    @JsProperty
    public int getStandardOffset() {
        return tz.getStandardOffset();
    }
}
