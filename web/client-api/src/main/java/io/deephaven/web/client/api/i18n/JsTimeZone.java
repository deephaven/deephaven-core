package io.deephaven.web.client.api.i18n;

import com.google.gwt.core.client.GWT;
import com.google.gwt.i18n.client.TimeZone;
import com.google.gwt.i18n.client.constants.TimeZoneConstants;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

@JsType(name = "TimeZone", namespace = "dh.i18n")
public class JsTimeZone {

    public static JsTimeZone getTimeZone(String tzCode) {
        if (tzCode.equals("UTC") || tzCode.equals("GMT") || tzCode.equals("Etc/GMT")
            || tzCode.equals("Z")) {
            return new JsTimeZone(TimeZone.createTimeZone(0));
        }
        return new JsTimeZone(TimeZone.createTimeZone(getJsonForCode(tzCode)));
    }

    private static String getJsonForCode(String tzCode) {
        TimeZoneConstants constants = GWT.create(TimeZoneConstants.class);
        switch (tzCode) {
            case "Asia/Tokyo":
            case "JP":
            case "JST":
                return constants.asiaTokyo();
            case "Asia/Seoul":
            case "KR":
            case "KST":
                return constants.asiaSeoul();
            case "Asia/Hong_Kong":
            case "HK":
            case "HKT":
                return constants.asiaHongKong();
            case "Asia/Singapore":
            case "SG":
            case "SGT":
                return constants.asiaSingapore();
            case "Asia/Calcutta":
            case "Asia/Kolkata":
            case "IN":
            case "IST":
                return constants.asiaCalcutta();
            case "Asia/Taipei":
            case "TW":
                return constants.asiaTaipei();
            case "Europe/Amsterdam":
            case "NL":
                return constants.europeAmsterdam();
            case "Europe/Berlin":
            case "CE":
            case "CET":
            case "CEST":
                return constants.europeBerlin();
            case "Europe/London":
            case "LON":
            case "BST":
                return constants.europeLondon();
            case "Europe/Zurich":
            case "CH":
                return constants.europeZurich();
            case "America/Sao_Paulo":
            case "BT":
            case "BRST":
            case "BRT":
                return constants.americaSaoPaulo();
            case "America/St_Johns":
            case "NF":
            case "NST":
            case "NDT":
                return constants.americaStJohns();
            case "America/Halifax":
            case "AT":
            case "AST":
            case "ADT":
                return constants.americaHalifax();
            case "America/New_York":
            case "NY":
            case "ET":
            case "EST":
            case "EDT":
                return constants.americaNewYork();
            case "America/Chicago":
            case "MN":
            case "CT":
            case "CST":
            case "CDT":
                return constants.americaChicago();
            case "America/Denver":
            case "MT":
            case "MST":
            case "MDT":
                return constants.americaDenver();
            case "America/Los_Angeles":
            case "PT":
            case "PST":
            case "PDT":
                return constants.americaLosAngeles();
            case "America/Anchorage":
            case "AL":
            case "AKST":
            case "AKDT":
                return constants.americaAnchorage();
            case "Pacific/Honolulu":
            case "HI":
            case "HST":
            case "HDT":
                return constants.pacificHonolulu();
            case "Australia/Sydney":
            case "SYD":
            case "AEST":
            case "AEDT":
                return constants.australiaSydney();
            case "Europe/Moscow":
            case "MOS":
                return constants.europeMoscow();
            case "Asia/Shanghai":
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
