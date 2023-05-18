package io.deephaven.time;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.StringReader;
import java.time.ZoneId;
import java.util.*;

//TODO: rename
//TODO: document
//TODO: unit test
//TODO: config file
public class TimeZoneAliases {

//NY,America/New_York
//ET,America/New_York
//MN,America/Chicago
//CT,America/Chicago
//MT,America/Denver
//PT,America/Los_Angeles
//HI,Pacific/Honolulu
//BT,America/Sao_Paulo
//KR,Asia/Seoul
//HK,Asia/Hong_Kong
//JP,Asia/Tokyo
//AT,Canada/Atlantic
//NF,Canada/Newfoundland
//AL,America/Anchorage
//IN,Asia/Kolkata
//CE,Europe/Berlin
//SG,Asia/Singapore
//LON,Europe/London
//MOS,Europe/Moscow
//SHG,Asia/Shanghai
//CH,Europe/Zurich
//NL,Europe/Amsterdam
//TW,Asia/Taipei
//SYD,Australia/Sydney
//UTC,UTC

    //TODO fix default
    public static final ZoneId TZ_DEFAULT = ZoneId.of("America/New_York"); //TODO ZoneId.systemDefault()?

    private static final Map<String, String> aliasMap = new HashMap<>();
    private static final Map<String, String> reverseAliasMap = new HashMap<>();
    private static final Map<String,ZoneId> allZones = new HashMap<>();

    static {
        final String file =
                "NY,America/New_York\n\n" +
//                        "ET,America/New_York\n\n" +
                        "MN,America/Chicago\n" +
//                        "CT,America/Chicago\n" +
                        "MT,America/Denver\n" +
                        "PT,America/Los_Angeles\n" +
                        "HI,Pacific/Honolulu\n" +
                        "BT,America/Sao_Paulo\n" +
                        "KR,Asia/Seoul\n" +
                        "HK,Asia/Hong_Kong\n" +
                        "JP,Asia/Tokyo\n" +
                        "AT,Canada/Atlantic\n" +
                        "NF,Canada/Newfoundland\n" +
                        "AL,America/Anchorage\n" +
                        "IN,Asia/Kolkata\n" +
                        "CE,Europe/Berlin\n" +
                        "SG,Asia/Singapore\n" +
                        "LON,Europe/London\n" +
                        "MOS,Europe/Moscow\n" +
                        "SHG,Asia/Shanghai\n" +
                        "CH,Europe/Zurich\n" +
                        "NL,Europe/Amsterdam\n" +
                        "TW,Asia/Taipei\n" +
                        "SYD,Australia/Sydney\n" +
                        "UTC,UTC\n";

        //TODO: Don't use SHORT_IDS! anywhere
        //TODO: make optional
//        aliasMap.putAll(ZoneId.SHORT_IDS);
//
//        for (Map.Entry<String, String> entry : aliasMap.entrySet()) {
//            reverseAliasMap.put(entry.getValue(), entry.getKey());
//        }


        //TODO: file reader
//        try (final BufferedReader br = new BufferedReader(new FileReader("book.csv"))) {
        try (final BufferedReader br = new BufferedReader(new StringReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length == 2) {
                    if (reverseAliasMap.containsKey(values[1])) {
                        throw new IllegalArgumentException("Alias for time zone is already present: time_zone=" + values[1]);
                    }

                    aliasMap.put(values[0], values[1]);
                    reverseAliasMap.put(values[1], values[0]);
                } else if (values.length > 2) {
                    throw new IllegalArgumentException("Line contains too many values: file=" + file + " values=" + Arrays.toString(values));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to load time zone alias file: " + file, e);
        }

        for (String tz : ZoneId.getAvailableZoneIds()) {
            allZones.put(tz, ZoneId.of(tz));
        }

        for (Map.Entry<String, String> e : aliasMap.entrySet()) {
            allZones.put(e.getKey(), ZoneId.of(e.getValue()));
        }
    }


//    private static final Map<String, ZoneId> cache = loadMapping();
//
//    private static Map<String,ZoneId> loadMapping() {
//        final String file =
//        "NY,America/New_York" +
//        "ET,America/New_York" +
//        "MN,America/Chicago" +
//        "CT,America/Chicago" +
//        "MT,America/Denver" +
//        "PT,America/Los_Angeles" +
//        "HI,Pacific/Honolulu" +
//        "BT,America/Sao_Paulo" +
//        "KR,Asia/Seoul" +
//        "HK,Asia/Hong_Kong" +
//        "JP,Asia/Tokyo" +
//        "AT,Canada/Atlantic" +
//        "NF,Canada/Newfoundland" +
//        "AL,America/Anchorage" +
//        "IN,Asia/Kolkata" +
//        "CE,Europe/Berlin" +
//        "SG,Asia/Singapore" +
//        "LON,Europe/London" +
//        "MOS,Europe/Moscow" +
//        "SHG,Asia/Shanghai" +
//        "CH,Europe/Zurich" +
//        "NL,Europe/Amsterdam" +
//        "TW,Asia/Taipei" +
//        "SYD,Australia/Sydney" +
//        "UTC,UTC";
//
//        final Map<String, ZoneId> rst = new HashMap<>();
//
//        //TODO: file reader
////        try (final BufferedReader br = new BufferedReader(new FileReader("book.csv"))) {
//        try (final BufferedReader br = new BufferedReader(new StringReader(file))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                String[] values = line.split(",");
//                if(values.length == 2) {
//                    rst.put(values[0], ZoneId.of(values[1]));
//                } else if (values.length >2){
//                    throw new IllegalArgumentException("Line contains too many values: file=" + file + " values=" + Arrays.toString(values));
//                }
//            }
//        } catch (Exception e) {
//            throw new RuntimeException("Unable to load time zone file: " + file, e);
//        }
//
//        return rst;
//    }

    @NotNull
    public static ZoneId zone(@NotNull final String timeZone) {
        return ZoneId.of(timeZone, aliasMap);
    }

    @NotNull
    public static String name(@NotNull final ZoneId timeZone) {
        return reverseAliasMap.getOrDefault(timeZone.getId(), timeZone.getId());
    }

    @NotNull
    public static Map<String, ZoneId> getAllZones() {
        return allZones;
    }

    //TODO handle capitalization

    //    //TODO fix default
//    public static ZoneId getDefault() {
//        return ZoneId.of("America/New_York");
//    }

}
