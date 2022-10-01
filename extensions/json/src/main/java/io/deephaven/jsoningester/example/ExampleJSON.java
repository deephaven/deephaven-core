package io.deephaven.jsoningester.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by rbasralian on 9/29/22
 */
public class ExampleJSON {

    /**
     * https://api.weather.gov/stations/KNYC/observations
     */
    public static final String observationsJson = "{\n" +
            "  \"@context\": [\n" +
            "    \"https://geojson.org/geojson-ld/geojson-context.jsonld\",\n" +
            "    {\n" +
            "      \"@version\": \"1.1\",\n" +
            "      \"wx\": \"https://api.weather.gov/ontology#\",\n" +
            "      \"s\": \"https://schema.org/\",\n" +
            "      \"geo\": \"http://www.opengis.net/ont/geosparql#\",\n" +
            "      \"unit\": \"http://codes.wmo.int/common/unit/\",\n" +
            "      \"@vocab\": \"https://api.weather.gov/ontology#\",\n" +
            "      \"geometry\": {\n" +
            "        \"@id\": \"s:GeoCoordinates\",\n" +
            "        \"@type\": \"geo:wktLiteral\"\n" +
            "      },\n" +
            "      \"city\": \"s:addressLocality\",\n" +
            "      \"state\": \"s:addressRegion\",\n" +
            "      \"distance\": {\n" +
            "        \"@id\": \"s:Distance\",\n" +
            "        \"@type\": \"s:QuantitativeValue\"\n" +
            "      },\n" +
            "      \"bearing\": {\n" +
            "        \"@type\": \"s:QuantitativeValue\"\n" +
            "      },\n" +
            "      \"value\": {\n" +
            "        \"@id\": \"s:value\"\n" +
            "      },\n" +
            "      \"unitCode\": {\n" +
            "        \"@id\": \"s:unitCode\",\n" +
            "        \"@type\": \"@id\"\n" +
            "      },\n" +
            "      \"forecastOffice\": {\n" +
            "        \"@type\": \"@id\"\n" +
            "      },\n" +
            "      \"forecastGridData\": {\n" +
            "        \"@type\": \"@id\"\n" +
            "      },\n" +
            "      \"publicZone\": {\n" +
            "        \"@type\": \"@id\"\n" +
            "      },\n" +
            "      \"county\": {\n" +
            "        \"@type\": \"@id\"\n" +
            "      }\n" +
            "    }\n" +
            "  ],\n" +
            "  \"id\": \"https://api.weather.gov/stations/KNYC/observations/2022-09-29T02:51:00+00:00\",\n" +
            "  \"type\": \"Feature\",\n" +
            "  \"geometry\": {\n" +
            "    \"type\": \"Point\",\n" +
            "    \"coordinates\": [\n" +
            "      -73.980000000000004,\n" +
            "      40.770000000000003\n" +
            "    ]\n" +
            "  },\n" +
            "  \"properties\": {\n" +
            "    \"@id\": \"https://api.weather.gov/stations/KNYC/observations/2022-09-29T02:51:00+00:00\",\n" +
            "    \"@type\": \"wx:ObservationStation\",\n" +
            "    \"elevation\": {\n" +
            "      \"unitCode\": \"wmoUnit:m\",\n" +
            "      \"value\": 27\n" +
            "    },\n" +
            "    \"station\": \"https://api.weather.gov/stations/KNYC\",\n" +
            "    \"timestamp\": \"2022-09-29T02:51:00+00:00\",\n" +
            "    \"rawMessage\": \"KNYC 290251Z AUTO VRB03KT 10SM FEW065 SCT085 17/10 A3026 RMK AO2 SLP237 T01720100 51018 $\",\n" +
            "    \"textDescription\": \"Partly Cloudy\",\n" +
            "    \"icon\": \"https://api.weather.gov/icons/land/night/sct?size=medium\",\n" +
            "    \"presentWeather\": [],\n" +
            "    \"temperature\": {\n" +
            "      \"unitCode\": \"wmoUnit:degC\",\n" +
            "      \"value\": 17.199999999999999,\n" +
            "      \"qualityControl\": \"V\"\n" +
            "    },\n" +
            "    \"dewpoint\": {\n" +
            "      \"unitCode\": \"wmoUnit:degC\",\n" +
            "      \"value\": 10,\n" +
            "      \"qualityControl\": \"V\"\n" +
            "    },\n" +
            "    \"windDirection\": {\n" +
            "      \"unitCode\": \"wmoUnit:degree_(angle)\",\n" +
            "      \"value\": null,\n" +
            "      \"qualityControl\": \"Z\"\n" +
            "    },\n" +
            "    \"windSpeed\": {\n" +
            "      \"unitCode\": \"wmoUnit:km_h-1\",\n" +
            "      \"value\": 5.4000000000000004,\n" +
            "      \"qualityControl\": \"V\"\n" +
            "    },\n" +
            "    \"windGust\": {\n" +
            "      \"unitCode\": \"wmoUnit:km_h-1\",\n" +
            "      \"value\": null,\n" +
            "      \"qualityControl\": \"Z\"\n" +
            "    },\n" +
            "    \"barometricPressure\": {\n" +
            "      \"unitCode\": \"wmoUnit:Pa\",\n" +
            "      \"value\": 102470,\n" +
            "      \"qualityControl\": \"V\"\n" +
            "    },\n" +
            "    \"seaLevelPressure\": {\n" +
            "      \"unitCode\": \"wmoUnit:Pa\",\n" +
            "      \"value\": 102370,\n" +
            "      \"qualityControl\": \"V\"\n" +
            "    },\n" +
            "    \"visibility\": {\n" +
            "      \"unitCode\": \"wmoUnit:m\",\n" +
            "      \"value\": 16090,\n" +
            "      \"qualityControl\": \"C\"\n" +
            "    },\n" +
            "    \"maxTemperatureLast24Hours\": {\n" +
            "      \"unitCode\": \"wmoUnit:degC\",\n" +
            "      \"value\": null\n" +
            "    },\n" +
            "    \"minTemperatureLast24Hours\": {\n" +
            "      \"unitCode\": \"wmoUnit:degC\",\n" +
            "      \"value\": null\n" +
            "    },\n" +
            "    \"precipitationLastHour\": {\n" +
            "      \"unitCode\": \"wmoUnit:m\",\n" +
            "      \"value\": null,\n" +
            "      \"qualityControl\": \"Z\"\n" +
            "    },\n" +
            "    \"precipitationLast3Hours\": {\n" +
            "      \"unitCode\": \"wmoUnit:m\",\n" +
            "      \"value\": null,\n" +
            "      \"qualityControl\": \"Z\"\n" +
            "    },\n" +
            "    \"precipitationLast6Hours\": {\n" +
            "      \"unitCode\": \"wmoUnit:m\",\n" +
            "      \"value\": null,\n" +
            "      \"qualityControl\": \"Z\"\n" +
            "    },\n" +
            "    \"relativeHumidity\": {\n" +
            "      \"unitCode\": \"wmoUnit:percent\",\n" +
            "      \"value\": 62.603057577541001,\n" +
            "      \"qualityControl\": \"V\"\n" +
            "    },\n" +
            "    \"windChill\": {\n" +
            "      \"unitCode\": \"wmoUnit:degC\",\n" +
            "      \"value\": null,\n" +
            "      \"qualityControl\": \"V\"\n" +
            "    },\n" +
            "    \"heatIndex\": {\n" +
            "      \"unitCode\": \"wmoUnit:degC\",\n" +
            "      \"value\": null,\n" +
            "      \"qualityControl\": \"V\"\n" +
            "    },\n" +
            "    \"cloudLayers\": [\n" +
            "      {\n" +
            "        \"base\": {\n" +
            "          \"unitCode\": \"wmoUnit:m\",\n" +
            "          \"value\": 1980\n" +
            "        },\n" +
            "        \"amount\": \"FEW\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"base\": {\n" +
            "          \"unitCode\": \"wmoUnit:m\",\n" +
            "          \"value\": 2590\n" +
            "        },\n" +
            "        \"amount\": \"SCT\"\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";

    public static String getFullObservationsJson(final String stationId) {

        final String url = "https://api.weather.gov/stations/" + stationId + "/observations";

        try {
            URL urlObj = new URL(url);
            String json;
            try (InputStream stream = urlObj.openStream()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                json = reader.lines().collect(java.util.stream.Collectors.joining(System.lineSeparator()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return json;
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public static final String fullObservationJsonShort = "{\n" +
            "  \"@context\": [\n" +
            "    \"https://geojson.org/geojson-ld/geojson-context.jsonld\",\n" +
            "    {\n" +
            "      \"@version\": \"1.1\",\n" +
            "      \"wx\": \"https://api.weather.gov/ontology#\",\n" +
            "      \"s\": \"https://schema.org/\",\n" +
            "      \"geo\": \"http://www.opengis.net/ont/geosparql#\",\n" +
            "      \"unit\": \"http://codes.wmo.int/common/unit/\",\n" +
            "      \"@vocab\": \"https://api.weather.gov/ontology#\",\n" +
            "      \"geometry\": {\n" +
            "        \"@id\": \"s:GeoCoordinates\",\n" +
            "        \"@type\": \"geo:wktLiteral\"\n" +
            "      },\n" +
            "      \"city\": \"s:addressLocality\",\n" +
            "      \"state\": \"s:addressRegion\",\n" +
            "      \"distance\": {\n" +
            "        \"@id\": \"s:Distance\",\n" +
            "        \"@type\": \"s:QuantitativeValue\"\n" +
            "      },\n" +
            "      \"bearing\": {\n" +
            "        \"@type\": \"s:QuantitativeValue\"\n" +
            "      },\n" +
            "      \"value\": {\n" +
            "        \"@id\": \"s:value\"\n" +
            "      },\n" +
            "      \"unitCode\": {\n" +
            "        \"@id\": \"s:unitCode\",\n" +
            "        \"@type\": \"@id\"\n" +
            "      },\n" +
            "      \"forecastOffice\": {\n" +
            "        \"@type\": \"@id\"\n" +
            "      },\n" +
            "      \"forecastGridData\": {\n" +
            "        \"@type\": \"@id\"\n" +
            "      },\n" +
            "      \"publicZone\": {\n" +
            "        \"@type\": \"@id\"\n" +
            "      },\n" +
            "      \"county\": {\n" +
            "        \"@type\": \"@id\"\n" +
            "      }\n" +
            "    }\n" +
            "  ],\n" +
            "  \"type\": \"FeatureCollection\",\n" +
            "  \"features\": [\n" +
            "    {\n" +
            "      \"id\": \"https://api.weather.gov/stations/KNYC/observations/2022-10-01T02:51:00+00:00\",\n" +
            "      \"type\": \"Feature\",\n" +
            "      \"geometry\": {\n" +
            "        \"type\": \"Point\",\n" +
            "        \"coordinates\": [\n" +
            "          -73.980000000000004,\n" +
            "          40.770000000000003\n" +
            "        ]\n" +
            "      },\n" +
            "      \"properties\": {\n" +
            "        \"@id\": \"https://api.weather.gov/stations/KNYC/observations/2022-10-01T02:51:00+00:00\",\n" +
            "        \"@type\": \"wx:ObservationStation\",\n" +
            "        \"elevation\": {\n" +
            "          \"unitCode\": \"wmoUnit:m\",\n" +
            "          \"value\": 27\n" +
            "        },\n" +
            "        \"station\": \"https://api.weather.gov/stations/KNYC\",\n" +
            "        \"timestamp\": \"2022-10-01T02:51:00+00:00\",\n" +
            "        \"rawMessage\": \"KNYC 010251Z AUTO 07007KT 10SM CLR 16/09 A3017 RMK AO2 SLP206 T01560094 58018 $\",\n" +
            "        \"textDescription\": \"Clear\",\n" +
            "        \"icon\": \"https://api.weather.gov/icons/land/night/skc?size=medium\",\n" +
            "        \"presentWeather\": [],\n" +
            "        \"temperature\": {\n" +
            "          \"unitCode\": \"wmoUnit:degC\",\n" +
            "          \"value\": 15.6,\n" +
            "          \"qualityControl\": \"V\"\n" +
            "        },\n" +
            "        \"dewpoint\": {\n" +
            "          \"unitCode\": \"wmoUnit:degC\",\n" +
            "          \"value\": 9.4000000000000004,\n" +
            "          \"qualityControl\": \"V\"\n" +
            "        },\n" +
            "        \"windDirection\": {\n" +
            "          \"unitCode\": \"wmoUnit:degree_(angle)\",\n" +
            "          \"value\": 70,\n" +
            "          \"qualityControl\": \"V\"\n" +
            "        },\n" +
            "        \"windSpeed\": {\n" +
            "          \"unitCode\": \"wmoUnit:km_h-1\",\n" +
            "          \"value\": 12.960000000000001,\n" +
            "          \"qualityControl\": \"V\"\n" +
            "        },\n" +
            "        \"windGust\": {\n" +
            "          \"unitCode\": \"wmoUnit:km_h-1\",\n" +
            "          \"value\": null,\n" +
            "          \"qualityControl\": \"Z\"\n" +
            "        },\n" +
            "        \"barometricPressure\": {\n" +
            "          \"unitCode\": \"wmoUnit:Pa\",\n" +
            "          \"value\": 102170,\n" +
            "          \"qualityControl\": \"V\"\n" +
            "        },\n" +
            "        \"seaLevelPressure\": {\n" +
            "          \"unitCode\": \"wmoUnit:Pa\",\n" +
            "          \"value\": 102060,\n" +
            "          \"qualityControl\": \"V\"\n" +
            "        },\n" +
            "        \"visibility\": {\n" +
            "          \"unitCode\": \"wmoUnit:m\",\n" +
            "          \"value\": 16090,\n" +
            "          \"qualityControl\": \"C\"\n" +
            "        },\n" +
            "        \"maxTemperatureLast24Hours\": {\n" +
            "          \"unitCode\": \"wmoUnit:degC\",\n" +
            "          \"value\": null\n" +
            "        },\n" +
            "        \"minTemperatureLast24Hours\": {\n" +
            "          \"unitCode\": \"wmoUnit:degC\",\n" +
            "          \"value\": null\n" +
            "        },\n" +
            "        \"precipitationLastHour\": {\n" +
            "          \"unitCode\": \"wmoUnit:m\",\n" +
            "          \"value\": null,\n" +
            "          \"qualityControl\": \"Z\"\n" +
            "        },\n" +
            "        \"precipitationLast3Hours\": {\n" +
            "          \"unitCode\": \"wmoUnit:m\",\n" +
            "          \"value\": null,\n" +
            "          \"qualityControl\": \"Z\"\n" +
            "        },\n" +
            "        \"precipitationLast6Hours\": {\n" +
            "          \"unitCode\": \"wmoUnit:m\",\n" +
            "          \"value\": null,\n" +
            "          \"qualityControl\": \"Z\"\n" +
            "        },\n" +
            "        \"relativeHumidity\": {\n" +
            "          \"unitCode\": \"wmoUnit:percent\",\n" +
            "          \"value\": 66.579992947826,\n" +
            "          \"qualityControl\": \"V\"\n" +
            "        },\n" +
            "        \"windChill\": {\n" +
            "          \"unitCode\": \"wmoUnit:degC\",\n" +
            "          \"value\": null,\n" +
            "          \"qualityControl\": \"V\"\n" +
            "        },\n" +
            "        \"heatIndex\": {\n" +
            "          \"unitCode\": \"wmoUnit:degC\",\n" +
            "          \"value\": null,\n" +
            "          \"qualityControl\": \"V\"\n" +
            "        },\n" +
            "        \"cloudLayers\": [\n" +
            "          {\n" +
            "            \"base\": {\n" +
            "              \"unitCode\": \"wmoUnit:m\",\n" +
            "              \"value\": null\n" +
            "            },\n" +
            "            \"amount\": \"CLR\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";
}
