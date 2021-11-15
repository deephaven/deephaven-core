package io.deephaven.web.client.api.i18n;

import com.google.gwt.core.client.GWT;
import com.google.gwt.i18n.client.TimeZone;
import com.google.gwt.i18n.client.constants.TimeZoneConstants;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import java.util.HashMap;
import java.util.Map;

@JsType(name = "TimeZone", namespace = "dh.i18n")
public class JsTimeZone {
    // Cache the time zones that are parsed so we don't need to recalculate them
    private static final Map<String, JsTimeZone> timeZoneCache = new HashMap<>();

    // Map of all time IDs to their json code string
    private static final Map<String, String> timeZones = new HashMap<>();

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
        initTimeZones();

        if (!timeZones.containsKey(tzCode)) {
            throw new IllegalArgumentException("Unsupported time zone " + tzCode);
        }

        return timeZones.get(tzCode);
    }

    private static void initTimeZones() {
        if (!timeZones.isEmpty()) {
            return;
        }

        final TimeZoneConstants constants = GWT.create(TimeZoneConstants.class);
        
        // Add all the constants from TimeZoneConstants
        addTimeZone(constants.africaAbidjan());
        addTimeZone(constants.africaAccra());
        addTimeZone(constants.africaAddisAbaba());
        addTimeZone(constants.africaAlgiers());
        addTimeZone(constants.africaAsmera());
        addTimeZone(constants.africaBamako());
        addTimeZone(constants.africaBangui());
        addTimeZone(constants.africaBanjul());
        addTimeZone(constants.africaBissau());
        addTimeZone(constants.africaBlantyre());
        addTimeZone(constants.africaBrazzaville());
        addTimeZone(constants.africaBujumbura());
        addTimeZone(constants.africaCairo());
        addTimeZone(constants.africaCasablanca());
        addTimeZone(constants.africaCeuta());
        addTimeZone(constants.africaConakry());
        addTimeZone(constants.africaDakar());
        addTimeZone(constants.africaDaresSalaam());
        addTimeZone(constants.africaDjibouti());
        addTimeZone(constants.africaDouala());
        addTimeZone(constants.africaElAaiun());
        addTimeZone(constants.africaFreetown());
        addTimeZone(constants.africaGaborone());
        addTimeZone(constants.africaHarare());
        addTimeZone(constants.africaJohannesburg());
        addTimeZone(constants.africaKampala());
        addTimeZone(constants.africaKhartoum());
        addTimeZone(constants.africaKigali());
        addTimeZone(constants.africaKinshasa());
        addTimeZone(constants.africaLagos());
        addTimeZone(constants.africaLibreville());
        addTimeZone(constants.africaLome());
        addTimeZone(constants.africaLuanda());
        addTimeZone(constants.africaLubumbashi());
        addTimeZone(constants.africaLusaka());
        addTimeZone(constants.africaMalabo());
        addTimeZone(constants.africaMaputo());
        addTimeZone(constants.africaMaseru());
        addTimeZone(constants.africaMbabane());
        addTimeZone(constants.africaMogadishu());
        addTimeZone(constants.africaMonrovia());
        addTimeZone(constants.africaNairobi());
        addTimeZone(constants.africaNdjamena());
        addTimeZone(constants.africaNiamey());
        addTimeZone(constants.africaNouakchott());
        addTimeZone(constants.africaOuagadougou());
        addTimeZone(constants.africaPortoNovo());
        addTimeZone(constants.africaSaoTome());
        addTimeZone(constants.africaTripoli());
        addTimeZone(constants.africaTunis());
        addTimeZone(constants.africaWindhoek());
        addTimeZone(constants.americaAdak());
        addTimeZone(constants.americaAnchorage());
        addTimeZone(constants.americaAnguilla());
        addTimeZone(constants.americaAntigua());
        addTimeZone(constants.americaAraguaina());
        addTimeZone(constants.americaArgentinaLaRioja());
        addTimeZone(constants.americaArgentinaRioGallegos());
        addTimeZone(constants.americaArgentinaSalta());
        addTimeZone(constants.americaArgentinaSanJuan());
        addTimeZone(constants.americaArgentinaSanLuis());
        addTimeZone(constants.americaArgentinaTucuman());
        addTimeZone(constants.americaArgentinaUshuaia());
        addTimeZone(constants.americaAruba());
        addTimeZone(constants.americaAsuncion());
        addTimeZone(constants.americaBahia());
        addTimeZone(constants.americaBahiaBanderas());
        addTimeZone(constants.americaBarbados());
        addTimeZone(constants.americaBelem());
        addTimeZone(constants.americaBelize());
        addTimeZone(constants.americaBlancSablon());
        addTimeZone(constants.americaBoaVista());
        addTimeZone(constants.americaBogota());
        addTimeZone(constants.americaBoise());
        addTimeZone(constants.americaBuenosAires());
        addTimeZone(constants.americaCambridgeBay());
        addTimeZone(constants.americaCampoGrande());
        addTimeZone(constants.americaCancun());
        addTimeZone(constants.americaCaracas());
        addTimeZone(constants.americaCatamarca());
        addTimeZone(constants.americaCayenne());
        addTimeZone(constants.americaCayman());
        addTimeZone(constants.americaChicago());
        addTimeZone(constants.americaChihuahua());
        addTimeZone(constants.americaCoralHarbour());
        addTimeZone(constants.americaCordoba());
        addTimeZone(constants.americaCostaRica());
        addTimeZone(constants.americaCreston());
        addTimeZone(constants.americaCuiaba());
        addTimeZone(constants.americaCuracao());
        addTimeZone(constants.americaDanmarkshavn());
        addTimeZone(constants.americaDawson());
        addTimeZone(constants.americaDawsonCreek());
        addTimeZone(constants.americaDenver());
        addTimeZone(constants.americaDetroit());
        addTimeZone(constants.americaDominica());
        addTimeZone(constants.americaEdmonton());
        addTimeZone(constants.americaEirunepe());
        addTimeZone(constants.americaElSalvador());
        addTimeZone(constants.americaFortaleza());
        addTimeZone(constants.americaGlaceBay());
        addTimeZone(constants.americaGodthab());
        addTimeZone(constants.americaGooseBay());
        addTimeZone(constants.americaGrandTurk());
        addTimeZone(constants.americaGrenada());
        addTimeZone(constants.americaGuadeloupe());
        addTimeZone(constants.americaGuatemala());
        addTimeZone(constants.americaGuayaquil());
        addTimeZone(constants.americaGuyana());
        addTimeZone(constants.americaHalifax());
        addTimeZone(constants.americaHavana());
        addTimeZone(constants.americaHermosillo());
        addTimeZone(constants.americaIndianaKnox());
        addTimeZone(constants.americaIndianaMarengo());
        addTimeZone(constants.americaIndianaPetersburg());
        addTimeZone(constants.americaIndianapolis());
        addTimeZone(constants.americaIndianaTellCity());
        addTimeZone(constants.americaIndianaVevay());
        addTimeZone(constants.americaIndianaVincennes());
        addTimeZone(constants.americaIndianaWinamac());
        addTimeZone(constants.americaInuvik());
        addTimeZone(constants.americaIqaluit());
        addTimeZone(constants.americaJamaica());
        addTimeZone(constants.americaJujuy());
        addTimeZone(constants.americaJuneau());
        addTimeZone(constants.americaKentuckyMonticello());
        addTimeZone(constants.americaKralendijk());
        addTimeZone(constants.americaLaPaz());
        addTimeZone(constants.americaLima());
        addTimeZone(constants.americaLosAngeles());
        addTimeZone(constants.americaLouisville());
        addTimeZone(constants.americaLowerPrinces());
        addTimeZone(constants.americaMaceio());
        addTimeZone(constants.americaManagua());
        addTimeZone(constants.americaManaus());
        addTimeZone(constants.americaMarigot());
        addTimeZone(constants.americaMartinique());
        addTimeZone(constants.americaMatamoros());
        addTimeZone(constants.americaMazatlan());
        addTimeZone(constants.americaMendoza());
        addTimeZone(constants.americaMenominee());
        addTimeZone(constants.americaMerida());
        addTimeZone(constants.americaMetlakatla());
        addTimeZone(constants.americaMexicoCity());
        addTimeZone(constants.americaMiquelon());
        addTimeZone(constants.americaMoncton());
        addTimeZone(constants.americaMonterrey());
        addTimeZone(constants.americaMontevideo());
        addTimeZone(constants.americaMontserrat());
        addTimeZone(constants.americaNassau());
        addTimeZone(constants.americaNewYork());
        addTimeZone(constants.americaNipigon());
        addTimeZone(constants.americaNome());
        addTimeZone(constants.americaNoronha());
        addTimeZone(constants.americaNorthDakotaBeulah());
        addTimeZone(constants.americaNorthDakotaCenter());
        addTimeZone(constants.americaNorthDakotaNewSalem());
        addTimeZone(constants.americaOjinaga());
        addTimeZone(constants.americaPanama());
        addTimeZone(constants.americaPangnirtung());
        addTimeZone(constants.americaParamaribo());
        addTimeZone(constants.americaPhoenix());
        addTimeZone(constants.americaPortauPrince());
        addTimeZone(constants.americaPortofSpain());
        addTimeZone(constants.americaPortoVelho());
        addTimeZone(constants.americaPuertoRico());
        addTimeZone(constants.americaRainyRiver());
        addTimeZone(constants.americaRankinInlet());
        addTimeZone(constants.americaRecife());
        addTimeZone(constants.americaRegina());
        addTimeZone(constants.americaResolute());
        addTimeZone(constants.americaRioBranco());
        addTimeZone(constants.americaSantaIsabel());
        addTimeZone(constants.americaSantarem());
        addTimeZone(constants.americaSantiago());
        addTimeZone(constants.americaSantoDomingo());
        addTimeZone(constants.americaSaoPaulo());
        addTimeZone(constants.americaScoresbysund());
        addTimeZone(constants.americaSitka());
        addTimeZone(constants.americaStBarthelemy());
        addTimeZone(constants.americaStJohns());
        addTimeZone(constants.americaStKitts());
        addTimeZone(constants.americaStLucia());
        addTimeZone(constants.americaStThomas());
        addTimeZone(constants.americaStVincent());
        addTimeZone(constants.americaSwiftCurrent());
        addTimeZone(constants.americaTegucigalpa());
        addTimeZone(constants.americaThule());
        addTimeZone(constants.americaThunderBay());
        addTimeZone(constants.americaTijuana());
        addTimeZone(constants.americaToronto());
        addTimeZone(constants.americaTortola());
        addTimeZone(constants.americaVancouver());
        addTimeZone(constants.americaWhitehorse());
        addTimeZone(constants.americaWinnipeg());
        addTimeZone(constants.americaYakutat());
        addTimeZone(constants.americaYellowknife());
        addTimeZone(constants.antarcticaCasey());
        addTimeZone(constants.antarcticaDavis());
        addTimeZone(constants.antarcticaDumontDUrville());
        addTimeZone(constants.antarcticaMacquarie());
        addTimeZone(constants.antarcticaMawson());
        addTimeZone(constants.antarcticaMcMurdo());
        addTimeZone(constants.antarcticaPalmer());
        addTimeZone(constants.antarcticaRothera());
        addTimeZone(constants.antarcticaSyowa());
        addTimeZone(constants.antarcticaVostok());
        addTimeZone(constants.arcticLongyearbyen());
        addTimeZone(constants.asiaAden());
        addTimeZone(constants.asiaAlmaty());
        addTimeZone(constants.asiaAmman());
        addTimeZone(constants.asiaAnadyr());
        addTimeZone(constants.asiaAqtau());
        addTimeZone(constants.asiaAqtobe());
        addTimeZone(constants.asiaAshgabat());
        addTimeZone(constants.asiaBaghdad());
        addTimeZone(constants.asiaBahrain());
        addTimeZone(constants.asiaBaku());
        addTimeZone(constants.asiaBangkok());
        addTimeZone(constants.asiaBeirut());
        addTimeZone(constants.asiaBishkek());
        addTimeZone(constants.asiaBrunei());
        addTimeZone(constants.asiaCalcutta());
        addTimeZone(constants.asiaChoibalsan());
        addTimeZone(constants.asiaChongqing());
        addTimeZone(constants.asiaColombo());
        addTimeZone(constants.asiaDamascus());
        addTimeZone(constants.asiaDhaka());
        addTimeZone(constants.asiaDili());
        addTimeZone(constants.asiaDubai());
        addTimeZone(constants.asiaDushanbe());
        addTimeZone(constants.asiaGaza());
        addTimeZone(constants.asiaHarbin());
        addTimeZone(constants.asiaHongKong());
        addTimeZone(constants.asiaHovd());
        addTimeZone(constants.asiaIrkutsk());
        addTimeZone(constants.asiaJakarta());
        addTimeZone(constants.asiaJayapura());
        addTimeZone(constants.asiaJerusalem());
        addTimeZone(constants.asiaKabul());
        addTimeZone(constants.asiaKamchatka());
        addTimeZone(constants.asiaKarachi());
        addTimeZone(constants.asiaKashgar());
        addTimeZone(constants.asiaKatmandu());
        addTimeZone(constants.asiaKrasnoyarsk());
        addTimeZone(constants.asiaKualaLumpur());
        addTimeZone(constants.asiaKuching());
        addTimeZone(constants.asiaKuwait());
        addTimeZone(constants.asiaMacau());
        addTimeZone(constants.asiaMagadan());
        addTimeZone(constants.asiaMakassar());
        addTimeZone(constants.asiaManila());
        addTimeZone(constants.asiaMuscat());
        addTimeZone(constants.asiaNicosia());
        addTimeZone(constants.asiaNovokuznetsk());
        addTimeZone(constants.asiaNovosibirsk());
        addTimeZone(constants.asiaOmsk());
        addTimeZone(constants.asiaOral());
        addTimeZone(constants.asiaPhnomPenh());
        addTimeZone(constants.asiaPontianak());
        addTimeZone(constants.asiaPyongyang());
        addTimeZone(constants.asiaQatar());
        addTimeZone(constants.asiaQyzylorda());
        addTimeZone(constants.asiaRangoon());
        addTimeZone(constants.asiaRiyadh());
        addTimeZone(constants.asiaSaigon());
        addTimeZone(constants.asiaSakhalin());
        addTimeZone(constants.asiaSamarkand());
        addTimeZone(constants.asiaSeoul());
        addTimeZone(constants.asiaShanghai());
        addTimeZone(constants.asiaSingapore());
        addTimeZone(constants.asiaTaipei());
        addTimeZone(constants.asiaTashkent());
        addTimeZone(constants.asiaTbilisi());
        addTimeZone(constants.asiaTehran());
        addTimeZone(constants.asiaThimphu());
        addTimeZone(constants.asiaTokyo());
        addTimeZone(constants.asiaUlaanbaatar());
        addTimeZone(constants.asiaUrumqi());
        addTimeZone(constants.asiaVientiane());
        addTimeZone(constants.asiaVladivostok());
        addTimeZone(constants.asiaYakutsk());
        addTimeZone(constants.asiaYekaterinburg());
        addTimeZone(constants.asiaYerevan());
        addTimeZone(constants.atlanticAzores());
        addTimeZone(constants.atlanticBermuda());
        addTimeZone(constants.atlanticCanary());
        addTimeZone(constants.atlanticCapeVerde());
        addTimeZone(constants.atlanticFaeroe());
        addTimeZone(constants.atlanticMadeira());
        addTimeZone(constants.atlanticReykjavik());
        addTimeZone(constants.atlanticSouthGeorgia());
        addTimeZone(constants.atlanticStanley());
        addTimeZone(constants.atlanticStHelena());
        addTimeZone(constants.australiaAdelaide());
        addTimeZone(constants.australiaBrisbane());
        addTimeZone(constants.australiaBrokenHill());
        addTimeZone(constants.australiaCurrie());
        addTimeZone(constants.australiaDarwin());
        addTimeZone(constants.australiaEucla());
        addTimeZone(constants.australiaHobart());
        addTimeZone(constants.australiaLindeman());
        addTimeZone(constants.australiaLordHowe());
        addTimeZone(constants.australiaMelbourne());
        addTimeZone(constants.australiaPerth());
        addTimeZone(constants.australiaSydney());
        addTimeZone(constants.cST6CDT());
        addTimeZone(constants.eST5EDT());
        addTimeZone(constants.europeAmsterdam());
        addTimeZone(constants.europeAndorra());
        addTimeZone(constants.europeAthens());
        addTimeZone(constants.europeBelgrade());
        addTimeZone(constants.europeBerlin());
        addTimeZone(constants.europeBratislava());
        addTimeZone(constants.europeBrussels());
        addTimeZone(constants.europeBucharest());
        addTimeZone(constants.europeBudapest());
        addTimeZone(constants.europeChisinau());
        addTimeZone(constants.europeCopenhagen());
        addTimeZone(constants.europeDublin());
        addTimeZone(constants.europeGibraltar());
        addTimeZone(constants.europeGuernsey());
        addTimeZone(constants.europeHelsinki());
        addTimeZone(constants.europeIsleofMan());
        addTimeZone(constants.europeIstanbul());
        addTimeZone(constants.europeJersey());
        addTimeZone(constants.europeKaliningrad());
        addTimeZone(constants.europeKiev());
        addTimeZone(constants.europeLisbon());
        addTimeZone(constants.europeLjubljana());
        addTimeZone(constants.europeLondon());
        addTimeZone(constants.europeLuxembourg());
        addTimeZone(constants.europeMadrid());
        addTimeZone(constants.europeMalta());
        addTimeZone(constants.europeMariehamn());
        addTimeZone(constants.europeMinsk());
        addTimeZone(constants.europeMonaco());
        addTimeZone(constants.europeMoscow());
        addTimeZone(constants.europeOslo());
        addTimeZone(constants.europeParis());
        addTimeZone(constants.europePodgorica());
        addTimeZone(constants.europePrague());
        addTimeZone(constants.europeRiga());
        addTimeZone(constants.europeRome());
        addTimeZone(constants.europeSamara());
        addTimeZone(constants.europeSanMarino());
        addTimeZone(constants.europeSarajevo());
        addTimeZone(constants.europeSimferopol());
        addTimeZone(constants.europeSkopje());
        addTimeZone(constants.europeSofia());
        addTimeZone(constants.europeStockholm());
        addTimeZone(constants.europeTallinn());
        addTimeZone(constants.europeTirane());
        addTimeZone(constants.europeUzhgorod());
        addTimeZone(constants.europeVaduz());
        addTimeZone(constants.europeVatican());
        addTimeZone(constants.europeVienna());
        addTimeZone(constants.europeVilnius());
        addTimeZone(constants.europeVolgograd());
        addTimeZone(constants.europeWarsaw());
        addTimeZone(constants.europeZagreb());
        addTimeZone(constants.europeZaporozhye());
        addTimeZone(constants.europeZurich());
        addTimeZone(constants.indianAntananarivo());
        addTimeZone(constants.indianChagos());
        addTimeZone(constants.indianChristmas());
        addTimeZone(constants.indianCocos());
        addTimeZone(constants.indianComoro());
        addTimeZone(constants.indianKerguelen());
        addTimeZone(constants.indianMahe());
        addTimeZone(constants.indianMaldives());
        addTimeZone(constants.indianMauritius());
        addTimeZone(constants.indianMayotte());
        addTimeZone(constants.indianReunion());
        addTimeZone(constants.mST7MDT());
        addTimeZone(constants.pacificApia());
        addTimeZone(constants.pacificAuckland());
        addTimeZone(constants.pacificChatham());
        addTimeZone(constants.pacificEaster());
        addTimeZone(constants.pacificEfate());
        addTimeZone(constants.pacificEnderbury());
        addTimeZone(constants.pacificFakaofo());
        addTimeZone(constants.pacificFiji());
        addTimeZone(constants.pacificFunafuti());
        addTimeZone(constants.pacificGalapagos());
        addTimeZone(constants.pacificGambier());
        addTimeZone(constants.pacificGuadalcanal());
        addTimeZone(constants.pacificGuam());
        addTimeZone(constants.pacificHonolulu());
        addTimeZone(constants.pacificJohnston());
        addTimeZone(constants.pacificKiritimati());
        addTimeZone(constants.pacificKosrae());
        addTimeZone(constants.pacificKwajalein());
        addTimeZone(constants.pacificMajuro());
        addTimeZone(constants.pacificMarquesas());
        addTimeZone(constants.pacificMidway());
        addTimeZone(constants.pacificNauru());
        addTimeZone(constants.pacificNiue());
        addTimeZone(constants.pacificNorfolk());
        addTimeZone(constants.pacificNoumea());
        addTimeZone(constants.pacificPagoPago());
        addTimeZone(constants.pacificPalau());
        addTimeZone(constants.pacificPitcairn());
        addTimeZone(constants.pacificPonape());
        addTimeZone(constants.pacificPortMoresby());
        addTimeZone(constants.pacificRarotonga());
        addTimeZone(constants.pacificSaipan());
        addTimeZone(constants.pacificTahiti());
        addTimeZone(constants.pacificTarawa());
        addTimeZone(constants.pacificTongatapu());
        addTimeZone(constants.pacificTruk());
        addTimeZone(constants.pacificWake());
        addTimeZone(constants.pacificWallis());
        addTimeZone(constants.pST8PDT());

        // Now some mapping from other short strings to their codes
        timeZones.put("JP", constants.asiaTokyo());
        timeZones.put("JST", constants.asiaTokyo());
        timeZones.put("KR", constants.asiaSeoul());
        timeZones.put("KST", constants.asiaSeoul());
        timeZones.put("HK", constants.asiaHongKong());
        timeZones.put("HKT", constants.asiaHongKong());
        timeZones.put("SG", constants.asiaSingapore());
        timeZones.put("SGT", constants.asiaSingapore());
        timeZones.put("Asia/Kolkata", constants.asiaCalcutta());
        timeZones.put("IN", constants.asiaCalcutta());
        timeZones.put("IST", constants.asiaCalcutta());
        timeZones.put("TW", constants.asiaTaipei());
        timeZones.put("NL", constants.europeAmsterdam());
        timeZones.put("CE", constants.europeBerlin());
        timeZones.put("CES", constants.europeBerlin());
        timeZones.put("CEST", constants.europeBerlin());
        timeZones.put("LON", constants.europeLondon());
        timeZones.put("BST", constants.europeLondon());
        timeZones.put("CH", constants.europeZurich());
        timeZones.put("BT", constants.americaSaoPaulo());
        timeZones.put("BRST", constants.americaSaoPaulo());
        timeZones.put("BRT", constants.americaSaoPaulo());
        timeZones.put("NF", constants.americaStJohns());
        timeZones.put("NST", constants.americaStJohns());
        timeZones.put("NDT", constants.americaStJohns());
        timeZones.put("AT", constants.americaHalifax());
        timeZones.put("AST", constants.americaHalifax());
        timeZones.put("ADT", constants.americaHalifax());
        timeZones.put("ET", constants.americaNewYork());
        timeZones.put("EST", constants.americaNewYork());
        timeZones.put("EDT", constants.americaNewYork());
        timeZones.put("MN", constants.americaChicago());
        timeZones.put("CT", constants.americaChicago());
        timeZones.put("CST", constants.americaChicago());
        timeZones.put("CDT", constants.americaChicago());
        timeZones.put("MT", constants.americaDenver());
        timeZones.put("MST", constants.americaDenver());
        timeZones.put("MDT", constants.americaDenver());
        timeZones.put("PT", constants.americaLosAngeles());
        timeZones.put("PST", constants.americaLosAngeles());
        timeZones.put("PDT", constants.americaLosAngeles());
        timeZones.put("AL", constants.americaAnchorage());
        timeZones.put("AKST", constants.americaAnchorage());
        timeZones.put("AKDT", constants.americaAnchorage());
        timeZones.put("HI", constants.pacificHonolulu());
        timeZones.put("HST", constants.pacificHonolulu());
        timeZones.put("HDT", constants.pacificHonolulu());
        timeZones.put("SYD", constants.australiaSydney());
        timeZones.put("AEST", constants.australiaSydney());
        timeZones.put("AEDT", constants.australiaSydney());
        timeZones.put("MOS", constants.europeMoscow());
        timeZones.put("SHG", constants.asiaShanghai());
    }

    /**
     * Add a mapping to the timezones map
     * @param json The JSON to add the time zone for
     */
    private static void addTimeZone(String json) {
        TimeZone tz = TimeZone.createTimeZone(json);
        timeZones.put(tz.getID(), json);
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
