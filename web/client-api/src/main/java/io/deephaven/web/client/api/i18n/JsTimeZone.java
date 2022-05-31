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
    // Map of all time IDs to their JsTimeZone
    private static final Map<String, JsTimeZone> timeZones = new HashMap<>();

    static {
        initTimeZones();
    }

    public static JsTimeZone getTimeZone(String tzCode) {
        if (!timeZones.containsKey(tzCode)) {
            throw new IllegalArgumentException("Unsupported time zone " + tzCode);
        }

        return timeZones.get(tzCode);
    }

    private static void initTimeZones() {
        final TimeZoneConstants constants = GWT.create(TimeZoneConstants.class);

        // Add all the constants from TimeZoneConstants
        addJsonTimeZone(constants.africaAbidjan());
        addJsonTimeZone(constants.africaAccra());
        addJsonTimeZone(constants.africaAddisAbaba());
        addJsonTimeZone(constants.africaAlgiers());
        addJsonTimeZone(constants.africaAsmera());
        addJsonTimeZone(constants.africaBamako());
        addJsonTimeZone(constants.africaBangui());
        addJsonTimeZone(constants.africaBanjul());
        addJsonTimeZone(constants.africaBissau());
        addJsonTimeZone(constants.africaBlantyre());
        addJsonTimeZone(constants.africaBrazzaville());
        addJsonTimeZone(constants.africaBujumbura());
        addJsonTimeZone(constants.africaCairo());
        addJsonTimeZone(constants.africaCasablanca());
        addJsonTimeZone(constants.africaCeuta());
        addJsonTimeZone(constants.africaConakry());
        addJsonTimeZone(constants.africaDakar());
        addJsonTimeZone(constants.africaDaresSalaam());
        addJsonTimeZone(constants.africaDjibouti());
        addJsonTimeZone(constants.africaDouala());
        addJsonTimeZone(constants.africaElAaiun());
        addJsonTimeZone(constants.africaFreetown());
        addJsonTimeZone(constants.africaGaborone());
        addJsonTimeZone(constants.africaHarare());
        addJsonTimeZone(constants.africaJohannesburg());
        addJsonTimeZone(constants.africaKampala());
        addJsonTimeZone(constants.africaKhartoum());
        addJsonTimeZone(constants.africaKigali());
        addJsonTimeZone(constants.africaKinshasa());
        addJsonTimeZone(constants.africaLagos());
        addJsonTimeZone(constants.africaLibreville());
        addJsonTimeZone(constants.africaLome());
        addJsonTimeZone(constants.africaLuanda());
        addJsonTimeZone(constants.africaLubumbashi());
        addJsonTimeZone(constants.africaLusaka());
        addJsonTimeZone(constants.africaMalabo());
        addJsonTimeZone(constants.africaMaputo());
        addJsonTimeZone(constants.africaMaseru());
        addJsonTimeZone(constants.africaMbabane());
        addJsonTimeZone(constants.africaMogadishu());
        addJsonTimeZone(constants.africaMonrovia());
        addJsonTimeZone(constants.africaNairobi());
        addJsonTimeZone(constants.africaNdjamena());
        addJsonTimeZone(constants.africaNiamey());
        addJsonTimeZone(constants.africaNouakchott());
        addJsonTimeZone(constants.africaOuagadougou());
        addJsonTimeZone(constants.africaPortoNovo());
        addJsonTimeZone(constants.africaSaoTome());
        addJsonTimeZone(constants.africaTripoli());
        addJsonTimeZone(constants.africaTunis());
        addJsonTimeZone(constants.africaWindhoek());
        addJsonTimeZone(constants.americaAdak());
        addJsonTimeZone(constants.americaAnchorage());
        addJsonTimeZone(constants.americaAnguilla());
        addJsonTimeZone(constants.americaAntigua());
        addJsonTimeZone(constants.americaAraguaina());
        addJsonTimeZone(constants.americaArgentinaLaRioja());
        addJsonTimeZone(constants.americaArgentinaRioGallegos());
        addJsonTimeZone(constants.americaArgentinaSalta());
        addJsonTimeZone(constants.americaArgentinaSanJuan());
        addJsonTimeZone(constants.americaArgentinaSanLuis());
        addJsonTimeZone(constants.americaArgentinaTucuman());
        addJsonTimeZone(constants.americaArgentinaUshuaia());
        addJsonTimeZone(constants.americaAruba());
        addJsonTimeZone(constants.americaAsuncion());
        addJsonTimeZone(constants.americaBahia());
        addJsonTimeZone(constants.americaBahiaBanderas());
        addJsonTimeZone(constants.americaBarbados());
        addJsonTimeZone(constants.americaBelem());
        addJsonTimeZone(constants.americaBelize());
        addJsonTimeZone(constants.americaBlancSablon());
        addJsonTimeZone(constants.americaBoaVista());
        addJsonTimeZone(constants.americaBogota());
        addJsonTimeZone(constants.americaBoise());
        addJsonTimeZone(constants.americaBuenosAires());
        addJsonTimeZone(constants.americaCambridgeBay());
        addJsonTimeZone(constants.americaCampoGrande());
        addJsonTimeZone(constants.americaCancun());
        addJsonTimeZone(constants.americaCaracas());
        addJsonTimeZone(constants.americaCatamarca());
        addJsonTimeZone(constants.americaCayenne());
        addJsonTimeZone(constants.americaCayman());
        addJsonTimeZone(constants.americaChicago());
        addJsonTimeZone(constants.americaChihuahua());
        addJsonTimeZone(constants.americaCoralHarbour());
        addJsonTimeZone(constants.americaCordoba());
        addJsonTimeZone(constants.americaCostaRica());
        addJsonTimeZone(constants.americaCreston());
        addJsonTimeZone(constants.americaCuiaba());
        addJsonTimeZone(constants.americaCuracao());
        addJsonTimeZone(constants.americaDanmarkshavn());
        addJsonTimeZone(constants.americaDawson());
        addJsonTimeZone(constants.americaDawsonCreek());
        addJsonTimeZone(constants.americaDenver());
        addJsonTimeZone(constants.americaDetroit());
        addJsonTimeZone(constants.americaDominica());
        addJsonTimeZone(constants.americaEdmonton());
        addJsonTimeZone(constants.americaEirunepe());
        addJsonTimeZone(constants.americaElSalvador());
        addJsonTimeZone(constants.americaFortaleza());
        addJsonTimeZone(constants.americaGlaceBay());
        addJsonTimeZone(constants.americaGodthab());
        addJsonTimeZone(constants.americaGooseBay());
        addJsonTimeZone(constants.americaGrandTurk());
        addJsonTimeZone(constants.americaGrenada());
        addJsonTimeZone(constants.americaGuadeloupe());
        addJsonTimeZone(constants.americaGuatemala());
        addJsonTimeZone(constants.americaGuayaquil());
        addJsonTimeZone(constants.americaGuyana());
        addJsonTimeZone(constants.americaHalifax());
        addJsonTimeZone(constants.americaHavana());
        addJsonTimeZone(constants.americaHermosillo());
        addJsonTimeZone(constants.americaIndianaKnox());
        addJsonTimeZone(constants.americaIndianaMarengo());
        addJsonTimeZone(constants.americaIndianaPetersburg());
        addJsonTimeZone(constants.americaIndianapolis());
        addJsonTimeZone(constants.americaIndianaTellCity());
        addJsonTimeZone(constants.americaIndianaVevay());
        addJsonTimeZone(constants.americaIndianaVincennes());
        addJsonTimeZone(constants.americaIndianaWinamac());
        addJsonTimeZone(constants.americaInuvik());
        addJsonTimeZone(constants.americaIqaluit());
        addJsonTimeZone(constants.americaJamaica());
        addJsonTimeZone(constants.americaJujuy());
        addJsonTimeZone(constants.americaJuneau());
        addJsonTimeZone(constants.americaKentuckyMonticello());
        addJsonTimeZone(constants.americaKralendijk());
        addJsonTimeZone(constants.americaLaPaz());
        addJsonTimeZone(constants.americaLima());
        addJsonTimeZone(constants.americaLosAngeles());
        addJsonTimeZone(constants.americaLouisville());
        addJsonTimeZone(constants.americaLowerPrinces());
        addJsonTimeZone(constants.americaMaceio());
        addJsonTimeZone(constants.americaManagua());
        addJsonTimeZone(constants.americaManaus());
        addJsonTimeZone(constants.americaMarigot());
        addJsonTimeZone(constants.americaMartinique());
        addJsonTimeZone(constants.americaMatamoros());
        addJsonTimeZone(constants.americaMazatlan());
        addJsonTimeZone(constants.americaMendoza());
        addJsonTimeZone(constants.americaMenominee());
        addJsonTimeZone(constants.americaMerida());
        addJsonTimeZone(constants.americaMetlakatla());
        addJsonTimeZone(constants.americaMexicoCity());
        addJsonTimeZone(constants.americaMiquelon());
        addJsonTimeZone(constants.americaMoncton());
        addJsonTimeZone(constants.americaMonterrey());
        addJsonTimeZone(constants.americaMontevideo());
        addJsonTimeZone(constants.americaMontserrat());
        addJsonTimeZone(constants.americaNassau());
        addJsonTimeZone(constants.americaNewYork());
        addJsonTimeZone(constants.americaNipigon());
        addJsonTimeZone(constants.americaNome());
        addJsonTimeZone(constants.americaNoronha());
        addJsonTimeZone(constants.americaNorthDakotaBeulah());
        addJsonTimeZone(constants.americaNorthDakotaCenter());
        addJsonTimeZone(constants.americaNorthDakotaNewSalem());
        addJsonTimeZone(constants.americaOjinaga());
        addJsonTimeZone(constants.americaPanama());
        addJsonTimeZone(constants.americaPangnirtung());
        addJsonTimeZone(constants.americaParamaribo());
        addJsonTimeZone(constants.americaPhoenix());
        addJsonTimeZone(constants.americaPortauPrince());
        addJsonTimeZone(constants.americaPortofSpain());
        addJsonTimeZone(constants.americaPortoVelho());
        addJsonTimeZone(constants.americaPuertoRico());
        addJsonTimeZone(constants.americaRainyRiver());
        addJsonTimeZone(constants.americaRankinInlet());
        addJsonTimeZone(constants.americaRecife());
        addJsonTimeZone(constants.americaRegina());
        addJsonTimeZone(constants.americaResolute());
        addJsonTimeZone(constants.americaRioBranco());
        addJsonTimeZone(constants.americaSantaIsabel());
        addJsonTimeZone(constants.americaSantarem());
        addJsonTimeZone(constants.americaSantiago());
        addJsonTimeZone(constants.americaSantoDomingo());
        addJsonTimeZone(constants.americaSaoPaulo());
        addJsonTimeZone(constants.americaScoresbysund());
        addJsonTimeZone(constants.americaSitka());
        addJsonTimeZone(constants.americaStBarthelemy());
        addJsonTimeZone(constants.americaStJohns());
        addJsonTimeZone(constants.americaStKitts());
        addJsonTimeZone(constants.americaStLucia());
        addJsonTimeZone(constants.americaStThomas());
        addJsonTimeZone(constants.americaStVincent());
        addJsonTimeZone(constants.americaSwiftCurrent());
        addJsonTimeZone(constants.americaTegucigalpa());
        addJsonTimeZone(constants.americaThule());
        addJsonTimeZone(constants.americaThunderBay());
        addJsonTimeZone(constants.americaTijuana());
        addJsonTimeZone(constants.americaToronto());
        addJsonTimeZone(constants.americaTortola());
        addJsonTimeZone(constants.americaVancouver());
        addJsonTimeZone(constants.americaWhitehorse());
        addJsonTimeZone(constants.americaWinnipeg());
        addJsonTimeZone(constants.americaYakutat());
        addJsonTimeZone(constants.americaYellowknife());
        addJsonTimeZone(constants.antarcticaCasey());
        addJsonTimeZone(constants.antarcticaDavis());
        addJsonTimeZone(constants.antarcticaDumontDUrville());
        addJsonTimeZone(constants.antarcticaMacquarie());
        addJsonTimeZone(constants.antarcticaMawson());
        addJsonTimeZone(constants.antarcticaMcMurdo());
        addJsonTimeZone(constants.antarcticaPalmer());
        addJsonTimeZone(constants.antarcticaRothera());
        addJsonTimeZone(constants.antarcticaSyowa());
        addJsonTimeZone(constants.antarcticaVostok());
        addJsonTimeZone(constants.arcticLongyearbyen());
        addJsonTimeZone(constants.asiaAden());
        addJsonTimeZone(constants.asiaAlmaty());
        addJsonTimeZone(constants.asiaAmman());
        addJsonTimeZone(constants.asiaAnadyr());
        addJsonTimeZone(constants.asiaAqtau());
        addJsonTimeZone(constants.asiaAqtobe());
        addJsonTimeZone(constants.asiaAshgabat());
        addJsonTimeZone(constants.asiaBaghdad());
        addJsonTimeZone(constants.asiaBahrain());
        addJsonTimeZone(constants.asiaBaku());
        addJsonTimeZone(constants.asiaBangkok());
        addJsonTimeZone(constants.asiaBeirut());
        addJsonTimeZone(constants.asiaBishkek());
        addJsonTimeZone(constants.asiaBrunei());
        addJsonTimeZone(constants.asiaCalcutta());
        addJsonTimeZone(constants.asiaChoibalsan());
        addJsonTimeZone(constants.asiaChongqing());
        addJsonTimeZone(constants.asiaColombo());
        addJsonTimeZone(constants.asiaDamascus());
        addJsonTimeZone(constants.asiaDhaka());
        addJsonTimeZone(constants.asiaDili());
        addJsonTimeZone(constants.asiaDubai());
        addJsonTimeZone(constants.asiaDushanbe());
        addJsonTimeZone(constants.asiaGaza());
        addJsonTimeZone(constants.asiaHarbin());
        addJsonTimeZone(constants.asiaHongKong());
        addJsonTimeZone(constants.asiaHovd());
        addJsonTimeZone(constants.asiaIrkutsk());
        addJsonTimeZone(constants.asiaJakarta());
        addJsonTimeZone(constants.asiaJayapura());
        addJsonTimeZone(constants.asiaJerusalem());
        addJsonTimeZone(constants.asiaKabul());
        addJsonTimeZone(constants.asiaKamchatka());
        addJsonTimeZone(constants.asiaKarachi());
        addJsonTimeZone(constants.asiaKashgar());
        addJsonTimeZone(constants.asiaKatmandu());
        addJsonTimeZone(constants.asiaKrasnoyarsk());
        addJsonTimeZone(constants.asiaKualaLumpur());
        addJsonTimeZone(constants.asiaKuching());
        addJsonTimeZone(constants.asiaKuwait());
        addJsonTimeZone(constants.asiaMacau());
        addJsonTimeZone(constants.asiaMagadan());
        addJsonTimeZone(constants.asiaMakassar());
        addJsonTimeZone(constants.asiaManila());
        addJsonTimeZone(constants.asiaMuscat());
        addJsonTimeZone(constants.asiaNicosia());
        addJsonTimeZone(constants.asiaNovokuznetsk());
        addJsonTimeZone(constants.asiaNovosibirsk());
        addJsonTimeZone(constants.asiaOmsk());
        addJsonTimeZone(constants.asiaOral());
        addJsonTimeZone(constants.asiaPhnomPenh());
        addJsonTimeZone(constants.asiaPontianak());
        addJsonTimeZone(constants.asiaPyongyang());
        addJsonTimeZone(constants.asiaQatar());
        addJsonTimeZone(constants.asiaQyzylorda());
        addJsonTimeZone(constants.asiaRangoon());
        addJsonTimeZone(constants.asiaRiyadh());
        addJsonTimeZone(constants.asiaSaigon());
        addJsonTimeZone(constants.asiaSakhalin());
        addJsonTimeZone(constants.asiaSamarkand());
        addJsonTimeZone(constants.asiaSeoul());
        addJsonTimeZone(constants.asiaShanghai());
        addJsonTimeZone(constants.asiaSingapore());
        addJsonTimeZone(constants.asiaTaipei());
        addJsonTimeZone(constants.asiaTashkent());
        addJsonTimeZone(constants.asiaTbilisi());
        addJsonTimeZone(constants.asiaTehran());
        addJsonTimeZone(constants.asiaThimphu());
        addJsonTimeZone(constants.asiaTokyo());
        addJsonTimeZone(constants.asiaUlaanbaatar());
        addJsonTimeZone(constants.asiaUrumqi());
        addJsonTimeZone(constants.asiaVientiane());
        addJsonTimeZone(constants.asiaVladivostok());
        addJsonTimeZone(constants.asiaYakutsk());
        addJsonTimeZone(constants.asiaYekaterinburg());
        addJsonTimeZone(constants.asiaYerevan());
        addJsonTimeZone(constants.atlanticAzores());
        addJsonTimeZone(constants.atlanticBermuda());
        addJsonTimeZone(constants.atlanticCanary());
        addJsonTimeZone(constants.atlanticCapeVerde());
        addJsonTimeZone(constants.atlanticFaeroe());
        addJsonTimeZone(constants.atlanticMadeira());
        addJsonTimeZone(constants.atlanticReykjavik());
        addJsonTimeZone(constants.atlanticSouthGeorgia());
        addJsonTimeZone(constants.atlanticStanley());
        addJsonTimeZone(constants.atlanticStHelena());
        addJsonTimeZone(constants.australiaAdelaide());
        addJsonTimeZone(constants.australiaBrisbane());
        addJsonTimeZone(constants.australiaBrokenHill());
        addJsonTimeZone(constants.australiaCurrie());
        addJsonTimeZone(constants.australiaDarwin());
        addJsonTimeZone(constants.australiaEucla());
        addJsonTimeZone(constants.australiaHobart());
        addJsonTimeZone(constants.australiaLindeman());
        addJsonTimeZone(constants.australiaLordHowe());
        addJsonTimeZone(constants.australiaMelbourne());
        addJsonTimeZone(constants.australiaPerth());
        addJsonTimeZone(constants.australiaSydney());
        addJsonTimeZone(constants.cST6CDT());
        addJsonTimeZone(constants.eST5EDT());
        addJsonTimeZone(constants.europeAmsterdam());
        addJsonTimeZone(constants.europeAndorra());
        addJsonTimeZone(constants.europeAthens());
        addJsonTimeZone(constants.europeBelgrade());
        addJsonTimeZone(constants.europeBerlin());
        addJsonTimeZone(constants.europeBratislava());
        addJsonTimeZone(constants.europeBrussels());
        addJsonTimeZone(constants.europeBucharest());
        addJsonTimeZone(constants.europeBudapest());
        addJsonTimeZone(constants.europeChisinau());
        addJsonTimeZone(constants.europeCopenhagen());
        addJsonTimeZone(constants.europeDublin());
        addJsonTimeZone(constants.europeGibraltar());
        addJsonTimeZone(constants.europeGuernsey());
        addJsonTimeZone(constants.europeHelsinki());
        addJsonTimeZone(constants.europeIsleofMan());
        addJsonTimeZone(constants.europeIstanbul());
        addJsonTimeZone(constants.europeJersey());
        addJsonTimeZone(constants.europeKaliningrad());
        addJsonTimeZone(constants.europeKiev());
        addJsonTimeZone(constants.europeLisbon());
        addJsonTimeZone(constants.europeLjubljana());
        addJsonTimeZone(constants.europeLondon());
        addJsonTimeZone(constants.europeLuxembourg());
        addJsonTimeZone(constants.europeMadrid());
        addJsonTimeZone(constants.europeMalta());
        addJsonTimeZone(constants.europeMariehamn());
        addJsonTimeZone(constants.europeMinsk());
        addJsonTimeZone(constants.europeMonaco());
        addJsonTimeZone(constants.europeMoscow());
        addJsonTimeZone(constants.europeOslo());
        addJsonTimeZone(constants.europeParis());
        addJsonTimeZone(constants.europePodgorica());
        addJsonTimeZone(constants.europePrague());
        addJsonTimeZone(constants.europeRiga());
        addJsonTimeZone(constants.europeRome());
        addJsonTimeZone(constants.europeSamara());
        addJsonTimeZone(constants.europeSanMarino());
        addJsonTimeZone(constants.europeSarajevo());
        addJsonTimeZone(constants.europeSimferopol());
        addJsonTimeZone(constants.europeSkopje());
        addJsonTimeZone(constants.europeSofia());
        addJsonTimeZone(constants.europeStockholm());
        addJsonTimeZone(constants.europeTallinn());
        addJsonTimeZone(constants.europeTirane());
        addJsonTimeZone(constants.europeUzhgorod());
        addJsonTimeZone(constants.europeVaduz());
        addJsonTimeZone(constants.europeVatican());
        addJsonTimeZone(constants.europeVienna());
        addJsonTimeZone(constants.europeVilnius());
        addJsonTimeZone(constants.europeVolgograd());
        addJsonTimeZone(constants.europeWarsaw());
        addJsonTimeZone(constants.europeZagreb());
        addJsonTimeZone(constants.europeZaporozhye());
        addJsonTimeZone(constants.europeZurich());
        addJsonTimeZone(constants.indianAntananarivo());
        addJsonTimeZone(constants.indianChagos());
        addJsonTimeZone(constants.indianChristmas());
        addJsonTimeZone(constants.indianCocos());
        addJsonTimeZone(constants.indianComoro());
        addJsonTimeZone(constants.indianKerguelen());
        addJsonTimeZone(constants.indianMahe());
        addJsonTimeZone(constants.indianMaldives());
        addJsonTimeZone(constants.indianMauritius());
        addJsonTimeZone(constants.indianMayotte());
        addJsonTimeZone(constants.indianReunion());
        addJsonTimeZone(constants.mST7MDT());
        addJsonTimeZone(constants.pacificApia());
        addJsonTimeZone(constants.pacificAuckland());
        addJsonTimeZone(constants.pacificChatham());
        addJsonTimeZone(constants.pacificEaster());
        addJsonTimeZone(constants.pacificEfate());
        addJsonTimeZone(constants.pacificEnderbury());
        addJsonTimeZone(constants.pacificFakaofo());
        addJsonTimeZone(constants.pacificFiji());
        addJsonTimeZone(constants.pacificFunafuti());
        addJsonTimeZone(constants.pacificGalapagos());
        addJsonTimeZone(constants.pacificGambier());
        addJsonTimeZone(constants.pacificGuadalcanal());
        addJsonTimeZone(constants.pacificGuam());
        addJsonTimeZone(constants.pacificHonolulu());
        addJsonTimeZone(constants.pacificJohnston());
        addJsonTimeZone(constants.pacificKiritimati());
        addJsonTimeZone(constants.pacificKosrae());
        addJsonTimeZone(constants.pacificKwajalein());
        addJsonTimeZone(constants.pacificMajuro());
        addJsonTimeZone(constants.pacificMarquesas());
        addJsonTimeZone(constants.pacificMidway());
        addJsonTimeZone(constants.pacificNauru());
        addJsonTimeZone(constants.pacificNiue());
        addJsonTimeZone(constants.pacificNorfolk());
        addJsonTimeZone(constants.pacificNoumea());
        addJsonTimeZone(constants.pacificPagoPago());
        addJsonTimeZone(constants.pacificPalau());
        addJsonTimeZone(constants.pacificPitcairn());
        addJsonTimeZone(constants.pacificPonape());
        addJsonTimeZone(constants.pacificPortMoresby());
        addJsonTimeZone(constants.pacificRarotonga());
        addJsonTimeZone(constants.pacificSaipan());
        addJsonTimeZone(constants.pacificTahiti());
        addJsonTimeZone(constants.pacificTarawa());
        addJsonTimeZone(constants.pacificTongatapu());
        addJsonTimeZone(constants.pacificTruk());
        addJsonTimeZone(constants.pacificWake());
        addJsonTimeZone(constants.pacificWallis());
        addJsonTimeZone(constants.pST8PDT());

        // Now some mapping from other short strings to their codes
        addMapping("JP", "Asia/Tokyo");
        addMapping("JST", "Asia/Tokyo");
        addMapping("KR", "Asia/Seoul");
        addMapping("KST", "Asia/Seoul");
        addMapping("HK", "Asia/Hong_Kong");
        addMapping("HKT", "Asia/Hong_Kong");
        addMapping("SG", "Asia/Singapore");
        addMapping("SGT", "Asia/Singapore");
        addMapping("Asia/Kolkata", "Asia/Calcutta");
        addMapping("IN", "Asia/Calcutta");
        addMapping("IST", "Asia/Calcutta");
        addMapping("TW", "Asia/Taipei");
        addMapping("NL", "Europe/Amsterdam");
        addMapping("CE", "Europe/Berlin");
        addMapping("CES", "Europe/Berlin");
        addMapping("CEST", "Europe/Berlin");
        addMapping("LON", "Europe/London");
        addMapping("BST", "Europe/London");
        addMapping("CH", "Europe/Zurich");
        addMapping("BT", "America/Sao_Paulo");
        addMapping("BRST", "America/Sao_Paulo");
        addMapping("BRT", "America/Sao_Paulo");
        addMapping("NF", "America/St_Johns");
        addMapping("NST", "America/St_Johns");
        addMapping("NDT", "America/St_Johns");
        addMapping("AT", "America/Halifax");
        addMapping("AST", "America/Halifax");
        addMapping("ADT", "America/Halifax");
        addMapping("ET", "America/New_York");
        addMapping("EST", "America/New_York");
        addMapping("EDT", "America/New_York");
        addMapping("MN", "America/Chicago");
        addMapping("CT", "America/Chicago");
        addMapping("CST", "America/Chicago");
        addMapping("CDT", "America/Chicago");
        addMapping("MT", "America/Denver");
        addMapping("MST", "America/Denver");
        addMapping("MDT", "America/Denver");
        addMapping("PT", "America/Los_Angeles");
        addMapping("PST", "America/Los_Angeles");
        addMapping("PDT", "America/Los_Angeles");
        addMapping("AL", "America/Anchorage");
        addMapping("AKST", "America/Anchorage");
        addMapping("AKDT", "America/Anchorage");
        addMapping("HI", "Pacific/Honolulu");
        addMapping("HST", "Pacific/Honolulu");
        addMapping("HDT", "Pacific/Honolulu");
        addMapping("SYD", "Australia/Sydney");
        addMapping("AEST", "Australia/Sydney");
        addMapping("AEDT", "Australia/Sydney");
        addMapping("MOS", "Europe/Moscow");
        addMapping("SHG", "Asia/Shanghai");

        // Add GMT mappings
        TimeZone gmtTimeZone = TimeZone.createTimeZone(0);
        addMapping("UTC", gmtTimeZone);
        addMapping("GMT", gmtTimeZone);
        addMapping("Etc/GMT", gmtTimeZone);
        addMapping("Z", gmtTimeZone);
    }

    /**
     * Create a time zone from the JSON provided and add to the map with all IDs and known names
     * 
     * @param json The JSON to add the time zone for
     */
    private static void addJsonTimeZone(String json) {
        TimeZone tz = TimeZone.createTimeZone(json);
        addMapping(tz.getID(), tz);
    }

    /**
     * Add a time zone to the map. Throws if there already exists an entry for that key
     *
     * @param key The key to map from
     * @param tz The TimeZone to map to
     */
    private static void addMapping(String key, TimeZone tz) {
        addMapping(key, new JsTimeZone(tz));
    }

    /**
     * Add a time zone to the map. Throws if there already exists an entry for that key
     * 
     * @param key The key to map from
     * @param tz The JsTimeZone to map to
     */
    private static void addMapping(String key, JsTimeZone tz) {
        if (timeZones.containsKey(key)) {
            throw new IllegalArgumentException("Attempting to add the same key twice: " + key);
        }

        timeZones.put(key, tz);
    }

    /**
     * Map an existing time zone from the map to another key. Keeps both mappings.
     *
     * @param key The key to map it to
     * @param existingKey The existing key to map from
     */
    private static void addMapping(String key, String existingKey) {
        final JsTimeZone existing = timeZones.get(existingKey);
        if (existing == null) {
            throw new IllegalArgumentException("Existing key didn't exist " + existingKey);
        }

        addMapping(key, existing);
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
