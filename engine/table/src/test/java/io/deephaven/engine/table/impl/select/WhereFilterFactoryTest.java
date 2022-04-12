/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.table.impl.RefreshingTableTestCase;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;

public class WhereFilterFactoryTest extends RefreshingTableTestCase {

    public void testIn() {
        assertEquals(MatchFilter.class, WhereFilterFactory.getExpression("Opra in oprasOfInterest").getClass());
    }

    public void testSourceColumn() {
        SelectColumn[] column = SelectColumnFactory.getExpressions("Parity0=Parity");
        assertEquals(SwitchColumn.class, column[0].getClass());
    }

    public void testInComplex() {
        QueryScope.setScope(new QueryScope.StandaloneImpl());

        assertEquals(MatchFilter.class, WhereFilterFactory.getExpression("Opra in opra1, opra2, opra3").getClass());
        QueryScope.addParam("pmExpiry", "World");
        assertEquals(MatchFilter.class, WhereFilterFactory.getExpression("amExpiry = pmExpiry").getClass());

        QueryScope.addParam("amExpiry", "Hello");
        QueryScope.addParam("pmExpiry", "World");

        WhereFilter filter = WhereFilterFactory.getExpression("Maturity in amExpiry,pmExpiry , \"AGAIN\"  ");
        assertEquals(MatchFilter.class, filter.getClass());
        TableDefinition tableDef = new TableDefinition(Collections.singletonList((Class) String.class),
                Collections.singletonList("Maturity"));

        filter.init(tableDef);
        Object[] values = ((MatchFilter) filter).getValues();
        System.out.println(Arrays.toString(values));
        assertEquals("Hello", values[0]);
        assertEquals("World", values[1]);
        assertEquals("AGAIN", values[2]);

        filter = WhereFilterFactory.getExpression("Maturity in amExpiry,1 , \"AGAIN\"  ");
        assertEquals(MatchFilter.class, filter.getClass());
        tableDef = new TableDefinition(Collections.singletonList((Class) String.class),
                Collections.singletonList("Maturity"));

        try {
            filter.init(tableDef);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Failed to convert literal value <1> for column \"Maturity\" of type java.lang.String");
        }
    }

    public void testCharMatch() {
        QueryScope.setScope(new QueryScope.StandaloneImpl());
        QueryScope.addParam("theChar", 'C');

        final Table tt = TableTools.newTable(TableTools.charCol("AChar", 'A', 'B', 'C', '\0', '\''));

        final String[] filterStrings = new String[] {
                "AChar = 'A'",
                "AChar != 'A'",
                "AChar == '''",
                "AChar in 'A', 'C', '''",
                "AChar not in 'A', 'C', '''",
                "AChar = \"A\"",
                "AChar != \"A\"",
                "AChar == \"'\"",
                "AChar in \"A\", \"C\", \"'\"",
                "AChar not in \"A\", \"C\", \"'\"",
                "AChar = theChar",
                "AChar == theChar",
                "AChar != theChar"
        };
        final RowSet[] expectedResults = new RowSet[] {
                TstUtils.i(0),
                TstUtils.ir(1, 4),
                TstUtils.i(4),
                TstUtils.i(0, 2, 4),
                TstUtils.i(1, 3),
                TstUtils.i(0),
                TstUtils.ir(1, 4),
                TstUtils.i(4),
                TstUtils.i(0, 2, 4),
                TstUtils.i(1, 3),
                TstUtils.i(2),
                TstUtils.i(2),
                TstUtils.i(0, 1, 3, 4)
        };

        for (int ii = 0; ii < filterStrings.length; ++ii) {
            final WhereFilter filter = WhereFilterFactory.getExpression(filterStrings[ii]);
            filter.init(tt.getDefinition());
            assertEquals(MatchFilter.class, filter.getClass());
            try (final RowSet selection = tt.getRowSet().copy();
                    final RowSet filtered = filter.filter(selection, tt.getRowSet(), tt, false);
                    final RowSet expected = expectedResults[ii]) {
                assertEquals(expected, filtered);
            } catch (Exception e) {
                System.err.println("Failed for test case: " + filterStrings[ii]);
                throw e;
            }
        }
    }

    public void testIcase() {
        Table t = TableTools
                .newTable(TableTools.col("Opra", "opra1", "opra2", "opra3", "Opra1", "Opra2", "Opra3", "Opra4", null));

        WhereFilter f = WhereFilterFactory.getExpression("Opra in `opra1`, `opra2`, `opra3`,`opra4`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        RowSet idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(3, idx.size());

        f = WhereFilterFactory.getExpression("Opra in `opra1`, `Opra4`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(2, idx.size());

        f = WhereFilterFactory.getExpression("Opra not in `opra2`, `opra4`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(7, idx.size());

        f = WhereFilterFactory.getExpression("Opra icase in `opra1`, `opra2`, `opra3`, `opra4`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(7, idx.size());

        f = WhereFilterFactory.getExpression("Opra icase not in `opra1`, `opra2`, `opra3`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(2, idx.size());

        f = WhereFilterFactory.getExpression("Opra icase not in `opra2`, `opra4`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(5, idx.size());

        f = WhereFilterFactory.getExpression("Opra icase in `opra1`, `opra2`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(4, idx.size());

        t = TstUtils.testRefreshingTable(
                TstUtils.c("Opra", "opra1", "opra2", "opra3", "Opra1", "Opra2", "Opra3", "Opra4", null, "OpRa5"),
                TstUtils.cG("Food", "Apple", "Orange", "bacon", "laffa", "pOtato", "carroT", "WafflE", null, "Apple"));

        f = WhereFilterFactory.getExpression("Food icase in `apple`, `orange`, `bacon`,`LAFFA`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(5, idx.size());

        f = WhereFilterFactory.getExpression("Food in `apple`, `orange`, `bacon`,`LAFFA`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(1, idx.size());

        f = WhereFilterFactory.getExpression("Food in `Apple`, `orange`, `bacon`,`LAFFA`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(3, idx.size());

        f = WhereFilterFactory.getExpression("Food icase not in `apple`, `orange`, `bacon`,`LAFFA`");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(4, idx.size());
    }

    public void testBigIn() {
        final WhereFilter f = WhereFilterFactory.getExpression(
                "USym in `A`,`AA`,`AABA`,`AAL`,`AAL1`,`AAN`,`AAOI`,`AAP`,`AAPL`,`AAT`,`AAWW`,`AAXN`,`AB`,`ABB`,`ABBV`,`ABC`,`ABEO`,`ABEV`,`ABG`,`ABMD`,`ABT`,`ABT1`,`ABX`,`ACAD`,`ACC`,`ACGL`,`ACH`,`ACHC`,`ACIA`,`ACIW`,`ACLS`,`ACM`,`ACN`,`ACOR`,`ACWI`,`ADBE`,`ADI`,`ADM`,`ADMP`,`ADNT`,`ADP`,`ADS`,`ADSK`,`ADTN`,`AEE`,`AEIS`,`AEM`,`AEO`,`AEP`,`AER`,`AERI`,`AES`,`AET`,`AFG`,`AFL`,`AFSI`,`AG`,`AGCO`,`AGIO`,`AGN`,`AGNC`,`AGO`,`AGQ`,`AGX`,`AHL`,`AHT`,`AI`,`AIG`,`AIMC`,`AIMT`,`AINV`,`AIR`,`AIV`,`AIZ`,`AJG`,`AJRD`,`AKAM`,`AKAO`,`AKCA`,`AKR`,`AKRX`,`AKS`,`AL`,`ALB`,`ALE`,`ALGN`,`ALGT`,`ALK`,`ALKS`,`ALL`,`ALLE`,`ALLT`,`ALLY`,`ALNY`,`ALRM`,`ALSN`,`ALV`,`ALV1`,`ALXN`,`AM`,`AMAG`,`AMAT`,`AMBA`,`AMBC`,`AMC`,`AMCX`,`AMD`,`AME`,`AMED`,`AMG`,`AMGN`,`AMJ`,`AMKR`,`AMLP`,`AMN`,`AMP`,`AMRN`,`AMRX`,`AMT`,`AMTD`,`AMWD`,`AMX`,`AMZN`,`AN`,`ANAB`,`ANDE`,`ANDX`,`ANET`,`ANF`,`ANGI`,`ANIK`,`ANSS`,`ANTM`,`AOBC`,`AON`,`AOS`,`AOSL`,`APA`,`APAM`,`APC`,`APD`,`APH`,`APO`,`APOG`,`APPN`,`APRN`,`APTI`,`APTV`,`APTV1`,`APU`,`AR`,`ARAY`,`ARCB`,`ARCC`,`ARCH`,`ARCO`,`ARE`,`ARII`,`ARLP`,`ARLP1`,`ARMK`,`ARNC`,`ARNC1`,`ARNC2`,`AROC`,`AROC1`,`ARRS`,`ARW`,`ARWR`,`ASB`,`ASGN`,`ASH`,`ASHR`,`ASML`,`ASNA`,`ASPS`,`ASRT`,`ASTE`,`ATEN`,`ATGE`,`ATH`,`ATHM`,`ATHN`,`ATI`,`ATO`,`ATR`,`ATSG`,`ATU`,`ATUS`,`ATVI`,`AU`,`AUY`,`AVA`,`AVAV`,`AVB`,`AVD`,`AVEO`,`AVGO`,`AVLR`,`AVNS`,`AVP`,`AVT`,`AVX`,`AVY`,`AVYA`,`AWI`,`AWK`,`AX`,`AXE`,`AXL`,`AXP`,`AXS`,`AXTA`,`AY`,`AYI`,`AYR`,`AYX`,`AZN`,`AZO`,`AZPN`,`AZUL`,`AZZ`,`BA`,`BABA`,`BAC`,`BAH`,`BAM`,`BANC`,`BAP`,`BAX`,`BB`,`BBBY`,`BBD`,`BBD1`,`BBL`,`BBT`,`BBW`,`BBY`,`BC`,`BCC`,`BCE`,`BCEI`,`BCO`,`BCOR`,`BCOV`,`BCS`,`BDC`,`BDN`,`BDX`,`BEAT`,`BECN`,`BEL`,`BEN`,`BERY`,`BG`,`BGCP`,`BGFV`,`BGG`,`BGNE`,`BGS`,`BHC`,`BHE`,`BHF`,`BHGE`,`BHGE1`,`BHP`,`BHR`,`BIB`,`BID`,`BIDU`,`BIG`,`BIIB`,`BIIB1`,`BILI`,`BITA`,`BJRI`,`BK`,`BKD`,`BKE`,`BKH`,`BKI`,`BKNG`,`BKS`,`BKU`,`BL`,`BLCM`,`BLD`,`BLDR`,`BLK`,`BLKB`,`BLL`,`BLMN`,`BLUE`,`BMA`,`BMI`,`BMO`,`BMRN`,`BMS`,`BMY`,`BNFT`,`BNS`,`BOH`,`BOJA`,`BOKF`,`BOKF1`,`BOOT`,`BOX`,`BP`,`BPI`,`BPL`,`BPMC`,`BPOP`,`BPT`,`BPY`,`BPY1`,`BR`,`BREW`,`BRFS`,`BRKB`,`BRKL`,`BRKR`,`BRKS`,`BRS`,`BRX`,`BSX`,`BTI`,`BTI1`,`BUD`,`BURL`,`BVN`,`BWA`,`BWXT`,`BX`,`BXP`,`BXS`,`BYD`,`BZH`,`BZUN`,`C`,`CA`,`CACC`,`CACI`,`CAG`,`CAH`,`CAI`,`CAKE`,`CAL`,`CALL`,`CALM`,`CALX`,`CAMP`,`CAR`,`CARA`,`CARB`,`CARG`,`CARS`,`CASA`,`CASY`,`CAT`,`CATM`,`CATO`,`CATY`,`CB`,`CBIO`,`CBL`,`CBOE`,`CBPO`,`CBRE`,`CBRL`,`CBS`,`CC`,`CCE`,`CCI`,`CCJ`,`CCK`,`CCL`,`CCMP`,`CCOI`,`CDAY`,`CDE`,`CDEV`,`CDK`,`CDNS`,`CDW`,`CE`,`CECO`,`CELG`,`CENT`,`CENX`,`CEO`,`CEQP`,`CERN`,`CERS`,`CEVA`,`CF`,`CFG`,`CFR`,`CFX`,`CG`,`CGNX`,`CHD`,`CHDN`,`CHE`,`CHEF`,`CHFC`,`CHGG`,`CHH`,`CHK`,`CHKP`,`CHL`,`CHRW`,`CHS`,`CHSP`,`CHTR`,`CHU`,`CHUY`,`CI`,`CIEN`,`CIM`,`CINF`,`CISN`,`CIT`,`CL`,`CLB`,`CLDR`,`CLF`,`CLFD`,`CLGX`,`CLH`,`CLI`,`CLMT`,`CLNE`,`CLNY`,`CLR`,`CLS`,`CLVS`,`CLX`,`CM`,`CMA`,`CMC`,`CMCM`,`CMCSA`,`CME`,`CMG`,`CMI`,`CMO`,`CMP`,`CMPR`,`CMS`,`CMTL`,`CNC`,`CNDT`,`CNHI`,`CNI`,`CNK`,`CNO`,`CNP`,`CNQ`,`CNX`,`CNX1`,`COF`,`COG`,`COHR`,`COHU`,`COHU1`,`COL`,`COLM`,`COMM`,`CONE`,`CONN`,`COOP1`,`COP`,`COR`,`CORE`,`CORT`,`COST`,`COT`,`COTY`,`COUP`,`CP`,`CPA`,`CPB`,`CPE`,`CPLG1`,`CPRT`,`CPT`,`CQP`,`CR`,`CRAY`,`CRC`,`CRCM`,`CREE`,`CRI`,`CRL`,`CRM`,`CRM1`,`CROX`,`CRR`,`CRS`,`CRSP`,`CRTO`,`CRUS`,`CRY`,`CRZO`,`CS`,`CSCO`,`CSFL`,`CSFL1`,`CSGS`,`CSII`,`CSIQ`,`CSOD`,`CSTE`,`CSTM`,`CSV`,`CSX`,`CTAS`,`CTB`,`CTL`,`CTL1`,`CTRL`,`CTRN`,`CTRP`,`CTSH`,`CTXS`,`CTXS1`,`CUBE`,`CVA`,`CVE`,`CVG`,`CVGW`,`CVI`,`CVIA1`,`CVLT`,`CVNA`,`CVRR`,`CVS`,`CVX`,`CWEN`,`CWH`,`CX`,`CX2`,`CXO`,`CXO1`,`CXP`,`CXW`,`CY`,`CYBR`,`CYD`,`CYH`,`CYOU`,`CZR`,`CZZ`,`D`,`DAL`,`DAN`,`DAR`,`DATA`,`DB`,`DBA`,`DBC`,`DBD`,`DBX`,`DCI`,`DCP`,`DDD`,`DDM`,`DDR1`,`DDR2`,`DDS`,`DE`,`DECK`,`DEI`,`DENN`,`DEO`,`DERM`,`DF`,`DFRG`,`DFS`,`DG`,`DGX`,`DHI`,`DHR`,`DHT`,`DIA`,`DIG`,`DIN`,`DIOD`,`DIS`,`DISCA`,`DISCK`,`DISH`,`DK`,`DKL`,`DKS`,`DLB`,`DLPH`,`DLR`,`DLTH`,`DLTR`,`DLX`,`DMRC`,`DNB`,`DNKN`,`DNOW`,`DNR`,`DO`,`DOCU`,`DOMO`,`DOOR`,`DORM`,`DOV`,`DOV1`,`DOX`,`DPLO`,`DPZ`,`DQ`,`DRE`,`DRH`,`DRI`,`DRQ`,`DSW`,`DSX`,`DTE`,`DUG`,`DUK`,`DUST`,`DVA`,`DVAX`,`DVMT`,`DVN`,`DVY`,`DWDP`,`DWDP1`,`DXC`,`DXC1`,`DXCM`,`DXD`,`DXD1`,`DXJ`,`DXPE`,`DY`,`E`,`EA`,`EAT`,`EBAY`,`EBIX`,`EBS`,`EBSB`,`ECA`,`ECHO`,`ECL`,`ECOL`,`ECOM`,`ECPG`,`ECYT`,`ED`,`EDC`,`EDIT`,`EDR`,`EDU`,`EDZ`,`EDZ1`,`EE`,`EEB`,`EEFT`,`EEM`,`EEP`,`EEV`,`EEV1`,`EFA`,`EFC`,`EFII`,`EFX`,`EGBN`,`EGHT`,`EGL`,`EGN`,`EGO`,`EGOV`,`EGP`,`EGRX`,`EHC`,`EIGI`,`EIX`,`EL`,`ELF`,`ELLI`,`ELY`,`EME`,`EMES`,`EMN`,`EMR`,`ENB`,`ENB1`,`ENDP`,`ENLC`,`ENLK`,`ENR`,`ENS`,`ENTG`,`ENV`,`ENVA`,`EOG`,`EPAM`,`EPAY`,`EPC`,`EPD`,`EPI`,`EPR`,`EQC`,`EQIX`,`EQM`,`EQM1`,`EQNR`,`EQR`,`EQT`,`EQT1`,`ERF`,`ERI`,`ERIC`,`ERJ`,`EROS`,`ERX`,`ERY`,`ERY1`,`ES`,`ESIO`,`ESL`,`ESND`,`ESNT`,`ESPR`,`ESRT`,`ESRX`,`ESS`,`ESV`,`ET`,`ET1`,`ET2`,`ETFC`,`ETH`,`ETM`,`ETN`,`ETP`,`ETP1`,`ETR`,`ETSY`,`EUO`,`EV`,`EVH`,`EVR`,`EVRG`,`EVRG1`,`EVTC`,`EW`,`EWA`,`EWBC`,`EWC`,`EWD`,`EWG`,`EWH`,`EWI`,`EWJ`,`EWM`,`EWQ`,`EWS`,`EWT`,`EWU`,`EWU1`,`EWW`,`EWY`,`EWZ`,`EXAS`,`EXC`,`EXEL`,`EXP`,`EXPD`,`EXPE`,`EXPR`,`EXR`,`EXTN`,`EXTR`,`EYE`,`EZA`,`EZPW`,`EZU`,`F`,`FANG`,`FARO`,`FAS`,`FAST`,`FAZ`,`FB`,`FBC`,`FBHS`,`FC`,`FCAU`,`FCAU3`,`FCEA`,`FCFS`,`FCN`,`FCX`,`FDC`,`FDP`,`FDS`,`FDX`,`FE`,`FELE`,`FET`,`FEYE`,`FEZ`,`FFBC`,`FFIV`,`FGEN`,`FGP`,`FHN`,`FI`,`FICO`,`FII`,`FIS`,`FISV`,`FIT`,`FITB`,`FIVE`,`FIVN`,`FIX`,`FIZZ`,`FL`,`FLDM`,`FLEX`,`FLIR`,`FLO`,`FLOW`,`FLR`,`FLS`,`FLT`,`FLXN`,`FLY`,`FMC`,`FMX`,`FN`,`FND`,`FNF`,`FNKO`,`FNSR`,`FNV`,`FOE`,`FOLD`,`FOMX`,`FOR`,`FORM`,`FOSL`,`FOX`,`FOXA`,`FPRX`,`FR`,`FRAN`,`FRC`,`FRED`,`FRFHF`,`FRGI`,`FRO`,`FRPT`,`FRSH`,`FRT`,`FSCT`,`FSLR`,`FSS`,`FTI`,`FTK`,`FTNT`,`FTR`,`FTR1`,`FTV`,`FUL`,`FULT`,`FUN`,`FWONA`,`FWRD`,`FXA`,`FXB`,`FXC`,`FXE`,`FXI`,`FXP`,`FXP1`,`FXY`,`G`,`GATX`,`GBT`,`GBX`,`GCAP`,`GCI`,`GCO`,`GD`,`GDDY`,`GDOT`,`GDS`,`GDX`,`GDXJ`,`GE`,`GEF`,`GEL`,`GEO`,`GEO1`,`GERN`,`GES`,`GG`,`GGAL`,`GGB`,`GGG`,`GHL`,`GIII`,`GIL`,`GILD`,`GILT`,`GIS`,`GLD`,`GLIB1`,`GLIBA`,`GLL`,`GLNG`,`GLOB`,`GLOG`,`GLPI`,`GLUU`,`GLW`,`GM`,`GME`,`GMED`,`GMLP`,`GNC`,`GNRC`,`GNTX`,`GNW`,`GOGL`,`GOGO`,`GOLD`,`GOOG`,`GOOGL`,`GOOS`,`GOV`,`GPC`,`GPI`,`GPK`,`GPN`,`GPOR`,`GPRE`,`GPRO`,`GPS`,`GRA`,`GREK`,`GRMN`,`GRPN`,`GRUB`,`GS`,`GSK`,`GSKY`,`GSM`,`GSVC`,`GT`,`GTLS`,`GTN`,`GTS`,`GTT`,`GTXI`,`GVA`,`GVA1`,`GWB`,`GWPH`,`GWR`,`GWRE`,`GWW`,`H`,`HA`,`HABT`,`HACK`,`HAIN`,`HAL`,`HALO`,`HAS`,`HBAN`,`HBI`,`HCA`,`HCI`,`HCLP`,`HCP`,`HCP1`,`HD`,`HDB`,`HDP`,`HDS`,`HE`,`HEAR`,`HEES`,`HELE`,`HEP`,`HES`,`HFC`,`HGV`,`HHC`,`HI`,`HIBB`,`HIFR`,`HIG`,`HII`,`HIIQ`,`HIMX`,`HIW`,`HLF`,`HLI`,`HLT`,`HLT1`,`HLX`,`HMC`,`HMHC`,`HMNY1`,`HMSY`,`HOG`,`HOLI`,`HOLX`,`HOMB`,`HOME`,`HON`,`HON1`,`HOPE`,`HOS`,`HOV`,`HP`,`HPE`,`HPE1`,`HPE2`,`HPP`,`HPQ`,`HPR`,`HPT`,`HQY`,`HR`,`HRB`,`HRI`,`HRL`,`HRS`,`HSBC`,`HSC`,`HSIC`,`HST`,`HSY`,`HT`,`HTA`,`HTH`,`HTHT`,`HTZ`,`HUBG`,`HUBS`,`HUM`,`HUN`,`HUYA`,`HXL`,`HYG`,`HZNP`,`HZO`,`I`,`IAC`,`IAU`,`IBB`,`IBKR`,`IBM`,`IBN`,`IBN1`,`ICE`,`ICHR`,`ICLR`,`ICON`,`ICPT`,`ICUI`,`IDA`,`IDCC`,`IDT`,`IDTI`,`IEO`,`IEP`,`IEX`,`IFF`,`IGT`,`IIVI`,`ILF`,`ILMN`,`ILPT`,`IMAX`,`IMGN`,`IMKTA`,`IMMR`,`IMMU`,`IMO`,`IMOS`,`IMPV`,`INAP`,`INCY`,`INFN`,`INFO`,`INFY`,`INFY1`,`ING`,`INGN`,`INGR`,`INN`,`INOV`,`INSM`,`INSY`,`INT`,`INTC`,`INTU`,`INVA`,`INXN`,`IONS`,`IP`,`IPG`,`IPGP`,`IPHI`,`IPHS`,`IQ`,`IQV`,`IR`,`IRBT`,`IRDM`,`IRM`,`ISRG`,`IT`,`ITB`,`ITCI`,`ITG`,`ITRI`,`ITT`,`ITUB`,`ITW`,`IVAC`,`IVR`,`IVV`,`IVZ`,`IWB`,`IWD`,`IWM`,`IWN`,`IWO`,`IYE`,`IYF`,`IYM`,`IYR`,`IYT`,`JACK`,`JAZZ`,`JBHT`,`JBL`,`JBLU`,`JBSS`,`JBT`,`JCI`,`JCI3`,`JCOM`,`JCP`,`JD`,`JEC`,`JEF`,`JJOFF`,`JKHY`,`JKS`,`JLL`,`JMEI`,`JNJ`,`JNK`,`JNPR`,`JNUG`,`JOE`,`JPM`,`JWN`,`K`,`KALU`,`KANG`,`KAR`,`KB`,`KBE`,`KBH`,`KBR`,`KDP`,`KDP1`,`KEM`,`KEX`,`KEY`,`KEYS`,`KEYW`,`KFRC`,`KGC`,`KHC`,`KIE`,`KIM`,`KIRK`,`KKR`,`KL`,`KLAC`,`KLIC`,`KLXI1`,`KMB`,`KMI`,`KMT`,`KMX`,`KN`,`KNDI`,`KNL`,`KNX`,`KNX1`,`KO`,`KODK`,`KOL`,`KOP`,`KORS`,`KR`,`KRA`,`KRC`,`KRE`,`KRO`,`KS`,`KSS`,`KSU`,`KW`,`KWR`,`L`,`LABL`,`LABU`,`LAD`,`LADR`,`LAMR`,`LAZ`,`LB`,`LBRDK`,`LBTYA`,`LBTYK`,`LC`,`LCII`,`LDOS`,`LE`,`LEA`,`LECO`,`LEG`,`LEN`,`LEN1`,`LFC`,`LFIN`,`LFUS`,`LGFA`,`LGFA1`,`LGIH`,`LGND`,`LH`,`LHCG`,`LHCG1`,`LHO`,`LII`,`LITE`,`LIVN`,`LKQ`,`LL`,`LLL`,`LLY`,`LM`,`LMNR`,`LMNX`,`LMT`,`LN`,`LNC`,`LNG`,`LNG1`,`LNN`,`LNT`,`LOCO`,`LOGI`,`LOGM`,`LOPE`,`LORL`,`LOW`,`LOXO`,`LPI`,`LPL`,`LPLA`,`LPNT`,`LPSN`,`LPT`,`LPX`,`LQD`,`LQDT`,`LRCX`,`LRN`,`LSCC`,`LSI`,`LSTR`,`LSXMA`,`LTC`,`LULU`,`LUV`,`LVS`,`LW`,`LXP`,`LYB`,`LYG`,`LYV`,`LZB`,`M`,`MA`,`MAA`,`MAC`,`MAIN`,`MAN`,`MANH`,`MANT`,`MANU`,`MAR`,`MAS`,`MAT`,`MATX`,`MB`,`MBFI`,`MBI`,`MBT`,`MBUU`,`MCD`,`MCF`,`MCHP`,`MCK`,`MCO`,`MCRI`,`MCS`,`MCY`,`MD`,`MDB`,`MDC`,`MDCO`,`MDGL`,`MDLZ`,`MDP`,`MDR`,`MDR1`,`MDR2`,`MDRX`,`MDSO`,`MDT`,`MDU`,`MDXG`,`MDY`,`MED`,`MEI`,`MELI`,`MEOH`,`MET`,`MET1`,`MFA`,`MFC`,`MFGP`,`MGA`,`MGI`,`MGLN`,`MGM`,`MGPI`,`MHK`,`MHLD`,`MHO`,`MIC`,`MIDD`,`MIK`,`MIME`,`MINI`,`MITL`,`MKC`,`MKSI`,`MKTX`,`MLCO`,`MLHR`,`MLM`,`MLNT`,`MLNX`,`MMC`,`MMLP`,`MMM`,`MMP`,`MMS`,`MMSI`,`MMYT`,`MNK`,`MNRO`,`MNST`,`MNTA`,`MO`,`MODN`,`MOH`,`MOMO`,`MOO`,`MOS`,`MOV`,`MPC`,`MPC1`,`MPC2`,`MPLX`,`MPW`,`MPWR`,`MRC`,`MRCY`,`MRK`,`MRO`,`MRTX`,`MRVL`,`MRVL1`,`MS`,`MSCI`,`MSFT`,`MSGN`,`MSI`,`MSM`,`MSTR`,`MT`,`MT1`,`MTB`,`MTCH`,`MTD`,`MTDR`,`MTG`,`MTH`,`MTN`,`MTOR`,`MTSI`,`MTW`,`MTZ`,`MU`,`MUR`,`MUSA`,`MX`,`MXIM`,`MXL`,`MXWL`,`MYGN`,`MYL`,`MZOR`,`NANO`,`NAT`,`NATI`,`NAV`,`NAVG`,`NAVI`,`NBIX`,`NBL`,`NBR`,`NCLH`,`NCMI`,`NCR`,`NCS`,`NDAQ`,`NDLS`,`NDSN`,`NDX`,`NE`,`NEE`,`NEM`,`NEU`,`NEWM`,`NEWR`,`NFBK`,`NFG`,`NFLX`,`NFX`,`NGD`,`NGG`,`NGL`,`NGVC`,`NHI`,`NHTC`,`NI`,`NJR`,`NKE`,`NKTR`,`NLS`,`NLSN`,`NLY`,`NLY1`,`NNN`,`NOC`,`NOK`,`NOV`,`NOVT`,`NOW`,`NPO`,`NPTN`,`NRG`,`NRP`,`NRZ`,`NS`,`NS1`,`NSC`,`NSIT`,`NSP`,`NTAP`,`NTCT`,`NTES`,`NTGR`,`NTLA`,`NTNX`,`NTR`,`NTR1`,`NTR2`,`NTRI`,`NTRS`,`NUAN`,`NUE`,`NUGT`,`NUS`,`NVCR`,`NVDA`,`NVGS`,`NVMI`,`NVO`,`NVRO`,`NVS`,`NVT`,`NWE`,`NWL`,`NWN`,`NWPX`,`NWS`,`NWSA`,`NXGN`,`NXPI`,`NXST`,`NXTM`,`NYCB`,`NYRT`,`NYRT3`,`NYT`,`O`,`OAK`,`OAS`,`OC`,`OCLR`,`OCN`,`ODFL`,`ODP`,`OEC`,`OEF`,`OFC`,`OFG`,`OGE`,`OGS`,`OHI`,`OI`,`OIH`,`OII`,`OILNF`,`OIS`,`OKE`,`OKTA`,`OLED`,`OLLI`,`OLN`,`OMC`,`OMCL`,`OMER`,`OMF`,`OMI`,`ON`,`ONCE`,`ONDK`,`OPK`,`ORA`,`ORAN`,`ORBK`,`ORCL`,`ORI`,`ORIG`,`ORLY`,`OSIS`,`OSK`,`OSPN`,`OSTK`,`OTEX`,`OUT`,`OXM`,`OXY`,`OZK`,`OZM`,`P`,`PAA`,`PAAS`,`PACW`,`PAG`,`PAGP`,`PAGP1`,`PAGS`,`PAH`,`PANW`,`PAYC`,`PAYX`,`PB`,`PBCT`,`PBCT1`,`PBF`,`PBFX`,`PBI`,`PBPB`,`PBR`,`PBYI`,`PCAR`,`PCG`,`PCH`,`PCH2`,`PCRX`,`PCTY`,`PCYG`,`PDCE`,`PDCO`,`PDD`,`PDFS`,`PDM`,`PE`,`PEB`,`PEG`,`PEGA`,`PEGI`,`PEI`,`PENN`,`PENN1`,`PEP`,`PERY`,`PETS`,`PF`,`PFE`,`PFG`,`PFPT`,`PG`,`PGJ`,`PGNX`,`PGR`,`PGRE`,`PGTI`,`PH`,`PHM`,`PI`,`PICO`,`PII`,`PINC`,`PIR`,`PKG`,`PKI`,`PKX`,`PLAB`,`PLAY`,`PLCE`,`PLD`,`PLD1`,`PLNT`,`PLT`,`PLXS`,`PM`,`PMT`,`PNC`,`PNFP`,`PNK`,`PNM`,`PNR`,`PNR1`,`PNW`,`PODD`,`POL`,`POOL`,`POR`,`POST`,`POWI`,`PPC`,`PPG`,`PPH`,`PPL`,`PRAA`,`PRAH`,`PRFT`,`PRGO`,`PRGS`,`PRI`,`PRLB`,`PRMW`,`PRO`,`PRSC`,`PRTA`,`PRTK`,`PRTY`,`PRU`,`PSA`,`PSB`,`PSEC`,`PSMT`,`PSTG`,`PSX`,`PSXP`,`PTC`,`PTEN`,`PTLA`,`PTR`,`PTR1`,`PTR2`,`PUK`,`PVG`,`PVH`,`PVTL`,`PWR`,`PX`,`PXD`,`PYPL`,`PZZA`,`QADA`,`QCOM`,`QD`,`QDEL`,`QEP`,`QID`,`QID1`,`QIWI`,`QLD`,`QLYS`,`QNST`,`QQQ`,`QRTEA`,`QRVO`,`QSR`,`QTWO`,`QUAD`,`QUOT`,`QURE`,`R`,`RACE`,`RAD`,`RAIL`,`RAMP`,`RAVN`,`RBA`,`RBBN`,`RBC`,`RBS`,`RCI`,`RCII`,`RCL`,`RDC`,`RDFN`,`RDN`,`RDSA`,`RDSB`,`RDUS`,`RDWR`,`RE`,`REG`,`REGI`,`REGN`,`REN`,`REPH`,`RES`,`RETA`,`REV`,`RF`,`RGA`,`RGEN`,`RGLD`,`RGNX`,`RGR`,`RH`,`RHI`,`RHT`,`RIG`,`RIO`,`RIOT`,`RJF`,`RL`,`RLGY`,`RLI`,`RLJ`,`RMBS`,`RMD`,`RMTI`,`RNG`,`RNR`,`ROCK`,`ROG`,`ROIC`,`ROK`,`ROKU`,`ROP`,`ROST`,`RP`,`RPAI`,`RPD`,`RPM`,`RRC`,`RRD`,`RRGB`,`RRR`,`RS`,`RSG`,`RST`,`RSX`,`RTEC`,`RTN`,`RTRX`,`RUBI`,`RUN`,`RUSHA`,`RUSL`,`RUTH`,`RWR`,`RXN`,`RY`,`RYAAY`,`RYAM`,`RYN`,`S`,`SA`,`SABR`,`SAFM`,`SAGE`,`SAH`,`SAIA`,`SAIC`,`SAIL`,`SAM`,`SAN`,`SAN1`,`SANM`,`SAP`,`SASR`,`SATS`,`SAVE`,`SBAC`,`SBGI`,`SBGL`,`SBGL2`,`SBH`,`SBNY`,`SBRA`,`SBUX`,`SC`,`SCCO`,`SCG`,`SCHD`,`SCHL`,`SCHN`,`SCHW`,`SCI`,`SCL`,`SCO`,`SCS`,`SCSC`,`SD`,`SDRL1`,`SDS`,`SDS1`,`SE`,`SEAS`,`SEDG`,`SEE`,`SEIC`,`SEM`,`SEMG`,`SEND`,`SEP`,`SERV`,`SERV1`,`SF`,`SFIX`,`SFL`,`SFLY`,`SFM`,`SFUN`,`SGEN`,`SGH`,`SGMO`,`SGMS`,`SH`,`SHAK`,`SHEN`,`SHLD`,`SHLX`,`SHO`,`SHOO`,`SHOO1`,`SHOP`,`SHPG`,`SHW`,`SIG`,`SIGM`,`SIGM1`,`SIL`,`SIMO`,`SINA`,`SINA2`,`SIR`,`SIRI`,`SITC`,`SITC1`,`SITC2`,`SIVB`,`SIX`,`SJI`,`SJM`,`SKF`,`SKT`,`SKX`,`SKYW`,`SLAB`,`SLB`,`SLCA`,`SLG`,`SLGN`,`SLM`,`SLV`,`SLX`,`SM`,`SMCI`,`SMG`,`SMH`,`SMLP`,`SMN`,`SMN1`,`SMTC`,`SN`,`SNA`,`SNAP`,`SNBR`,`SNCR`,`SNE`,`SNH`,`SNHY`,`SNN`,`SNP`,`SNPS`,`SNV`,`SNX`,`SNX1`,`SNY`,`SO`,`SODA`,`SOGO`,`SOHU`,`SON`,`SONC`,`SONO`,`SPA`,`SPB`,`SPB1`,`SPG`,`SPGI`,`SPH`,`SPLK`,`SPN`,`SPOT`,`SPPI`,`SPR`,`SPTN`,`SPWR`,`SPX`,`SPXC`,`SPXL`,`SPXS`,`SPXU`,`SPXU1`,`SPY`,`SQ`,`SQM`,`SQM1`,`SQQQ`,`SQQQ1`,`SRC`,`SRC1`,`SRCI`,`SRCL`,`SRE`,`SRG`,`SRI`,`SRPT`,`SRS`,`SSB`,`SSC`,`SSD`,`SSL`,`SSNC`,`SSO`,`SSP`,`SSRM`,`SSTK`,`SSW`,`SSYS`,`ST`,`STAY`,`STI`,`STKL`,`STLD`,`STM`,`STMP`,`STNG`,`STON`,`STOR`,`STRA`,`STRA1`,`STT`,`STWD`,`STX`,`STZ`,`SU`,`SUI`,`SUM`,`SUN`,`SUP`,`SVU`,`SVU1`,`SVXY`,`SVXY1`,`SWCH`,`SWIR`,`SWK`,`SWKS`,`SWM`,`SWN`,`SWX`,`SXC`,`SYF`,`SYK`,`SYKE`,`SYMC`,`SYNA`,`SYNH`,`SYX`,`SYY`,`T`,`T1`,`TACO`,`TAHO`,`TAL`,`TAP`,`TBI`,`TBT`,`TCBI`,`TCF`,`TCO`,`TCP`,`TCS`,`TCX`,`TD`,`TDC`,`TDG`,`TDOC`,`TDS`,`TEAM`,`TECD`,`TECK`,`TECL`,`TEF`,`TEL`,`TEN`,`TEO`,`TER`,`TERP`,`TEVA`,`TEX`,`TFSL`,`TGE`,`TGE1`,`TGI`,`TGNA`,`TGP`,`TGT`,`TGTX`,`THC`,`THO`,`THRM`,`THS`,`TIF`,`TISI`,`TITN`,`TIVO`,`TJX`,`TK`,`TKR`,`TLRD`,`TLRY`,`TLT`,`TLYS`,`TM`,`TMF`,`TMHC`,`TMHC1`,`TMK`,`TMO`,`TMST`,`TMUS`,`TNA`,`TNC`,`TNDM`,`TNET`,`TOL`,`TOO`,`TOT`,`TOWN`,`TPC`,`TPH`,`TPR`,`TPX`,`TQQQ`,`TRCO`,`TREE`,`TREX`,`TRGP`,`TRI`,`TRIP`,`TRMB`,`TRMK`,`TRN`,`TROW`,`TROX`,`TRP`,`TRQ`,`TRS`,`TRTN`,`TRU`,`TRUE`,`TRV`,`TRVG`,`TRVN`,`TS`,`TSCO`,`TSE`,`TSEM`,`TSG`,`TSLA`,`TSLA1`,`TSM`,`TSN`,`TSRO`,`TSS`,`TTC`,`TTD`,`TTGT`,`TTM`,`TTMI`,`TTS`,`TTWO`,`TU`,`TUP`,`TUR`,`TUSK`,`TV`,`TVPT`,`TVTY`,`TWI`,`TWLO`,`TWM`,`TWNK`,`TWO`,`TWO1`,`TWOU`,`TWTR`,`TX`,`TXMD`,`TXN`,`TXRH`,`TXT`,`TYL`,`TYPE`,`TZOO`,`UA`,`UAA`,`UAL`,`UBNT`,`UCO`,`UCO2`,`UCTT`,`UDR`,`UEPS`,`UFS`,`UGI`,`UHS`,`UIHC`,`UIS`,`UL`,`ULTA`,`ULTI`,`UN`,`UNFI`,`UNG`,`UNG1`,`UNH`,`UNIT`,`UNM`,`UNP`,`UNT`,`UNVR`,`UPLD`,`UPRO`,`UPS`,`URBN`,`URE`,`URI`,`USAC`,`USB`,`USCR`,`USFD`,`USG`,`USM`,`USO`,`USPH`,`UTHR`,`UTX`,`UUP`,`UVE`,`UVXY`,`UVXY1`,`UVXY2`,`UVXY3`,`UWM`,`UYG`,`UYM`,`V`,`VAC`,`VAC1`,`VALE`,`VAR`,`VC`,`VCRA`,`VDE`,`VECO`,`VEEV`,`VEON`,`VER`,`VFC`,`VG`,`VGK`,`VGR`,`VGR1`,`VIAB`,`VIAV`,`VICR`,`VIG`,`VIIX`,`VIPS`,`VIRT`,`VIXY`,`VIXY2`,`VJET`,`VKTX`,`VLO`,`VLP`,`VLRS`,`VLY`,`VMC`,`VMI`,`VMW`,`VNCE`,`VNDA`,`VNET`,`VNO`,`VNOM`,`VNQ`,`VOD`,`VOO`,`VOYA`,`VRA`,`VRNS`,`VRNT`,`VRSK`,`VRSN`,`VRTX`,`VSAT`,`VSH`,`VSI`,`VSLR`,`VSM`,`VST`,`VST1`,`VSTO`,`VTI`,`VTL`,`VTR`,`VVC`,`VVV`,`VWO`,`VXX`,`VXX2`,`VXXB`,`VXZ`,`VXZB`,`VYM`,`VZ`,`VZ1`,`W`,`WAB`,`WABC`,`WAGE`,`WAL`,`WAT`,`WATT`,`WB`,`WBA`,`WBAI`,`WBC`,`WBS`,`WCC`,`WCG`,`WCN`,`WDAY`,`WDC`,`WDFC`,`WDR`,`WEC`,`WELL`,`WEN`,`WERN`,`WES`,`WETF`,`WEX`,`WFC`,`WFT`,`WGO`,`WH`,`WHR`,`WIFI`,`WIN`,`WIN1`,`WING`,`WIRE`,`WIT`,`WIX`,`WK`,`WLH`,`WLK`,`WLKP`,`WLL`,`WLL1`,`WLTW`,`WM`,`WMB`,`WMB1`,`WMS`,`WMT`,`WNC`,`WOR`,`WP`,`WPC`,`WPG`,`WPM`,`WPP`,`WPX`,`WRB`,`WRD`,`WRE`,`WRI`,`WRK`,`WRLD`,`WSM`,`WSO`,`WTFC`,`WTR`,`WTW`,`WU`,`WUBA`,`WWD`,`WWE`,`WWW`,`WY`,`WYND`,`WYND1`,`WYNN`,`X`,`XBI`,`XEC`,`XEL`,`XENT`,`XES`,`XHB`,`XLB`,`XLE`,`XLF`,`XLI`,`XLK`,`XLNX`,`XLP`,`XLRN`,`XLU`,`XLV`,`XLY`,`XME`,`XNET`,`XOM`,`XON`,`XON2`,`XONE`,`XOP`,`XOXO`,`XPER`,`XPO`,`XRAY`,`XRT`,`XRX`,`XRX1`,`XRX2`,`XYL`,`YCS`,`YELP`,`YEXT`,`YNDX`,`YPF`,`YRCW`,`YRD`,`YUM`,`YUM1`,`YUMC`,`YY`,`Z`,`ZAGG`,`ZAYO`,`ZBH`,`ZBRA`,`ZEN`,`ZG`,`ZGNX`,`ZION`,`ZNGA`,`ZNH`,`ZOES`,`ZS`,`ZSL`,`ZTO`,`ZTS`,`ZUMZ`,`ZUO`");
        assertEquals(MatchFilter.class, f.getClass());
    }


    public void testInDateTimes() {
        DateTime wed = DateTimeUtils.convertDateTime("2018-05-02T10:00:00 NY");// not in the table

        DateTime mon = DateTimeUtils.convertDateTime("2018-04-30T10:00:00 NY");
        DateTime tues = DateTimeUtils.convertDateTime("2018-05-01T10:00:00 NY");
        DateTime thurs = DateTimeUtils.convertDateTime("2018-05-03T10:00:00 NY");
        Table t = TableTools.newTable(TableTools.col("Timestamp", new DateTime(mon.getNanos()),
                new DateTime(tues.getNanos()), new DateTime(thurs.getNanos())));
        // match one item
        WhereFilter f = WhereFilterFactory.getExpression("Timestamp in '" + mon + "'");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        RowSet idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(1, idx.size());
        assertEquals(mon, t.getColumn(0).get(idx.firstRowKey()));
        // match one of two items
        f = WhereFilterFactory.getExpression("Timestamp in '" + tues + "', '" + wed + "'");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(1, idx.size());
        assertEquals(tues, t.getColumn(0).get(idx.firstRowKey()));

        // match two of two items
        f = WhereFilterFactory.getExpression("Timestamp in '" + tues + "', '" + thurs + "'");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(2, idx.size());
        assertEquals(tues, t.getColumn(0).get(idx.firstRowKey()));
        assertEquals(thurs, t.getColumn(0).get(idx.lastRowKey()));

        // match zero of one item
        f = WhereFilterFactory.getExpression("Timestamp in '" + wed + "'");
        f.init(t.getDefinition());
        assertEquals(MatchFilter.class, f.getClass());
        idx = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(0, idx.size());
    }

    public void testTypeInference() {
        checkResult("1", true, true, true, true, true, true, false, true, true, (byte) 1, (short) 1, 1, 1,
                new BigInteger("1"), 1.0, new BigDecimal("1"), '1');
        checkResult("-11", true, true, true, true, true, true, false, true, false, (byte) -11, (short) -11, -11, -11,
                new BigInteger("-11"), -11.0, new BigDecimal("-11"), '0');
        checkResult("1.", false, false, false, false, false, true, false, true, false, (byte) 1, (short) 1, 1, 1, null,
                1.0, new BigDecimal("1.0"), '0');
        checkResult("-11.", false, false, false, false, false, true, false, true, false, (byte) 1, (short) 1, 1, 1,
                null, -11.0, new BigDecimal("-11.0"), '0');
        checkResult("1.1", false, false, false, false, false, true, false, true, false, (byte) 1, (short) 1, 1, 1, null,
                1.1, new BigDecimal("1.1"), '0');
        checkResult("1.01", false, false, false, false, false, true, false, true, false, (byte) 1, (short) 1, 1, 1,
                null, 1.01, new BigDecimal("1.01"), '0');
        checkResult("128", false, true, true, true, true, true, false, true, false, (byte) 1, (short) 128, 128, 128,
                new BigInteger("128"), 128, new BigDecimal("128"), '0');
        checkResult("-128", true, true, true, true, true, true, false, true, false, (byte) -128, (short) -128, -128,
                -128, new BigInteger("-128"), -128, new BigDecimal("-128"), '0');
        checkResult("32768", false, false, true, true, true, true, false, true, false, (byte) 0, (short) 0, 32768,
                32768, new BigInteger("32768"), 32768, new BigDecimal("32768"), '0');
        checkResult("-32768", false, true, true, true, true, true, false, true, false, (byte) 0, (short) -32768, -32768,
                -32768, new BigInteger("-32768"), -32768, new BigDecimal("-32768"), '0');
        checkResult("2147483648", false, false, false, true, true, true, false, true, false, (byte) 0, (short) 0, 0,
                2147483648l, new BigInteger("2147483648"), 2147483648l, new BigDecimal("2147483648"), '0');
        checkResult("-2147483648", false, false, true, true, true, true, false, true, false, (byte) 0, (short) 0,
                -2147483648, -2147483648, new BigInteger("-2147483648"), -2147483648, new BigDecimal("-2147483648"),
                '0');

        checkResult("true", false, false, false, false, false, false, true, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');
        checkResult("True", false, false, false, false, false, false, true, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');
        checkResult("TrUe", false, false, false, false, false, false, true, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');
        checkResult("TRUE", false, false, false, false, false, false, true, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');
        checkResult("tru3", false, false, false, false, false, false, false, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');
        checkResult("false", false, false, false, false, false, false, true, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');
        checkResult("False", false, false, false, false, false, false, true, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');
        checkResult("FaLsE", false, false, false, false, false, false, true, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');
        checkResult("FALSE", false, false, false, false, false, false, true, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');
        checkResult("FALS3", false, false, false, false, false, false, false, false, false, (byte) 0, (short) 0, 0, 0,
                null, 0, null, '0');

        checkDateRange("18:43", makeDateTime("18:43"), makeDateTime("18:44"));
        checkDateRange("18:43:40", makeDateTime("18:43:40"), makeDateTime("18:43:41"));
        checkDateRange("18:43:40.100", makeDateTime("18:43:40.100"), makeDateTime("18:43:40.101"));
        checkDateRange("2018-03-25 NY", DateTimeUtils.convertDateTime("2018-03-25 NY"),
                DateTimeUtils.convertDateTime("2018-03-26 NY"));
        checkDateRange("2018-03-25T18:00 NY", DateTimeUtils.convertDateTime("2018-03-25T18:00 NY"),
                DateTimeUtils.convertDateTime("2018-03-25T18:01 NY"));
        checkDateRange("2018-03-25T18:00:00 NY", DateTimeUtils.convertDateTime("2018-03-25T18:00:00 NY"),
                DateTimeUtils.convertDateTime("2018-03-25T18:00:01 NY"));
    }

    private DateTime makeDateTime(String timeStr) {
        ZonedDateTime zdt = ZonedDateTime.now(ZoneId.of("America/New_York")).truncatedTo(ChronoUnit.DAYS)
                .plus(DateTimeUtils.convertTime(timeStr), ChronoUnit.NANOS);
        return DateTimeUtils.millisToTime(zdt.toInstant().toEpochMilli());
    }

    private void checkDateRange(String input, DateTime lowerDate, DateTime upperDate) {
        WhereFilterFactory.InferenceResult inf = new WhereFilterFactory.InferenceResult(input);
        assertEquals(false, inf.isByte);
        assertEquals(false, inf.isShort);
        assertEquals(false, inf.isInt);
        assertEquals(false, inf.isLong);
        assertEquals(false, inf.isBigInt);
        assertEquals(false, inf.isBigDecimal);
        assertEquals(false, inf.isBool);
        assertEquals(false, inf.isChar);

        assertEquals(lowerDate.getNanos(), inf.dateLower.getNanos());
        assertEquals(upperDate.getNanos(), inf.dateUpper.getNanos());
    }

    private void checkResult(String input, boolean isByte, boolean isShort, boolean isInt, boolean isLong,
            boolean isBigInt,
            boolean isBigDecimal, boolean isBool, boolean isDouble, boolean isChar, byte byteVal, short shortVal,
            int intVal, long longVal, BigInteger biVal,
            double doubleVal, BigDecimal decVal, char charVal) {
        WhereFilterFactory.InferenceResult inf = new WhereFilterFactory.InferenceResult(input);
        assertEquals(isByte, inf.isByte);
        assertEquals(isShort, inf.isShort);
        assertEquals(isInt, inf.isInt);
        assertEquals(isLong, inf.isLong);
        assertEquals(isBigInt, inf.isBigInt);
        assertEquals(isBigDecimal, inf.isBigDecimal);
        assertEquals(isBool, inf.isBool);
        assertEquals(isChar, inf.isChar);


        if (isByte) {
            assertEquals(byteVal, inf.byteVal);
        }

        if (isShort) {
            assertEquals(shortVal, inf.shortVal);
        }

        if (isInt) {
            assertEquals(intVal, inf.intVal);
        }

        if (isLong) {
            assertEquals(longVal, inf.longVal);
        }

        if (isBigInt) {
            assertTrue(biVal.equals(inf.bigIntVal));
        }

        if (isDouble) {
            assertEquals(doubleVal, inf.doubleVal);
        }

        if (isBigDecimal) {
            assertEquals(decVal.compareTo(inf.bigDecVal), 0);
        }

        if (isChar) {
            assertEquals(charVal, inf.charVal);
        }
    }

    public void testIncludesMatcher() {
        Table t = TableTools.newTable(TableTools.col("Phrase",
                /* 0 */ "T1",
                /* 1 */ "T2",
                /* 2 */ "T3",
                /* 3 */ "ABCt1DEF",
                /* 4 */ "t2",
                /* 5 */ "John is Hungry at time T-1",
                /* 6 */ "Sometimes T1 is better than T2,  but  T-3 is always awful",
                /* 7 */ "T1 is Not t2,  and T3 is awesome",
                /* 8 */ "John is Hungry at time t2",
                /* 9 */ "Sometimes T1 is better than T2,  but  T3 is always awful",
                /* 10 */ "ABCT1T2T3DEF",
                /* 11 */ "John is Hungry at time T1 and Also t2 and T3!",
                /* 12 */ " All of these T's are getting pretty tiring",
                /* 13 */ "I amNot t2, and neither are you!",
                /* 14 */ null));

        WhereFilter f = WhereFilterFactory.getExpression("Phrase icase includes any `T1`, `T2`, `T3`");
        assertTrue("f instanceof StringContainsFilter", f instanceof StringContainsFilter);
        f.init(t.getDefinition());
        RowSet result = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(12, result.size());
        assertEquals(RowSetFactory.fromKeys(0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13), result);

        f = WhereFilterFactory.getExpression("Phrase icase includes all `T1`, `T2`, `T3`");
        assertTrue("f instanceof StringContainsFilter", f instanceof StringContainsFilter);
        f.init(t.getDefinition());
        result = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(4, result.size());
        assertEquals(RowSetFactory.fromKeys(7, 9, 10, 11), result);

        f = WhereFilterFactory.getExpression("Phrase includes any `T1`, `T2`, `T3`");
        assertTrue("f instanceof StringContainsFilter", f instanceof StringContainsFilter);
        f.init(t.getDefinition());
        result = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(RowSetFactory.fromKeys(0, 1, 2, 6, 7, 9, 10, 11), result);

        f = WhereFilterFactory.getExpression("Phrase includes all `T1`, `t2`, `T3`");
        assertTrue("f instanceof StringContainsFilter", f instanceof StringContainsFilter);
        f.init(t.getDefinition());
        result = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(RowSetFactory.fromKeys(7, 11), result);

        f = WhereFilterFactory.getExpression("Phrase icase not includes any `T1`, `T2`, `T3`");
        assertTrue("f instanceof StringContainsFilter", f instanceof StringContainsFilter);
        f.init(t.getDefinition());
        result = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(RowSetFactory.fromKeys(0, 1, 2, 3, 4, 5, 6, 8, 12, 13), result);

        f = WhereFilterFactory.getExpression("Phrase icase not includes all `T1`, `T2`, `T3`");
        assertTrue("f instanceof StringContainsFilter", f instanceof StringContainsFilter);
        f.init(t.getDefinition());
        result = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(RowSetFactory.fromKeys(5, 12), result);

        f = WhereFilterFactory.getExpression("Phrase not includes any `T1`, `T2`, `T3`");
        assertTrue("f instanceof StringContainsFilter", f instanceof StringContainsFilter);
        f.init(t.getDefinition());
        result = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(RowSetFactory.fromKeys(0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 13), result);

        f = WhereFilterFactory.getExpression("Phrase not includes all `T1`, `t2`, `T3`");
        assertTrue("f instanceof StringContainsFilter", f instanceof StringContainsFilter);
        f.init(t.getDefinition());
        result = f.filter(t.getRowSet().copy(), t.getRowSet(), t, false);
        assertEquals(RowSetFactory.fromKeys(1, 3, 5, 12), result);
    }
}
