package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.comboaggregaterequest;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.ComboAggregateRequest.AggTypeMap",
    namespace = JsPackage.GLOBAL)
public interface AggTypeMap {
    @JsOverlay
    static AggTypeMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "ABS_SUM")
    double getABS_SUM();

    @JsProperty(name = "ARRAY")
    double getARRAY();

    @JsProperty(name = "AVG")
    double getAVG();

    @JsProperty(name = "COUNT")
    double getCOUNT();

    @JsProperty(name = "FIRST")
    double getFIRST();

    @JsProperty(name = "LAST")
    double getLAST();

    @JsProperty(name = "MAX")
    double getMAX();

    @JsProperty(name = "MEDIAN")
    double getMEDIAN();

    @JsProperty(name = "MIN")
    double getMIN();

    @JsProperty(name = "PERCENTILE")
    double getPERCENTILE();

    @JsProperty(name = "STD")
    double getSTD();

    @JsProperty(name = "SUM")
    double getSUM();

    @JsProperty(name = "VAR")
    double getVAR();

    @JsProperty(name = "WEIGHTED_AVG")
    double getWEIGHTED_AVG();

    @JsProperty(name = "ABS_SUM")
    void setABS_SUM(double ABS_SUM);

    @JsProperty(name = "ARRAY")
    void setARRAY(double ARRAY);

    @JsProperty(name = "AVG")
    void setAVG(double AVG);

    @JsProperty(name = "COUNT")
    void setCOUNT(double COUNT);

    @JsProperty(name = "FIRST")
    void setFIRST(double FIRST);

    @JsProperty(name = "LAST")
    void setLAST(double LAST);

    @JsProperty(name = "MAX")
    void setMAX(double MAX);

    @JsProperty(name = "MEDIAN")
    void setMEDIAN(double MEDIAN);

    @JsProperty(name = "MIN")
    void setMIN(double MIN);

    @JsProperty(name = "PERCENTILE")
    void setPERCENTILE(double PERCENTILE);

    @JsProperty(name = "STD")
    void setSTD(double STD);

    @JsProperty(name = "SUM")
    void setSUM(double SUM);

    @JsProperty(name = "VAR")
    void setVAR(double VAR);

    @JsProperty(name = "WEIGHTED_AVG")
    void setWEIGHTED_AVG(double WEIGHTED_AVG);
}
