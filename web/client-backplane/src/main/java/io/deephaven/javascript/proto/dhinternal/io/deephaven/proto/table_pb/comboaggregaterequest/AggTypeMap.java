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

  @JsProperty(name = "ABSSUM")
  double getABSSUM();

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

  @JsProperty(name = "WEIGHTEDAVG")
  double getWEIGHTEDAVG();

  @JsProperty(name = "ABSSUM")
  void setABSSUM(double ABSSUM);

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

  @JsProperty(name = "WEIGHTEDAVG")
  void setWEIGHTEDAVG(double WEIGHTEDAVG);
}
