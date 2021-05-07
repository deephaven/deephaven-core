package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.filterdescription;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.FilterDescription.OperationMap",
    namespace = JsPackage.GLOBAL)
public interface OperationMap {
  @JsOverlay
  static OperationMap create() {
    return Js.uncheckedCast(JsPropertyMap.of());
  }

  @JsProperty(name = "AND")
  double getAND();

  @JsProperty(name = "CONTAINS")
  double getCONTAINS();

  @JsProperty(name = "CONTAINS_ICASE")
  double getCONTAINS_ICASE();

  @JsProperty(name = "EQ")
  double getEQ();

  @JsProperty(name = "EQ_ICASE")
  double getEQ_ICASE();

  @JsProperty(name = "GT")
  double getGT();

  @JsProperty(name = "GTE")
  double getGTE();

  @JsProperty(name = "IN")
  double getIN();

  @JsProperty(name = "INVOKE")
  double getINVOKE();

  @JsProperty(name = "IN_ICASE")
  double getIN_ICASE();

  @JsProperty(name = "IS_NULL")
  double getIS_NULL();

  @JsProperty(name = "LITERAL")
  double getLITERAL();

  @JsProperty(name = "LT")
  double getLT();

  @JsProperty(name = "LTE")
  double getLTE();

  @JsProperty(name = "MATCHES")
  double getMATCHES();

  @JsProperty(name = "MATCHES_ICASE")
  double getMATCHES_ICASE();

  @JsProperty(name = "NEQ")
  double getNEQ();

  @JsProperty(name = "NEQ_ICASE")
  double getNEQ_ICASE();

  @JsProperty(name = "NOT")
  double getNOT();

  @JsProperty(name = "NOT_IN")
  double getNOT_IN();

  @JsProperty(name = "NOT_IN_ICASE")
  double getNOT_IN_ICASE();

  @JsProperty(name = "OR")
  double getOR();

  @JsProperty(name = "REFERENCE")
  double getREFERENCE();

  @JsProperty(name = "SEARCH")
  double getSEARCH();

  @JsProperty(name = "UNKNOWN")
  double getUNKNOWN();

  @JsProperty(name = "AND")
  void setAND(double AND);

  @JsProperty(name = "CONTAINS")
  void setCONTAINS(double CONTAINS);

  @JsProperty(name = "CONTAINS_ICASE")
  void setCONTAINS_ICASE(double CONTAINS_ICASE);

  @JsProperty(name = "EQ")
  void setEQ(double EQ);

  @JsProperty(name = "EQ_ICASE")
  void setEQ_ICASE(double EQ_ICASE);

  @JsProperty(name = "GT")
  void setGT(double GT);

  @JsProperty(name = "GTE")
  void setGTE(double GTE);

  @JsProperty(name = "IN")
  void setIN(double IN);

  @JsProperty(name = "INVOKE")
  void setINVOKE(double INVOKE);

  @JsProperty(name = "IN_ICASE")
  void setIN_ICASE(double IN_ICASE);

  @JsProperty(name = "IS_NULL")
  void setIS_NULL(double IS_NULL);

  @JsProperty(name = "LITERAL")
  void setLITERAL(double LITERAL);

  @JsProperty(name = "LT")
  void setLT(double LT);

  @JsProperty(name = "LTE")
  void setLTE(double LTE);

  @JsProperty(name = "MATCHES")
  void setMATCHES(double MATCHES);

  @JsProperty(name = "MATCHES_ICASE")
  void setMATCHES_ICASE(double MATCHES_ICASE);

  @JsProperty(name = "NEQ")
  void setNEQ(double NEQ);

  @JsProperty(name = "NEQ_ICASE")
  void setNEQ_ICASE(double NEQ_ICASE);

  @JsProperty(name = "NOT")
  void setNOT(double NOT);

  @JsProperty(name = "NOT_IN")
  void setNOT_IN(double NOT_IN);

  @JsProperty(name = "NOT_IN_ICASE")
  void setNOT_IN_ICASE(double NOT_IN_ICASE);

  @JsProperty(name = "OR")
  void setOR(double OR);

  @JsProperty(name = "REFERENCE")
  void setREFERENCE(double REFERENCE);

  @JsProperty(name = "SEARCH")
  void setSEARCH(double SEARCH);

  @JsProperty(name = "UNKNOWN")
  void setUNKNOWN(double UNKNOWN);
}
