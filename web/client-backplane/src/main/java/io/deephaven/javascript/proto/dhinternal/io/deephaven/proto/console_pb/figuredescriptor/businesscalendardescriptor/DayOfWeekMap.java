package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.businesscalendardescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.BusinessCalendarDescriptor.DayOfWeekMap",
    namespace = JsPackage.GLOBAL)
public interface DayOfWeekMap {
  @JsOverlay
  static DayOfWeekMap create() {
    return Js.uncheckedCast(JsPropertyMap.of());
  }

  @JsProperty(name = "FRIDAY")
  double getFRIDAY();

  @JsProperty(name = "MONDAY")
  double getMONDAY();

  @JsProperty(name = "SATURDAY")
  double getSATURDAY();

  @JsProperty(name = "SUNDAY")
  double getSUNDAY();

  @JsProperty(name = "THURSDAY")
  double getTHURSDAY();

  @JsProperty(name = "TUESDAY")
  double getTUESDAY();

  @JsProperty(name = "WEDNESDAY")
  double getWEDNESDAY();

  @JsProperty(name = "FRIDAY")
  void setFRIDAY(double FRIDAY);

  @JsProperty(name = "MONDAY")
  void setMONDAY(double MONDAY);

  @JsProperty(name = "SATURDAY")
  void setSATURDAY(double SATURDAY);

  @JsProperty(name = "SUNDAY")
  void setSUNDAY(double SUNDAY);

  @JsProperty(name = "THURSDAY")
  void setTHURSDAY(double THURSDAY);

  @JsProperty(name = "TUESDAY")
  void setTUESDAY(double TUESDAY);

  @JsProperty(name = "WEDNESDAY")
  void setWEDNESDAY(double WEDNESDAY);
}
