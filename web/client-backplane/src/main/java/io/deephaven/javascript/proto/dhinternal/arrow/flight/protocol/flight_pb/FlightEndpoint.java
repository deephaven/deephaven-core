package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.Flight_pb.FlightEndpoint",
    namespace = JsPackage.GLOBAL)
public class FlightEndpoint {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LocationListFieldType {
      @JsOverlay
      static FlightEndpoint.ToObjectReturnType.LocationListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getUri();

      @JsProperty
      void setUri(String uri);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TicketFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static FlightEndpoint.ToObjectReturnType.TicketFieldType.GetTicketUnionType of(Object o) {
          return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
          return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
          return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
          return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
          return (Object) this instanceof Uint8Array;
        }
      }

      @JsOverlay
      static FlightEndpoint.ToObjectReturnType.TicketFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FlightEndpoint.ToObjectReturnType.TicketFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(FlightEndpoint.ToObjectReturnType.TicketFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js.<FlightEndpoint.ToObjectReturnType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js.<FlightEndpoint.ToObjectReturnType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                ticket));
      }
    }

    @JsOverlay
    static FlightEndpoint.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<FlightEndpoint.ToObjectReturnType.LocationListFieldType> getLocationList();

    @JsProperty
    FlightEndpoint.ToObjectReturnType.TicketFieldType getTicket();

    @JsProperty
    void setLocationList(
        JsArray<FlightEndpoint.ToObjectReturnType.LocationListFieldType> locationList);

    @JsOverlay
    default void setLocationList(
        FlightEndpoint.ToObjectReturnType.LocationListFieldType[] locationList) {
      setLocationList(
          Js.<JsArray<FlightEndpoint.ToObjectReturnType.LocationListFieldType>>uncheckedCast(
              locationList));
    }

    @JsProperty
    void setTicket(FlightEndpoint.ToObjectReturnType.TicketFieldType ticket);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LocationListFieldType {
      @JsOverlay
      static FlightEndpoint.ToObjectReturnType0.LocationListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getUri();

      @JsProperty
      void setUri(String uri);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TicketFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static FlightEndpoint.ToObjectReturnType0.TicketFieldType.GetTicketUnionType of(Object o) {
          return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
          return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
          return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
          return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
          return (Object) this instanceof Uint8Array;
        }
      }

      @JsOverlay
      static FlightEndpoint.ToObjectReturnType0.TicketFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FlightEndpoint.ToObjectReturnType0.TicketFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(FlightEndpoint.ToObjectReturnType0.TicketFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js.<FlightEndpoint.ToObjectReturnType0.TicketFieldType.GetTicketUnionType>uncheckedCast(
                ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js.<FlightEndpoint.ToObjectReturnType0.TicketFieldType.GetTicketUnionType>uncheckedCast(
                ticket));
      }
    }

    @JsOverlay
    static FlightEndpoint.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<FlightEndpoint.ToObjectReturnType0.LocationListFieldType> getLocationList();

    @JsProperty
    FlightEndpoint.ToObjectReturnType0.TicketFieldType getTicket();

    @JsProperty
    void setLocationList(
        JsArray<FlightEndpoint.ToObjectReturnType0.LocationListFieldType> locationList);

    @JsOverlay
    default void setLocationList(
        FlightEndpoint.ToObjectReturnType0.LocationListFieldType[] locationList) {
      setLocationList(
          Js.<JsArray<FlightEndpoint.ToObjectReturnType0.LocationListFieldType>>uncheckedCast(
              locationList));
    }

    @JsProperty
    void setTicket(FlightEndpoint.ToObjectReturnType0.TicketFieldType ticket);
  }

  public static native FlightEndpoint deserializeBinary(Uint8Array bytes);

  public static native FlightEndpoint deserializeBinaryFromReader(
      FlightEndpoint message, Object reader);

  public static native void serializeBinaryToWriter(FlightEndpoint message, Object writer);

  public static native FlightEndpoint.ToObjectReturnType toObject(
      boolean includeInstance, FlightEndpoint msg);

  public native Location addLocation();

  public native Location addLocation(Location value, double index);

  public native Location addLocation(Location value);

  public native void clearLocationList();

  public native void clearTicket();

  public native JsArray<Location> getLocationList();

  public native Ticket getTicket();

  public native boolean hasTicket();

  public native Uint8Array serializeBinary();

  public native void setLocationList(JsArray<Location> value);

  @JsOverlay
  public final void setLocationList(Location[] value) {
    setLocationList(Js.<JsArray<Location>>uncheckedCast(value));
  }

  public native void setTicket();

  public native void setTicket(Ticket value);

  public native FlightEndpoint.ToObjectReturnType0 toObject();

  public native FlightEndpoint.ToObjectReturnType0 toObject(boolean includeInstance);
}
