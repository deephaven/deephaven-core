package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.changedocumentrequest.TextDocumentContentChangeEvent;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.ChangeDocumentRequest",
    namespace = JsPackage.GLOBAL)
public class ChangeDocumentRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ChangeDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType of(
                    Object o) {
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
            static ChangeDocumentRequest.ToObjectReturnType.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ChangeDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                ChangeDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<ChangeDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<ChangeDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContentChangesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RangeFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface StartFieldType {
                    @JsOverlay
                    static ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType.RangeFieldType.StartFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getCharacter();

                    @JsProperty
                    double getLine();

                    @JsProperty
                    void setCharacter(double character);

                    @JsProperty
                    void setLine(double line);
                }

                @JsOverlay
                static ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType.RangeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getEnd();

                @JsProperty
                ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType.RangeFieldType.StartFieldType getStart();

                @JsProperty
                void setEnd(Object end);

                @JsProperty
                void setStart(
                    ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType.RangeFieldType.StartFieldType start);
            }

            @JsOverlay
            static ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType.RangeFieldType getRange();

            @JsProperty
            double getRangeLength();

            @JsProperty
            String getText();

            @JsProperty
            void setRange(
                ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType.RangeFieldType range);

            @JsProperty
            void setRangeLength(double rangeLength);

            @JsProperty
            void setText(String text);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextDocumentFieldType {
            @JsOverlay
            static ChangeDocumentRequest.ToObjectReturnType.TextDocumentFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getUri();

            @JsProperty
            double getVersion();

            @JsProperty
            void setUri(String uri);

            @JsProperty
            void setVersion(double version);
        }

        @JsOverlay
        static ChangeDocumentRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ChangeDocumentRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

        @JsProperty
        JsArray<ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType> getContentChangesList();

        @JsProperty
        ChangeDocumentRequest.ToObjectReturnType.TextDocumentFieldType getTextDocument();

        @JsProperty
        void setConsoleId(ChangeDocumentRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);

        @JsOverlay
        default void setContentChangesList(
            ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType[] contentChangesList) {
            setContentChangesList(
                Js.<JsArray<ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType>>uncheckedCast(
                    contentChangesList));
        }

        @JsProperty
        void setContentChangesList(
            JsArray<ChangeDocumentRequest.ToObjectReturnType.ContentChangesListFieldType> contentChangesList);

        @JsProperty
        void setTextDocument(
            ChangeDocumentRequest.ToObjectReturnType.TextDocumentFieldType textDocument);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ChangeDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType of(
                    Object o) {
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
            static ChangeDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ChangeDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                ChangeDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<ChangeDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<ChangeDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContentChangesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RangeFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface StartFieldType {
                    @JsOverlay
                    static ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType.RangeFieldType.StartFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getCharacter();

                    @JsProperty
                    double getLine();

                    @JsProperty
                    void setCharacter(double character);

                    @JsProperty
                    void setLine(double line);
                }

                @JsOverlay
                static ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType.RangeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getEnd();

                @JsProperty
                ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType.RangeFieldType.StartFieldType getStart();

                @JsProperty
                void setEnd(Object end);

                @JsProperty
                void setStart(
                    ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType.RangeFieldType.StartFieldType start);
            }

            @JsOverlay
            static ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType.RangeFieldType getRange();

            @JsProperty
            double getRangeLength();

            @JsProperty
            String getText();

            @JsProperty
            void setRange(
                ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType.RangeFieldType range);

            @JsProperty
            void setRangeLength(double rangeLength);

            @JsProperty
            void setText(String text);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextDocumentFieldType {
            @JsOverlay
            static ChangeDocumentRequest.ToObjectReturnType0.TextDocumentFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getUri();

            @JsProperty
            double getVersion();

            @JsProperty
            void setUri(String uri);

            @JsProperty
            void setVersion(double version);
        }

        @JsOverlay
        static ChangeDocumentRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ChangeDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

        @JsProperty
        JsArray<ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType> getContentChangesList();

        @JsProperty
        ChangeDocumentRequest.ToObjectReturnType0.TextDocumentFieldType getTextDocument();

        @JsProperty
        void setConsoleId(ChangeDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);

        @JsOverlay
        default void setContentChangesList(
            ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType[] contentChangesList) {
            setContentChangesList(
                Js.<JsArray<ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType>>uncheckedCast(
                    contentChangesList));
        }

        @JsProperty
        void setContentChangesList(
            JsArray<ChangeDocumentRequest.ToObjectReturnType0.ContentChangesListFieldType> contentChangesList);

        @JsProperty
        void setTextDocument(
            ChangeDocumentRequest.ToObjectReturnType0.TextDocumentFieldType textDocument);
    }

    public static native ChangeDocumentRequest deserializeBinary(Uint8Array bytes);

    public static native ChangeDocumentRequest deserializeBinaryFromReader(
        ChangeDocumentRequest message, Object reader);

    public static native void serializeBinaryToWriter(ChangeDocumentRequest message, Object writer);

    public static native ChangeDocumentRequest.ToObjectReturnType toObject(
        boolean includeInstance, ChangeDocumentRequest msg);

    public native TextDocumentContentChangeEvent addContentChanges();

    public native TextDocumentContentChangeEvent addContentChanges(
        TextDocumentContentChangeEvent value, double index);

    public native TextDocumentContentChangeEvent addContentChanges(
        TextDocumentContentChangeEvent value);

    public native void clearConsoleId();

    public native void clearContentChangesList();

    public native void clearTextDocument();

    public native Ticket getConsoleId();

    public native JsArray<TextDocumentContentChangeEvent> getContentChangesList();

    public native VersionedTextDocumentIdentifier getTextDocument();

    public native boolean hasConsoleId();

    public native boolean hasTextDocument();

    public native Uint8Array serializeBinary();

    public native void setConsoleId();

    public native void setConsoleId(Ticket value);

    public native void setContentChangesList(JsArray<TextDocumentContentChangeEvent> value);

    @JsOverlay
    public final void setContentChangesList(TextDocumentContentChangeEvent[] value) {
        setContentChangesList(Js.<JsArray<TextDocumentContentChangeEvent>>uncheckedCast(value));
    }

    public native void setTextDocument();

    public native void setTextDocument(VersionedTextDocumentIdentifier value);

    public native ChangeDocumentRequest.ToObjectReturnType0 toObject();

    public native ChangeDocumentRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
