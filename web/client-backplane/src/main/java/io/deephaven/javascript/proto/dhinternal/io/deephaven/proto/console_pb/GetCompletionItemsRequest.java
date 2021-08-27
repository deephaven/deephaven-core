package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.GetCompletionItemsRequest",
        namespace = JsPackage.GLOBAL)
public class GetCompletionItemsRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static GetCompletionItemsRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType of(Object o) {
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
            static GetCompletionItemsRequest.ToObjectReturnType.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            GetCompletionItemsRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    GetCompletionItemsRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<GetCompletionItemsRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<GetCompletionItemsRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContextFieldType {
            @JsOverlay
            static GetCompletionItemsRequest.ToObjectReturnType.ContextFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getTriggerCharacter();

            @JsProperty
            double getTriggerKind();

            @JsProperty
            void setTriggerCharacter(String triggerCharacter);

            @JsProperty
            void setTriggerKind(double triggerKind);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PositionFieldType {
            @JsOverlay
            static GetCompletionItemsRequest.ToObjectReturnType.PositionFieldType create() {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextDocumentFieldType {
            @JsOverlay
            static GetCompletionItemsRequest.ToObjectReturnType.TextDocumentFieldType create() {
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
        static GetCompletionItemsRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetCompletionItemsRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

        @JsProperty
        GetCompletionItemsRequest.ToObjectReturnType.ContextFieldType getContext();

        @JsProperty
        GetCompletionItemsRequest.ToObjectReturnType.PositionFieldType getPosition();

        @JsProperty
        double getRequestId();

        @JsProperty
        GetCompletionItemsRequest.ToObjectReturnType.TextDocumentFieldType getTextDocument();

        @JsProperty
        void setConsoleId(GetCompletionItemsRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);

        @JsProperty
        void setContext(GetCompletionItemsRequest.ToObjectReturnType.ContextFieldType context);

        @JsProperty
        void setPosition(GetCompletionItemsRequest.ToObjectReturnType.PositionFieldType position);

        @JsProperty
        void setRequestId(double requestId);

        @JsProperty
        void setTextDocument(
                GetCompletionItemsRequest.ToObjectReturnType.TextDocumentFieldType textDocument);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static GetCompletionItemsRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType of(
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
            static GetCompletionItemsRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            GetCompletionItemsRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    GetCompletionItemsRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<GetCompletionItemsRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<GetCompletionItemsRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContextFieldType {
            @JsOverlay
            static GetCompletionItemsRequest.ToObjectReturnType0.ContextFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getTriggerCharacter();

            @JsProperty
            double getTriggerKind();

            @JsProperty
            void setTriggerCharacter(String triggerCharacter);

            @JsProperty
            void setTriggerKind(double triggerKind);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PositionFieldType {
            @JsOverlay
            static GetCompletionItemsRequest.ToObjectReturnType0.PositionFieldType create() {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextDocumentFieldType {
            @JsOverlay
            static GetCompletionItemsRequest.ToObjectReturnType0.TextDocumentFieldType create() {
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
        static GetCompletionItemsRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetCompletionItemsRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

        @JsProperty
        GetCompletionItemsRequest.ToObjectReturnType0.ContextFieldType getContext();

        @JsProperty
        GetCompletionItemsRequest.ToObjectReturnType0.PositionFieldType getPosition();

        @JsProperty
        double getRequestId();

        @JsProperty
        GetCompletionItemsRequest.ToObjectReturnType0.TextDocumentFieldType getTextDocument();

        @JsProperty
        void setConsoleId(GetCompletionItemsRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);

        @JsProperty
        void setContext(GetCompletionItemsRequest.ToObjectReturnType0.ContextFieldType context);

        @JsProperty
        void setPosition(GetCompletionItemsRequest.ToObjectReturnType0.PositionFieldType position);

        @JsProperty
        void setRequestId(double requestId);

        @JsProperty
        void setTextDocument(
                GetCompletionItemsRequest.ToObjectReturnType0.TextDocumentFieldType textDocument);
    }

    public static native GetCompletionItemsRequest deserializeBinary(Uint8Array bytes);

    public static native GetCompletionItemsRequest deserializeBinaryFromReader(
            GetCompletionItemsRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            GetCompletionItemsRequest message, Object writer);

    public static native GetCompletionItemsRequest.ToObjectReturnType toObject(
            boolean includeInstance, GetCompletionItemsRequest msg);

    public native void clearConsoleId();

    public native void clearContext();

    public native void clearPosition();

    public native void clearTextDocument();

    public native Ticket getConsoleId();

    public native CompletionContext getContext();

    public native Position getPosition();

    public native int getRequestId();

    public native VersionedTextDocumentIdentifier getTextDocument();

    public native boolean hasConsoleId();

    public native boolean hasContext();

    public native boolean hasPosition();

    public native boolean hasTextDocument();

    public native Uint8Array serializeBinary();

    public native void setConsoleId();

    public native void setConsoleId(Ticket value);

    public native void setContext();

    public native void setContext(CompletionContext value);

    public native void setPosition();

    public native void setPosition(Position value);

    public native void setRequestId(int value);

    public native void setTextDocument();

    public native void setTextDocument(VersionedTextDocumentIdentifier value);

    public native GetCompletionItemsRequest.ToObjectReturnType0 toObject();

    public native GetCompletionItemsRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
