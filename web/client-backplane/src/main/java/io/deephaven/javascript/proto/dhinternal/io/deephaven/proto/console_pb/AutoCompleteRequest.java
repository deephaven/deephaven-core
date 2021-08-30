package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

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
        name = "dhinternal.io.deephaven.proto.console_pb.AutoCompleteRequest",
        namespace = JsPackage.GLOBAL)
public class AutoCompleteRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ChangeDocumentFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContentChangesListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface RangeFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface StartFieldType {
                        @JsOverlay
                        static AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType.StartFieldType create() {
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
                    static AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getEnd();

                    @JsProperty
                    AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType.StartFieldType getStart();

                    @JsProperty
                    void setEnd(Object end);

                    @JsProperty
                    void setStart(
                            AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType.StartFieldType start);
                }

                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType getRange();

                @JsProperty
                double getRangeLength();

                @JsProperty
                String getText();

                @JsProperty
                void setRange(
                        AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType range);

                @JsProperty
                void setRangeLength(double rangeLength);

                @JsProperty
                void setText(String text);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TextDocumentFieldType {
                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.TextDocumentFieldType create() {
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
            static AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getConsoleId();

            @JsProperty
            JsArray<AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType> getContentChangesList();

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.TextDocumentFieldType getTextDocument();

            @JsProperty
            void setConsoleId(Object consoleId);

            @JsOverlay
            default void setContentChangesList(
                    AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType[] contentChangesList) {
                setContentChangesList(
                        Js.<JsArray<AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType>>uncheckedCast(
                                contentChangesList));
            }

            @JsProperty
            void setContentChangesList(
                    JsArray<AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.ContentChangesListFieldType> contentChangesList);

            @JsProperty
            void setTextDocument(
                    AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType.TextDocumentFieldType textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CloseDocumentFieldType {
            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType.CloseDocumentFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getConsoleId();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setConsoleId(Object consoleId);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetCompletionItemsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContextFieldType {
                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType.GetCompletionItemsFieldType.ContextFieldType create() {
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

            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType.GetCompletionItemsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getConsoleId();

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType.GetCompletionItemsFieldType.ContextFieldType getContext();

            @JsProperty
            Object getPosition();

            @JsProperty
            double getRequestId();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setConsoleId(Object consoleId);

            @JsProperty
            void setContext(
                    AutoCompleteRequest.ToObjectReturnType.GetCompletionItemsFieldType.ContextFieldType context);

            @JsProperty
            void setPosition(Object position);

            @JsProperty
            void setRequestId(double requestId);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OpenDocumentFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ConsoleIdFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType of(
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
                static AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.ConsoleIdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TextDocumentFieldType {
                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.TextDocumentFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getLanguageId();

                @JsProperty
                String getText();

                @JsProperty
                String getUri();

                @JsProperty
                double getVersion();

                @JsProperty
                void setLanguageId(String languageId);

                @JsProperty
                void setText(String text);

                @JsProperty
                void setUri(String uri);

                @JsProperty
                void setVersion(double version);
            }

            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.ConsoleIdFieldType getConsoleId();

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.TextDocumentFieldType getTextDocument();

            @JsProperty
            void setConsoleId(
                    AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.ConsoleIdFieldType consoleId);

            @JsProperty
            void setTextDocument(
                    AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.TextDocumentFieldType textDocument);
        }

        @JsOverlay
        static AutoCompleteRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType getChangeDocument();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType.CloseDocumentFieldType getCloseDocument();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType.GetCompletionItemsFieldType getGetCompletionItems();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType getOpenDocument();

        @JsProperty
        void setChangeDocument(
                AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType changeDocument);

        @JsProperty
        void setCloseDocument(
                AutoCompleteRequest.ToObjectReturnType.CloseDocumentFieldType closeDocument);

        @JsProperty
        void setGetCompletionItems(
                AutoCompleteRequest.ToObjectReturnType.GetCompletionItemsFieldType getCompletionItems);

        @JsProperty
        void setOpenDocument(AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType openDocument);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ChangeDocumentFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContentChangesListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface RangeFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface StartFieldType {
                        @JsOverlay
                        static AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType.StartFieldType create() {
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
                    static AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getEnd();

                    @JsProperty
                    AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType.StartFieldType getStart();

                    @JsProperty
                    void setEnd(Object end);

                    @JsProperty
                    void setStart(
                            AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType.StartFieldType start);
                }

                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType getRange();

                @JsProperty
                double getRangeLength();

                @JsProperty
                String getText();

                @JsProperty
                void setRange(
                        AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType.RangeFieldType range);

                @JsProperty
                void setRangeLength(double rangeLength);

                @JsProperty
                void setText(String text);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TextDocumentFieldType {
                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.TextDocumentFieldType create() {
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
            static AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getConsoleId();

            @JsProperty
            JsArray<AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType> getContentChangesList();

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.TextDocumentFieldType getTextDocument();

            @JsProperty
            void setConsoleId(Object consoleId);

            @JsOverlay
            default void setContentChangesList(
                    AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType[] contentChangesList) {
                setContentChangesList(
                        Js.<JsArray<AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType>>uncheckedCast(
                                contentChangesList));
            }

            @JsProperty
            void setContentChangesList(
                    JsArray<AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.ContentChangesListFieldType> contentChangesList);

            @JsProperty
            void setTextDocument(
                    AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType.TextDocumentFieldType textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CloseDocumentFieldType {
            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType0.CloseDocumentFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getConsoleId();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setConsoleId(Object consoleId);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetCompletionItemsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContextFieldType {
                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType0.GetCompletionItemsFieldType.ContextFieldType create() {
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

            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType0.GetCompletionItemsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getConsoleId();

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType0.GetCompletionItemsFieldType.ContextFieldType getContext();

            @JsProperty
            Object getPosition();

            @JsProperty
            double getRequestId();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setConsoleId(Object consoleId);

            @JsProperty
            void setContext(
                    AutoCompleteRequest.ToObjectReturnType0.GetCompletionItemsFieldType.ContextFieldType context);

            @JsProperty
            void setPosition(Object position);

            @JsProperty
            void setRequestId(double requestId);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OpenDocumentFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ConsoleIdFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType of(
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
                static AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.ConsoleIdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TextDocumentFieldType {
                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.TextDocumentFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getLanguageId();

                @JsProperty
                String getText();

                @JsProperty
                String getUri();

                @JsProperty
                double getVersion();

                @JsProperty
                void setLanguageId(String languageId);

                @JsProperty
                void setText(String text);

                @JsProperty
                void setUri(String uri);

                @JsProperty
                void setVersion(double version);
            }

            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.ConsoleIdFieldType getConsoleId();

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.TextDocumentFieldType getTextDocument();

            @JsProperty
            void setConsoleId(
                    AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.ConsoleIdFieldType consoleId);

            @JsProperty
            void setTextDocument(
                    AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.TextDocumentFieldType textDocument);
        }

        @JsOverlay
        static AutoCompleteRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType getChangeDocument();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType0.CloseDocumentFieldType getCloseDocument();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType0.GetCompletionItemsFieldType getGetCompletionItems();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType getOpenDocument();

        @JsProperty
        void setChangeDocument(
                AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType changeDocument);

        @JsProperty
        void setCloseDocument(
                AutoCompleteRequest.ToObjectReturnType0.CloseDocumentFieldType closeDocument);

        @JsProperty
        void setGetCompletionItems(
                AutoCompleteRequest.ToObjectReturnType0.GetCompletionItemsFieldType getCompletionItems);

        @JsProperty
        void setOpenDocument(
                AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType openDocument);
    }

    public static native AutoCompleteRequest deserializeBinary(Uint8Array bytes);

    public static native AutoCompleteRequest deserializeBinaryFromReader(
            AutoCompleteRequest message, Object reader);

    public static native void serializeBinaryToWriter(AutoCompleteRequest message, Object writer);

    public static native AutoCompleteRequest.ToObjectReturnType toObject(
            boolean includeInstance, AutoCompleteRequest msg);

    public native void clearChangeDocument();

    public native void clearCloseDocument();

    public native void clearGetCompletionItems();

    public native void clearOpenDocument();

    public native ChangeDocumentRequest getChangeDocument();

    public native CloseDocumentRequest getCloseDocument();

    public native GetCompletionItemsRequest getGetCompletionItems();

    public native OpenDocumentRequest getOpenDocument();

    public native int getRequestCase();

    public native boolean hasChangeDocument();

    public native boolean hasCloseDocument();

    public native boolean hasGetCompletionItems();

    public native boolean hasOpenDocument();

    public native Uint8Array serializeBinary();

    public native void setChangeDocument();

    public native void setChangeDocument(ChangeDocumentRequest value);

    public native void setCloseDocument();

    public native void setCloseDocument(CloseDocumentRequest value);

    public native void setGetCompletionItems();

    public native void setGetCompletionItems(GetCompletionItemsRequest value);

    public native void setOpenDocument();

    public native void setOpenDocument(OpenDocumentRequest value);

    public native AutoCompleteRequest.ToObjectReturnType0 toObject();

    public native AutoCompleteRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
