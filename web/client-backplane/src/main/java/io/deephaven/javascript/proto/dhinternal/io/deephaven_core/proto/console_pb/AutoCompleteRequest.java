//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.AutoCompleteRequest",
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
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType of(
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
            static AutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
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
        public interface GetDiagnosticFieldType {
            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType.GetDiagnosticFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getIdentifier();

            @JsProperty
            String getPreviousResultId();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setIdentifier(String identifier);

            @JsProperty
            void setPreviousResultId(String previousResultId);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetHoverFieldType {
            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType.GetHoverFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getPosition();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setPosition(Object position);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetSignatureHelpFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContextFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ActiveSignatureHelpFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface SignaturesListFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface DocumentationFieldType {
                            @JsOverlay
                            static AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getKind();

                            @JsProperty
                            String getValue();

                            @JsProperty
                            void setKind(String kind);

                            @JsProperty
                            void setValue(String value);
                        }

                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface ParametersListFieldType {
                            @JsOverlay
                            static AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            Object getDocumentation();

                            @JsProperty
                            String getLabel();

                            @JsProperty
                            void setDocumentation(Object documentation);

                            @JsProperty
                            void setLabel(String label);
                        }

                        @JsOverlay
                        static AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getActiveParameter();

                        @JsProperty
                        AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType getDocumentation();

                        @JsProperty
                        String getLabel();

                        @JsProperty
                        JsArray<AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

                        @JsProperty
                        void setActiveParameter(double activeParameter);

                        @JsProperty
                        void setDocumentation(
                                AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType documentation);

                        @JsProperty
                        void setLabel(String label);

                        @JsProperty
                        void setParametersList(
                                JsArray<AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> parametersList);

                        @JsOverlay
                        default void setParametersList(
                                AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                            setParametersList(
                                    Js.<JsArray<AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                            parametersList));
                        }
                    }

                    @JsOverlay
                    static AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getActiveParameter();

                    @JsProperty
                    double getActiveSignature();

                    @JsProperty
                    JsArray<AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType> getSignaturesList();

                    @JsProperty
                    void setActiveParameter(double activeParameter);

                    @JsProperty
                    void setActiveSignature(double activeSignature);

                    @JsProperty
                    void setSignaturesList(
                            JsArray<AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType> signaturesList);

                    @JsOverlay
                    default void setSignaturesList(
                            AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType[] signaturesList) {
                        setSignaturesList(
                                Js.<JsArray<AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType>>uncheckedCast(
                                        signaturesList));
                    }
                }

                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType getActiveSignatureHelp();

                @JsProperty
                String getTriggerCharacter();

                @JsProperty
                double getTriggerKind();

                @JsProperty
                boolean isIsRetrigger();

                @JsProperty
                void setActiveSignatureHelp(
                        AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType activeSignatureHelp);

                @JsProperty
                void setIsRetrigger(boolean isRetrigger);

                @JsProperty
                void setTriggerCharacter(String triggerCharacter);

                @JsProperty
                void setTriggerKind(double triggerKind);
            }

            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType getContext();

            @JsProperty
            Object getPosition();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setContext(
                    AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType.ContextFieldType context);

            @JsProperty
            void setPosition(Object position);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OpenDocumentFieldType {
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
            Object getConsoleId();

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType.TextDocumentFieldType getTextDocument();

            @JsProperty
            void setConsoleId(Object consoleId);

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
        AutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType.GetCompletionItemsFieldType getGetCompletionItems();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType.GetDiagnosticFieldType getGetDiagnostic();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType.GetHoverFieldType getGetHover();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType getGetSignatureHelp();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType getOpenDocument();

        @JsProperty
        double getRequestId();

        @JsProperty
        void setChangeDocument(
                AutoCompleteRequest.ToObjectReturnType.ChangeDocumentFieldType changeDocument);

        @JsProperty
        void setCloseDocument(
                AutoCompleteRequest.ToObjectReturnType.CloseDocumentFieldType closeDocument);

        @JsProperty
        void setConsoleId(AutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);

        @JsProperty
        void setGetCompletionItems(
                AutoCompleteRequest.ToObjectReturnType.GetCompletionItemsFieldType getCompletionItems);

        @JsProperty
        void setGetDiagnostic(
                AutoCompleteRequest.ToObjectReturnType.GetDiagnosticFieldType getDiagnostic);

        @JsProperty
        void setGetHover(AutoCompleteRequest.ToObjectReturnType.GetHoverFieldType getHover);

        @JsProperty
        void setGetSignatureHelp(
                AutoCompleteRequest.ToObjectReturnType.GetSignatureHelpFieldType getSignatureHelp);

        @JsProperty
        void setOpenDocument(AutoCompleteRequest.ToObjectReturnType.OpenDocumentFieldType openDocument);

        @JsProperty
        void setRequestId(double requestId);
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
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType of(
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
            static AutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
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
        public interface GetDiagnosticFieldType {
            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType0.GetDiagnosticFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getIdentifier();

            @JsProperty
            String getPreviousResultId();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setIdentifier(String identifier);

            @JsProperty
            void setPreviousResultId(String previousResultId);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetHoverFieldType {
            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType0.GetHoverFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getPosition();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setPosition(Object position);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetSignatureHelpFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContextFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ActiveSignatureHelpFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface SignaturesListFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface DocumentationFieldType {
                            @JsOverlay
                            static AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getKind();

                            @JsProperty
                            String getValue();

                            @JsProperty
                            void setKind(String kind);

                            @JsProperty
                            void setValue(String value);
                        }

                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface ParametersListFieldType {
                            @JsOverlay
                            static AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            Object getDocumentation();

                            @JsProperty
                            String getLabel();

                            @JsProperty
                            void setDocumentation(Object documentation);

                            @JsProperty
                            void setLabel(String label);
                        }

                        @JsOverlay
                        static AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getActiveParameter();

                        @JsProperty
                        AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType getDocumentation();

                        @JsProperty
                        String getLabel();

                        @JsProperty
                        JsArray<AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

                        @JsProperty
                        void setActiveParameter(double activeParameter);

                        @JsProperty
                        void setDocumentation(
                                AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType documentation);

                        @JsProperty
                        void setLabel(String label);

                        @JsProperty
                        void setParametersList(
                                JsArray<AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> parametersList);

                        @JsOverlay
                        default void setParametersList(
                                AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                            setParametersList(
                                    Js.<JsArray<AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                            parametersList));
                        }
                    }

                    @JsOverlay
                    static AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getActiveParameter();

                    @JsProperty
                    double getActiveSignature();

                    @JsProperty
                    JsArray<AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType> getSignaturesList();

                    @JsProperty
                    void setActiveParameter(double activeParameter);

                    @JsProperty
                    void setActiveSignature(double activeSignature);

                    @JsProperty
                    void setSignaturesList(
                            JsArray<AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType> signaturesList);

                    @JsOverlay
                    default void setSignaturesList(
                            AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType[] signaturesList) {
                        setSignaturesList(
                                Js.<JsArray<AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType>>uncheckedCast(
                                        signaturesList));
                    }
                }

                @JsOverlay
                static AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType getActiveSignatureHelp();

                @JsProperty
                String getTriggerCharacter();

                @JsProperty
                double getTriggerKind();

                @JsProperty
                boolean isIsRetrigger();

                @JsProperty
                void setActiveSignatureHelp(
                        AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType.ActiveSignatureHelpFieldType activeSignatureHelp);

                @JsProperty
                void setIsRetrigger(boolean isRetrigger);

                @JsProperty
                void setTriggerCharacter(String triggerCharacter);

                @JsProperty
                void setTriggerKind(double triggerKind);
            }

            @JsOverlay
            static AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType getContext();

            @JsProperty
            Object getPosition();

            @JsProperty
            Object getTextDocument();

            @JsProperty
            void setContext(
                    AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType.ContextFieldType context);

            @JsProperty
            void setPosition(Object position);

            @JsProperty
            void setTextDocument(Object textDocument);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OpenDocumentFieldType {
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
            Object getConsoleId();

            @JsProperty
            AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType.TextDocumentFieldType getTextDocument();

            @JsProperty
            void setConsoleId(Object consoleId);

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
        AutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType0.GetCompletionItemsFieldType getGetCompletionItems();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType0.GetDiagnosticFieldType getGetDiagnostic();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType0.GetHoverFieldType getGetHover();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType getGetSignatureHelp();

        @JsProperty
        AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType getOpenDocument();

        @JsProperty
        double getRequestId();

        @JsProperty
        void setChangeDocument(
                AutoCompleteRequest.ToObjectReturnType0.ChangeDocumentFieldType changeDocument);

        @JsProperty
        void setCloseDocument(
                AutoCompleteRequest.ToObjectReturnType0.CloseDocumentFieldType closeDocument);

        @JsProperty
        void setConsoleId(AutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);

        @JsProperty
        void setGetCompletionItems(
                AutoCompleteRequest.ToObjectReturnType0.GetCompletionItemsFieldType getCompletionItems);

        @JsProperty
        void setGetDiagnostic(
                AutoCompleteRequest.ToObjectReturnType0.GetDiagnosticFieldType getDiagnostic);

        @JsProperty
        void setGetHover(AutoCompleteRequest.ToObjectReturnType0.GetHoverFieldType getHover);

        @JsProperty
        void setGetSignatureHelp(
                AutoCompleteRequest.ToObjectReturnType0.GetSignatureHelpFieldType getSignatureHelp);

        @JsProperty
        void setOpenDocument(
                AutoCompleteRequest.ToObjectReturnType0.OpenDocumentFieldType openDocument);

        @JsProperty
        void setRequestId(double requestId);
    }

    public static native AutoCompleteRequest deserializeBinary(Uint8Array bytes);

    public static native AutoCompleteRequest deserializeBinaryFromReader(
            AutoCompleteRequest message, Object reader);

    public static native void serializeBinaryToWriter(AutoCompleteRequest message, Object writer);

    public static native AutoCompleteRequest.ToObjectReturnType toObject(
            boolean includeInstance, AutoCompleteRequest msg);

    public native void clearChangeDocument();

    public native void clearCloseDocument();

    public native void clearConsoleId();

    public native void clearGetCompletionItems();

    public native void clearGetDiagnostic();

    public native void clearGetHover();

    public native void clearGetSignatureHelp();

    public native void clearOpenDocument();

    public native ChangeDocumentRequest getChangeDocument();

    public native CloseDocumentRequest getCloseDocument();

    public native Ticket getConsoleId();

    public native GetCompletionItemsRequest getGetCompletionItems();

    public native GetDiagnosticRequest getGetDiagnostic();

    public native GetHoverRequest getGetHover();

    public native GetSignatureHelpRequest getGetSignatureHelp();

    public native OpenDocumentRequest getOpenDocument();

    public native int getRequestCase();

    public native int getRequestId();

    public native boolean hasChangeDocument();

    public native boolean hasCloseDocument();

    public native boolean hasConsoleId();

    public native boolean hasGetCompletionItems();

    public native boolean hasGetDiagnostic();

    public native boolean hasGetHover();

    public native boolean hasGetSignatureHelp();

    public native boolean hasOpenDocument();

    public native Uint8Array serializeBinary();

    public native void setChangeDocument();

    public native void setChangeDocument(ChangeDocumentRequest value);

    public native void setCloseDocument();

    public native void setCloseDocument(CloseDocumentRequest value);

    public native void setConsoleId();

    public native void setConsoleId(Ticket value);

    public native void setGetCompletionItems();

    public native void setGetCompletionItems(GetCompletionItemsRequest value);

    public native void setGetDiagnostic();

    public native void setGetDiagnostic(GetDiagnosticRequest value);

    public native void setGetHover();

    public native void setGetHover(GetHoverRequest value);

    public native void setGetSignatureHelp();

    public native void setGetSignatureHelp(GetSignatureHelpRequest value);

    public native void setOpenDocument();

    public native void setOpenDocument(OpenDocumentRequest value);

    public native void setRequestId(int value);

    public native AutoCompleteRequest.ToObjectReturnType0 toObject();

    public native AutoCompleteRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
