//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb;

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
        name = "dhinternal.io.deephaven_core.proto.console_pb.AutoCompleteResponse",
        namespace = JsPackage.GLOBAL)
public class AutoCompleteResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CompletionItemsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ItemsListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface DocumentationFieldType {
                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.DocumentationFieldType create() {
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
                public interface TextEditFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface RangeFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface StartFieldType {
                            @JsOverlay
                            static AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType create() {
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
                        static AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Object getEnd();

                        @JsProperty
                        AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType getStart();

                        @JsProperty
                        void setEnd(Object end);

                        @JsProperty
                        void setStart(
                                AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType start);
                    }

                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType getRange();

                    @JsProperty
                    String getText();

                    @JsProperty
                    void setRange(
                            AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType range);

                    @JsProperty
                    void setText(String text);
                }

                @JsOverlay
                static AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<Object> getAdditionalTextEditsList();

                @JsProperty
                JsArray<String> getCommitCharactersList();

                @JsProperty
                String getDetail();

                @JsProperty
                AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.DocumentationFieldType getDocumentation();

                @JsProperty
                String getFilterText();

                @JsProperty
                double getInsertTextFormat();

                @JsProperty
                double getKind();

                @JsProperty
                String getLabel();

                @JsProperty
                double getLength();

                @JsProperty
                String getSortText();

                @JsProperty
                double getStart();

                @JsProperty
                AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType getTextEdit();

                @JsProperty
                boolean isDeprecated();

                @JsProperty
                boolean isPreselect();

                @JsProperty
                void setAdditionalTextEditsList(JsArray<Object> additionalTextEditsList);

                @JsOverlay
                default void setAdditionalTextEditsList(Object[] additionalTextEditsList) {
                    setAdditionalTextEditsList(Js.<JsArray<Object>>uncheckedCast(additionalTextEditsList));
                }

                @JsProperty
                void setCommitCharactersList(JsArray<String> commitCharactersList);

                @JsOverlay
                default void setCommitCharactersList(String[] commitCharactersList) {
                    setCommitCharactersList(Js.<JsArray<String>>uncheckedCast(commitCharactersList));
                }

                @JsProperty
                void setDeprecated(boolean deprecated);

                @JsProperty
                void setDetail(String detail);

                @JsProperty
                void setDocumentation(
                        AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.DocumentationFieldType documentation);

                @JsProperty
                void setFilterText(String filterText);

                @JsProperty
                void setInsertTextFormat(double insertTextFormat);

                @JsProperty
                void setKind(double kind);

                @JsProperty
                void setLabel(String label);

                @JsProperty
                void setLength(double length);

                @JsProperty
                void setPreselect(boolean preselect);

                @JsProperty
                void setSortText(String sortText);

                @JsProperty
                void setStart(double start);

                @JsProperty
                void setTextEdit(
                        AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType textEdit);
            }

            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType> getItemsList();

            @JsProperty
            double getRequestId();

            @JsProperty
            boolean isSuccess();

            @JsOverlay
            default void setItemsList(
                    AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType[] itemsList) {
                setItemsList(
                        Js.<JsArray<AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType>>uncheckedCast(
                                itemsList));
            }

            @JsProperty
            void setItemsList(
                    JsArray<AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType.ItemsListFieldType> itemsList);

            @JsProperty
            void setRequestId(double requestId);

            @JsProperty
            void setSuccess(boolean success);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DiagnosticFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ItemsListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface CodeDescriptionFieldType {
                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType.CodeDescriptionFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getHref();

                    @JsProperty
                    void setHref(String href);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetDataUnionType {
                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType of(
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
                static AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getCode();

                @JsProperty
                AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType.CodeDescriptionFieldType getCodeDescription();

                @JsProperty
                AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType getData();

                @JsProperty
                String getMessage();

                @JsProperty
                Object getRange();

                @JsProperty
                double getSeverity();

                @JsProperty
                String getSource();

                @JsProperty
                JsArray<Double> getTagsList();

                @JsProperty
                void setCode(String code);

                @JsProperty
                void setCodeDescription(
                        AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType.CodeDescriptionFieldType codeDescription);

                @JsProperty
                void setData(
                        AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType data);

                @JsOverlay
                default void setData(String data) {
                    setData(
                            Js.<AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType>uncheckedCast(
                                    data));
                }

                @JsOverlay
                default void setData(Uint8Array data) {
                    setData(
                            Js.<AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType>uncheckedCast(
                                    data));
                }

                @JsProperty
                void setMessage(String message);

                @JsProperty
                void setRange(Object range);

                @JsProperty
                void setSeverity(double severity);

                @JsProperty
                void setSource(String source);

                @JsProperty
                void setTagsList(JsArray<Double> tagsList);

                @JsOverlay
                default void setTagsList(double[] tagsList) {
                    setTagsList(Js.<JsArray<Double>>uncheckedCast(tagsList));
                }
            }

            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType> getItemsList();

            @JsProperty
            String getKind();

            @JsProperty
            String getResultId();

            @JsOverlay
            default void setItemsList(
                    AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType[] itemsList) {
                setItemsList(
                        Js.<JsArray<AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType>>uncheckedCast(
                                itemsList));
            }

            @JsProperty
            void setItemsList(
                    JsArray<AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType.ItemsListFieldType> itemsList);

            @JsProperty
            void setKind(String kind);

            @JsProperty
            void setResultId(String resultId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DiagnosticPublishFieldType {
            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType.DiagnosticPublishFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Object> getDiagnosticsList();

            @JsProperty
            String getUri();

            @JsProperty
            double getVersion();

            @JsProperty
            void setDiagnosticsList(JsArray<Object> diagnosticsList);

            @JsOverlay
            default void setDiagnosticsList(Object[] diagnosticsList) {
                setDiagnosticsList(Js.<JsArray<Object>>uncheckedCast(diagnosticsList));
            }

            @JsProperty
            void setUri(String uri);

            @JsProperty
            void setVersion(double version);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface HoverFieldType {
            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType.HoverFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getContents();

            @JsProperty
            Object getRange();

            @JsProperty
            void setContents(Object contents);

            @JsProperty
            void setRange(Object range);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SignaturesFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SignaturesListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ParametersListFieldType {
                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType create() {
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
                static AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getActiveParameter();

                @JsProperty
                Object getDocumentation();

                @JsProperty
                String getLabel();

                @JsProperty
                JsArray<AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

                @JsProperty
                void setActiveParameter(double activeParameter);

                @JsProperty
                void setDocumentation(Object documentation);

                @JsProperty
                void setLabel(String label);

                @JsProperty
                void setParametersList(
                        JsArray<AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType> parametersList);

                @JsOverlay
                default void setParametersList(
                        AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                    setParametersList(
                            Js.<JsArray<AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                    parametersList));
                }
            }

            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getActiveParameter();

            @JsProperty
            double getActiveSignature();

            @JsProperty
            JsArray<AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType> getSignaturesList();

            @JsProperty
            void setActiveParameter(double activeParameter);

            @JsProperty
            void setActiveSignature(double activeSignature);

            @JsProperty
            void setSignaturesList(
                    JsArray<AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType> signaturesList);

            @JsOverlay
            default void setSignaturesList(
                    AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType[] signaturesList) {
                setSignaturesList(
                        Js.<JsArray<AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType.SignaturesListFieldType>>uncheckedCast(
                                signaturesList));
            }
        }

        @JsOverlay
        static AutoCompleteResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType getCompletionItems();

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType getDiagnostic();

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType.DiagnosticPublishFieldType getDiagnosticPublish();

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType.HoverFieldType getHover();

        @JsProperty
        double getRequestId();

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType getSignatures();

        @JsProperty
        boolean isSuccess();

        @JsProperty
        void setCompletionItems(
                AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType completionItems);

        @JsProperty
        void setDiagnostic(AutoCompleteResponse.ToObjectReturnType.DiagnosticFieldType diagnostic);

        @JsProperty
        void setDiagnosticPublish(
                AutoCompleteResponse.ToObjectReturnType.DiagnosticPublishFieldType diagnosticPublish);

        @JsProperty
        void setHover(AutoCompleteResponse.ToObjectReturnType.HoverFieldType hover);

        @JsProperty
        void setRequestId(double requestId);

        @JsProperty
        void setSignatures(AutoCompleteResponse.ToObjectReturnType.SignaturesFieldType signatures);

        @JsProperty
        void setSuccess(boolean success);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CompletionItemsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ItemsListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface DocumentationFieldType {
                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.DocumentationFieldType create() {
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
                public interface TextEditFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface RangeFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface StartFieldType {
                            @JsOverlay
                            static AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType create() {
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
                        static AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Object getEnd();

                        @JsProperty
                        AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType getStart();

                        @JsProperty
                        void setEnd(Object end);

                        @JsProperty
                        void setStart(
                                AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType start);
                    }

                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType getRange();

                    @JsProperty
                    String getText();

                    @JsProperty
                    void setRange(
                            AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType.RangeFieldType range);

                    @JsProperty
                    void setText(String text);
                }

                @JsOverlay
                static AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<Object> getAdditionalTextEditsList();

                @JsProperty
                JsArray<String> getCommitCharactersList();

                @JsProperty
                String getDetail();

                @JsProperty
                AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.DocumentationFieldType getDocumentation();

                @JsProperty
                String getFilterText();

                @JsProperty
                double getInsertTextFormat();

                @JsProperty
                double getKind();

                @JsProperty
                String getLabel();

                @JsProperty
                double getLength();

                @JsProperty
                String getSortText();

                @JsProperty
                double getStart();

                @JsProperty
                AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType getTextEdit();

                @JsProperty
                boolean isDeprecated();

                @JsProperty
                boolean isPreselect();

                @JsProperty
                void setAdditionalTextEditsList(JsArray<Object> additionalTextEditsList);

                @JsOverlay
                default void setAdditionalTextEditsList(Object[] additionalTextEditsList) {
                    setAdditionalTextEditsList(Js.<JsArray<Object>>uncheckedCast(additionalTextEditsList));
                }

                @JsProperty
                void setCommitCharactersList(JsArray<String> commitCharactersList);

                @JsOverlay
                default void setCommitCharactersList(String[] commitCharactersList) {
                    setCommitCharactersList(Js.<JsArray<String>>uncheckedCast(commitCharactersList));
                }

                @JsProperty
                void setDeprecated(boolean deprecated);

                @JsProperty
                void setDetail(String detail);

                @JsProperty
                void setDocumentation(
                        AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.DocumentationFieldType documentation);

                @JsProperty
                void setFilterText(String filterText);

                @JsProperty
                void setInsertTextFormat(double insertTextFormat);

                @JsProperty
                void setKind(double kind);

                @JsProperty
                void setLabel(String label);

                @JsProperty
                void setLength(double length);

                @JsProperty
                void setPreselect(boolean preselect);

                @JsProperty
                void setSortText(String sortText);

                @JsProperty
                void setStart(double start);

                @JsProperty
                void setTextEdit(
                        AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType.TextEditFieldType textEdit);
            }

            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType> getItemsList();

            @JsProperty
            double getRequestId();

            @JsProperty
            boolean isSuccess();

            @JsOverlay
            default void setItemsList(
                    AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType[] itemsList) {
                setItemsList(
                        Js.<JsArray<AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType>>uncheckedCast(
                                itemsList));
            }

            @JsProperty
            void setItemsList(
                    JsArray<AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType.ItemsListFieldType> itemsList);

            @JsProperty
            void setRequestId(double requestId);

            @JsProperty
            void setSuccess(boolean success);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DiagnosticFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ItemsListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface CodeDescriptionFieldType {
                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType.CodeDescriptionFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getHref();

                    @JsProperty
                    void setHref(String href);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetDataUnionType {
                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType of(
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
                static AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getCode();

                @JsProperty
                AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType.CodeDescriptionFieldType getCodeDescription();

                @JsProperty
                AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType getData();

                @JsProperty
                String getMessage();

                @JsProperty
                Object getRange();

                @JsProperty
                double getSeverity();

                @JsProperty
                String getSource();

                @JsProperty
                JsArray<Double> getTagsList();

                @JsProperty
                void setCode(String code);

                @JsProperty
                void setCodeDescription(
                        AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType.CodeDescriptionFieldType codeDescription);

                @JsProperty
                void setData(
                        AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType data);

                @JsOverlay
                default void setData(String data) {
                    setData(
                            Js.<AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType>uncheckedCast(
                                    data));
                }

                @JsOverlay
                default void setData(Uint8Array data) {
                    setData(
                            Js.<AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType.GetDataUnionType>uncheckedCast(
                                    data));
                }

                @JsProperty
                void setMessage(String message);

                @JsProperty
                void setRange(Object range);

                @JsProperty
                void setSeverity(double severity);

                @JsProperty
                void setSource(String source);

                @JsProperty
                void setTagsList(JsArray<Double> tagsList);

                @JsOverlay
                default void setTagsList(double[] tagsList) {
                    setTagsList(Js.<JsArray<Double>>uncheckedCast(tagsList));
                }
            }

            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType> getItemsList();

            @JsProperty
            String getKind();

            @JsProperty
            String getResultId();

            @JsOverlay
            default void setItemsList(
                    AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType[] itemsList) {
                setItemsList(
                        Js.<JsArray<AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType>>uncheckedCast(
                                itemsList));
            }

            @JsProperty
            void setItemsList(
                    JsArray<AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType.ItemsListFieldType> itemsList);

            @JsProperty
            void setKind(String kind);

            @JsProperty
            void setResultId(String resultId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DiagnosticPublishFieldType {
            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType0.DiagnosticPublishFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Object> getDiagnosticsList();

            @JsProperty
            String getUri();

            @JsProperty
            double getVersion();

            @JsProperty
            void setDiagnosticsList(JsArray<Object> diagnosticsList);

            @JsOverlay
            default void setDiagnosticsList(Object[] diagnosticsList) {
                setDiagnosticsList(Js.<JsArray<Object>>uncheckedCast(diagnosticsList));
            }

            @JsProperty
            void setUri(String uri);

            @JsProperty
            void setVersion(double version);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface HoverFieldType {
            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType0.HoverFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getContents();

            @JsProperty
            Object getRange();

            @JsProperty
            void setContents(Object contents);

            @JsProperty
            void setRange(Object range);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SignaturesFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SignaturesListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ParametersListFieldType {
                    @JsOverlay
                    static AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType create() {
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
                static AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getActiveParameter();

                @JsProperty
                Object getDocumentation();

                @JsProperty
                String getLabel();

                @JsProperty
                JsArray<AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

                @JsProperty
                void setActiveParameter(double activeParameter);

                @JsProperty
                void setDocumentation(Object documentation);

                @JsProperty
                void setLabel(String label);

                @JsProperty
                void setParametersList(
                        JsArray<AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType> parametersList);

                @JsOverlay
                default void setParametersList(
                        AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                    setParametersList(
                            Js.<JsArray<AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                    parametersList));
                }
            }

            @JsOverlay
            static AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getActiveParameter();

            @JsProperty
            double getActiveSignature();

            @JsProperty
            JsArray<AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType> getSignaturesList();

            @JsProperty
            void setActiveParameter(double activeParameter);

            @JsProperty
            void setActiveSignature(double activeSignature);

            @JsProperty
            void setSignaturesList(
                    JsArray<AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType> signaturesList);

            @JsOverlay
            default void setSignaturesList(
                    AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType[] signaturesList) {
                setSignaturesList(
                        Js.<JsArray<AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType.SignaturesListFieldType>>uncheckedCast(
                                signaturesList));
            }
        }

        @JsOverlay
        static AutoCompleteResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType getCompletionItems();

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType getDiagnostic();

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType0.DiagnosticPublishFieldType getDiagnosticPublish();

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType0.HoverFieldType getHover();

        @JsProperty
        double getRequestId();

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType getSignatures();

        @JsProperty
        boolean isSuccess();

        @JsProperty
        void setCompletionItems(
                AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType completionItems);

        @JsProperty
        void setDiagnostic(AutoCompleteResponse.ToObjectReturnType0.DiagnosticFieldType diagnostic);

        @JsProperty
        void setDiagnosticPublish(
                AutoCompleteResponse.ToObjectReturnType0.DiagnosticPublishFieldType diagnosticPublish);

        @JsProperty
        void setHover(AutoCompleteResponse.ToObjectReturnType0.HoverFieldType hover);

        @JsProperty
        void setRequestId(double requestId);

        @JsProperty
        void setSignatures(AutoCompleteResponse.ToObjectReturnType0.SignaturesFieldType signatures);

        @JsProperty
        void setSuccess(boolean success);
    }

    public static native AutoCompleteResponse deserializeBinary(Uint8Array bytes);

    public static native AutoCompleteResponse deserializeBinaryFromReader(
            AutoCompleteResponse message, Object reader);

    public static native void serializeBinaryToWriter(AutoCompleteResponse message, Object writer);

    public static native AutoCompleteResponse.ToObjectReturnType toObject(
            boolean includeInstance, AutoCompleteResponse msg);

    public native void clearCompletionItems();

    public native void clearDiagnostic();

    public native void clearDiagnosticPublish();

    public native void clearHover();

    public native void clearSignatures();

    public native GetCompletionItemsResponse getCompletionItems();

    public native GetPullDiagnosticResponse getDiagnostic();

    public native GetPublishDiagnosticResponse getDiagnosticPublish();

    public native GetHoverResponse getHover();

    public native int getRequestId();

    public native int getResponseCase();

    public native GetSignatureHelpResponse getSignatures();

    public native boolean getSuccess();

    public native boolean hasCompletionItems();

    public native boolean hasDiagnostic();

    public native boolean hasDiagnosticPublish();

    public native boolean hasHover();

    public native boolean hasSignatures();

    public native Uint8Array serializeBinary();

    public native void setCompletionItems();

    public native void setCompletionItems(GetCompletionItemsResponse value);

    public native void setDiagnostic();

    public native void setDiagnostic(GetPullDiagnosticResponse value);

    public native void setDiagnosticPublish();

    public native void setDiagnosticPublish(GetPublishDiagnosticResponse value);

    public native void setHover();

    public native void setHover(GetHoverResponse value);

    public native void setRequestId(int value);

    public native void setSignatures();

    public native void setSignatures(GetSignatureHelpResponse value);

    public native void setSuccess(boolean value);

    public native AutoCompleteResponse.ToObjectReturnType0 toObject();

    public native AutoCompleteResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
