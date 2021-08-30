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
        name = "dhinternal.io.deephaven.proto.console_pb.AutoCompleteResponse",
        namespace = JsPackage.GLOBAL)
public class AutoCompleteResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CompletionItemsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ItemsListFieldType {
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
                String getDocumentation();

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
                void setDocumentation(String documentation);

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

        @JsOverlay
        static AutoCompleteResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType getCompletionItems();

        @JsProperty
        void setCompletionItems(
                AutoCompleteResponse.ToObjectReturnType.CompletionItemsFieldType completionItems);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CompletionItemsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ItemsListFieldType {
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
                String getDocumentation();

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
                void setDocumentation(String documentation);

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

        @JsOverlay
        static AutoCompleteResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType getCompletionItems();

        @JsProperty
        void setCompletionItems(
                AutoCompleteResponse.ToObjectReturnType0.CompletionItemsFieldType completionItems);
    }

    public static native AutoCompleteResponse deserializeBinary(Uint8Array bytes);

    public static native AutoCompleteResponse deserializeBinaryFromReader(
            AutoCompleteResponse message, Object reader);

    public static native void serializeBinaryToWriter(AutoCompleteResponse message, Object writer);

    public static native AutoCompleteResponse.ToObjectReturnType toObject(
            boolean includeInstance, AutoCompleteResponse msg);

    public native void clearCompletionItems();

    public native GetCompletionItemsResponse getCompletionItems();

    public native int getResponseCase();

    public native boolean hasCompletionItems();

    public native Uint8Array serializeBinary();

    public native void setCompletionItems();

    public native void setCompletionItems(GetCompletionItemsResponse value);

    public native AutoCompleteResponse.ToObjectReturnType0 toObject();

    public native AutoCompleteResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
