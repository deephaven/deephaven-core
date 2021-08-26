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
        name = "dhinternal.io.deephaven.proto.console_pb.GetCompletionItemsResponse",
        namespace = JsPackage.GLOBAL)
public class GetCompletionItemsResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ItemsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TextEditFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface RangeFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface StartFieldType {
                        @JsOverlay
                        static GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType create() {
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
                    static GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TextEditFieldType.RangeFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getEnd();

                    @JsProperty
                    GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType getStart();

                    @JsProperty
                    void setEnd(Object end);

                    @JsProperty
                    void setStart(
                            GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType start);
                }

                @JsOverlay
                static GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TextEditFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TextEditFieldType.RangeFieldType getRange();

                @JsProperty
                String getText();

                @JsProperty
                void setRange(
                        GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TextEditFieldType.RangeFieldType range);

                @JsProperty
                void setText(String text);
            }

            @JsOverlay
            static GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType create() {
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
            GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TextEditFieldType getTextEdit();

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
                    GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TextEditFieldType textEdit);
        }

        @JsOverlay
        static GetCompletionItemsResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType> getItemsList();

        @JsProperty
        double getRequestId();

        @JsProperty
        boolean isSuccess();

        @JsOverlay
        default void setItemsList(
                GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType[] itemsList) {
            setItemsList(
                    Js.<JsArray<GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType>>uncheckedCast(
                            itemsList));
        }

        @JsProperty
        void setItemsList(
                JsArray<GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType> itemsList);

        @JsProperty
        void setRequestId(double requestId);

        @JsProperty
        void setSuccess(boolean success);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ItemsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TextEditFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface RangeFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface StartFieldType {
                        @JsOverlay
                        static GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType create() {
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
                    static GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TextEditFieldType.RangeFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getEnd();

                    @JsProperty
                    GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType getStart();

                    @JsProperty
                    void setEnd(Object end);

                    @JsProperty
                    void setStart(
                            GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TextEditFieldType.RangeFieldType.StartFieldType start);
                }

                @JsOverlay
                static GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TextEditFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TextEditFieldType.RangeFieldType getRange();

                @JsProperty
                String getText();

                @JsProperty
                void setRange(
                        GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TextEditFieldType.RangeFieldType range);

                @JsProperty
                void setText(String text);
            }

            @JsOverlay
            static GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType create() {
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
            GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TextEditFieldType getTextEdit();

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
                    GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TextEditFieldType textEdit);
        }

        @JsOverlay
        static GetCompletionItemsResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType> getItemsList();

        @JsProperty
        double getRequestId();

        @JsProperty
        boolean isSuccess();

        @JsOverlay
        default void setItemsList(
                GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType[] itemsList) {
            setItemsList(
                    Js.<JsArray<GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType>>uncheckedCast(
                            itemsList));
        }

        @JsProperty
        void setItemsList(
                JsArray<GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType> itemsList);

        @JsProperty
        void setRequestId(double requestId);

        @JsProperty
        void setSuccess(boolean success);
    }

    public static native GetCompletionItemsResponse deserializeBinary(Uint8Array bytes);

    public static native GetCompletionItemsResponse deserializeBinaryFromReader(
            GetCompletionItemsResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            GetCompletionItemsResponse message, Object writer);

    public static native GetCompletionItemsResponse.ToObjectReturnType toObject(
            boolean includeInstance, GetCompletionItemsResponse msg);

    public native CompletionItem addItems();

    public native CompletionItem addItems(CompletionItem value, double index);

    public native CompletionItem addItems(CompletionItem value);

    public native void clearItemsList();

    public native JsArray<CompletionItem> getItemsList();

    public native int getRequestId();

    public native boolean getSuccess();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setItemsList(CompletionItem[] value) {
        setItemsList(Js.<JsArray<CompletionItem>>uncheckedCast(value));
    }

    public native void setItemsList(JsArray<CompletionItem> value);

    public native void setRequestId(int value);

    public native void setSuccess(boolean value);

    public native GetCompletionItemsResponse.ToObjectReturnType0 toObject();

    public native GetCompletionItemsResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
