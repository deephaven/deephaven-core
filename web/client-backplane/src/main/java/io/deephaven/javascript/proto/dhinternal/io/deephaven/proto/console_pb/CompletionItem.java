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
        name = "dhinternal.io.deephaven.proto.console_pb.CompletionItem",
        namespace = JsPackage.GLOBAL)
public class CompletionItem {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextEditFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RangeFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface StartFieldType {
                    @JsOverlay
                    static CompletionItem.ToObjectReturnType.TextEditFieldType.RangeFieldType.StartFieldType create() {
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
                static CompletionItem.ToObjectReturnType.TextEditFieldType.RangeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getEnd();

                @JsProperty
                CompletionItem.ToObjectReturnType.TextEditFieldType.RangeFieldType.StartFieldType getStart();

                @JsProperty
                void setEnd(Object end);

                @JsProperty
                void setStart(
                        CompletionItem.ToObjectReturnType.TextEditFieldType.RangeFieldType.StartFieldType start);
            }

            @JsOverlay
            static CompletionItem.ToObjectReturnType.TextEditFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CompletionItem.ToObjectReturnType.TextEditFieldType.RangeFieldType getRange();

            @JsProperty
            String getText();

            @JsProperty
            void setRange(CompletionItem.ToObjectReturnType.TextEditFieldType.RangeFieldType range);

            @JsProperty
            void setText(String text);
        }

        @JsOverlay
        static CompletionItem.ToObjectReturnType create() {
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
        CompletionItem.ToObjectReturnType.TextEditFieldType getTextEdit();

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
        void setTextEdit(CompletionItem.ToObjectReturnType.TextEditFieldType textEdit);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextEditFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RangeFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface StartFieldType {
                    @JsOverlay
                    static CompletionItem.ToObjectReturnType0.TextEditFieldType.RangeFieldType.StartFieldType create() {
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
                static CompletionItem.ToObjectReturnType0.TextEditFieldType.RangeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getEnd();

                @JsProperty
                CompletionItem.ToObjectReturnType0.TextEditFieldType.RangeFieldType.StartFieldType getStart();

                @JsProperty
                void setEnd(Object end);

                @JsProperty
                void setStart(
                        CompletionItem.ToObjectReturnType0.TextEditFieldType.RangeFieldType.StartFieldType start);
            }

            @JsOverlay
            static CompletionItem.ToObjectReturnType0.TextEditFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CompletionItem.ToObjectReturnType0.TextEditFieldType.RangeFieldType getRange();

            @JsProperty
            String getText();

            @JsProperty
            void setRange(CompletionItem.ToObjectReturnType0.TextEditFieldType.RangeFieldType range);

            @JsProperty
            void setText(String text);
        }

        @JsOverlay
        static CompletionItem.ToObjectReturnType0 create() {
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
        CompletionItem.ToObjectReturnType0.TextEditFieldType getTextEdit();

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
        void setTextEdit(CompletionItem.ToObjectReturnType0.TextEditFieldType textEdit);
    }

    public static native CompletionItem deserializeBinary(Uint8Array bytes);

    public static native CompletionItem deserializeBinaryFromReader(
            CompletionItem message, Object reader);

    public static native void serializeBinaryToWriter(CompletionItem message, Object writer);

    public static native CompletionItem.ToObjectReturnType toObject(
            boolean includeInstance, CompletionItem msg);

    public native TextEdit addAdditionalTextEdits();

    public native TextEdit addAdditionalTextEdits(TextEdit value, double index);

    public native TextEdit addAdditionalTextEdits(TextEdit value);

    public native String addCommitCharacters(String value, double index);

    public native String addCommitCharacters(String value);

    public native void clearAdditionalTextEditsList();

    public native void clearCommitCharactersList();

    public native void clearTextEdit();

    public native JsArray<TextEdit> getAdditionalTextEditsList();

    public native JsArray<String> getCommitCharactersList();

    public native boolean getDeprecated();

    public native String getDetail();

    public native String getDocumentation();

    public native String getFilterText();

    public native double getInsertTextFormat();

    public native double getKind();

    public native String getLabel();

    public native double getLength();

    public native boolean getPreselect();

    public native String getSortText();

    public native double getStart();

    public native TextEdit getTextEdit();

    public native boolean hasTextEdit();

    public native Uint8Array serializeBinary();

    public native void setAdditionalTextEditsList(JsArray<TextEdit> value);

    @JsOverlay
    public final void setAdditionalTextEditsList(TextEdit[] value) {
        setAdditionalTextEditsList(Js.<JsArray<TextEdit>>uncheckedCast(value));
    }

    public native void setCommitCharactersList(JsArray<String> value);

    @JsOverlay
    public final void setCommitCharactersList(String[] value) {
        setCommitCharactersList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setDeprecated(boolean value);

    public native void setDetail(String value);

    public native void setDocumentation(String value);

    public native void setFilterText(String value);

    public native void setInsertTextFormat(double value);

    public native void setKind(double value);

    public native void setLabel(String value);

    public native void setLength(double value);

    public native void setPreselect(boolean value);

    public native void setSortText(String value);

    public native void setStart(double value);

    public native void setTextEdit();

    public native void setTextEdit(TextEdit value);

    public native CompletionItem.ToObjectReturnType0 toObject();

    public native CompletionItem.ToObjectReturnType0 toObject(boolean includeInstance);
}
