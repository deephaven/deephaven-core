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
    public interface TexteditFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface RangeFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface StartFieldType {
          @JsOverlay
          static CompletionItem.ToObjectReturnType.TexteditFieldType.RangeFieldType.StartFieldType
              create() {
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
        static CompletionItem.ToObjectReturnType.TexteditFieldType.RangeFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getEnd();

        @JsProperty
        CompletionItem.ToObjectReturnType.TexteditFieldType.RangeFieldType.StartFieldType
            getStart();

        @JsProperty
        void setEnd(Object end);

        @JsProperty
        void setStart(
            CompletionItem.ToObjectReturnType.TexteditFieldType.RangeFieldType.StartFieldType
                start);
      }

      @JsOverlay
      static CompletionItem.ToObjectReturnType.TexteditFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      CompletionItem.ToObjectReturnType.TexteditFieldType.RangeFieldType getRange();

      @JsProperty
      String getText();

      @JsProperty
      void setRange(CompletionItem.ToObjectReturnType.TexteditFieldType.RangeFieldType range);

      @JsProperty
      void setText(String text);
    }

    @JsOverlay
    static CompletionItem.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Object> getAdditionaltexteditsList();

    @JsProperty
    JsArray<String> getCommitcharactersList();

    @JsProperty
    String getDetail();

    @JsProperty
    String getDocumentation();

    @JsProperty
    String getFiltertext();

    @JsProperty
    double getInserttextformat();

    @JsProperty
    double getKind();

    @JsProperty
    String getLabel();

    @JsProperty
    double getLength();

    @JsProperty
    String getSorttext();

    @JsProperty
    double getStart();

    @JsProperty
    CompletionItem.ToObjectReturnType.TexteditFieldType getTextedit();

    @JsProperty
    boolean isDeprecated();

    @JsProperty
    boolean isPreselect();

    @JsProperty
    void setAdditionaltexteditsList(JsArray<Object> additionaltexteditsList);

    @JsOverlay
    default void setAdditionaltexteditsList(Object[] additionaltexteditsList) {
      setAdditionaltexteditsList(Js.<JsArray<Object>>uncheckedCast(additionaltexteditsList));
    }

    @JsProperty
    void setCommitcharactersList(JsArray<String> commitcharactersList);

    @JsOverlay
    default void setCommitcharactersList(String[] commitcharactersList) {
      setCommitcharactersList(Js.<JsArray<String>>uncheckedCast(commitcharactersList));
    }

    @JsProperty
    void setDeprecated(boolean deprecated);

    @JsProperty
    void setDetail(String detail);

    @JsProperty
    void setDocumentation(String documentation);

    @JsProperty
    void setFiltertext(String filtertext);

    @JsProperty
    void setInserttextformat(double inserttextformat);

    @JsProperty
    void setKind(double kind);

    @JsProperty
    void setLabel(String label);

    @JsProperty
    void setLength(double length);

    @JsProperty
    void setPreselect(boolean preselect);

    @JsProperty
    void setSorttext(String sorttext);

    @JsProperty
    void setStart(double start);

    @JsProperty
    void setTextedit(CompletionItem.ToObjectReturnType.TexteditFieldType textedit);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TexteditFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface RangeFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface StartFieldType {
          @JsOverlay
          static CompletionItem.ToObjectReturnType0.TexteditFieldType.RangeFieldType.StartFieldType
              create() {
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
        static CompletionItem.ToObjectReturnType0.TexteditFieldType.RangeFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getEnd();

        @JsProperty
        CompletionItem.ToObjectReturnType0.TexteditFieldType.RangeFieldType.StartFieldType
            getStart();

        @JsProperty
        void setEnd(Object end);

        @JsProperty
        void setStart(
            CompletionItem.ToObjectReturnType0.TexteditFieldType.RangeFieldType.StartFieldType
                start);
      }

      @JsOverlay
      static CompletionItem.ToObjectReturnType0.TexteditFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      CompletionItem.ToObjectReturnType0.TexteditFieldType.RangeFieldType getRange();

      @JsProperty
      String getText();

      @JsProperty
      void setRange(CompletionItem.ToObjectReturnType0.TexteditFieldType.RangeFieldType range);

      @JsProperty
      void setText(String text);
    }

    @JsOverlay
    static CompletionItem.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Object> getAdditionaltexteditsList();

    @JsProperty
    JsArray<String> getCommitcharactersList();

    @JsProperty
    String getDetail();

    @JsProperty
    String getDocumentation();

    @JsProperty
    String getFiltertext();

    @JsProperty
    double getInserttextformat();

    @JsProperty
    double getKind();

    @JsProperty
    String getLabel();

    @JsProperty
    double getLength();

    @JsProperty
    String getSorttext();

    @JsProperty
    double getStart();

    @JsProperty
    CompletionItem.ToObjectReturnType0.TexteditFieldType getTextedit();

    @JsProperty
    boolean isDeprecated();

    @JsProperty
    boolean isPreselect();

    @JsProperty
    void setAdditionaltexteditsList(JsArray<Object> additionaltexteditsList);

    @JsOverlay
    default void setAdditionaltexteditsList(Object[] additionaltexteditsList) {
      setAdditionaltexteditsList(Js.<JsArray<Object>>uncheckedCast(additionaltexteditsList));
    }

    @JsProperty
    void setCommitcharactersList(JsArray<String> commitcharactersList);

    @JsOverlay
    default void setCommitcharactersList(String[] commitcharactersList) {
      setCommitcharactersList(Js.<JsArray<String>>uncheckedCast(commitcharactersList));
    }

    @JsProperty
    void setDeprecated(boolean deprecated);

    @JsProperty
    void setDetail(String detail);

    @JsProperty
    void setDocumentation(String documentation);

    @JsProperty
    void setFiltertext(String filtertext);

    @JsProperty
    void setInserttextformat(double inserttextformat);

    @JsProperty
    void setKind(double kind);

    @JsProperty
    void setLabel(String label);

    @JsProperty
    void setLength(double length);

    @JsProperty
    void setPreselect(boolean preselect);

    @JsProperty
    void setSorttext(String sorttext);

    @JsProperty
    void setStart(double start);

    @JsProperty
    void setTextedit(CompletionItem.ToObjectReturnType0.TexteditFieldType textedit);
  }

  public static native CompletionItem deserializeBinary(Uint8Array bytes);

  public static native CompletionItem deserializeBinaryFromReader(
      CompletionItem message, Object reader);

  public static native void serializeBinaryToWriter(CompletionItem message, Object writer);

  public static native CompletionItem.ToObjectReturnType toObject(
      boolean includeInstance, CompletionItem msg);

  public native TextEdit addAdditionaltextedits();

  public native TextEdit addAdditionaltextedits(TextEdit value, double index);

  public native TextEdit addAdditionaltextedits(TextEdit value);

  public native String addCommitcharacters(String value, double index);

  public native String addCommitcharacters(String value);

  public native void clearAdditionaltexteditsList();

  public native void clearCommitcharactersList();

  public native void clearTextedit();

  public native JsArray<TextEdit> getAdditionaltexteditsList();

  public native JsArray<String> getCommitcharactersList();

  public native boolean getDeprecated();

  public native String getDetail();

  public native String getDocumentation();

  public native String getFiltertext();

  public native double getInserttextformat();

  public native double getKind();

  public native String getLabel();

  public native double getLength();

  public native boolean getPreselect();

  public native String getSorttext();

  public native double getStart();

  public native TextEdit getTextedit();

  public native boolean hasTextedit();

  public native Uint8Array serializeBinary();

  public native void setAdditionaltexteditsList(JsArray<TextEdit> value);

  @JsOverlay
  public final void setAdditionaltexteditsList(TextEdit[] value) {
    setAdditionaltexteditsList(Js.<JsArray<TextEdit>>uncheckedCast(value));
  }

  public native void setCommitcharactersList(JsArray<String> value);

  @JsOverlay
  public final void setCommitcharactersList(String[] value) {
    setCommitcharactersList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setDeprecated(boolean value);

  public native void setDetail(String value);

  public native void setDocumentation(String value);

  public native void setFiltertext(String value);

  public native void setInserttextformat(double value);

  public native void setKind(double value);

  public native void setLabel(String value);

  public native void setLength(double value);

  public native void setPreselect(boolean value);

  public native void setSorttext(String value);

  public native void setStart(double value);

  public native void setTextedit();

  public native void setTextedit(TextEdit value);

  public native CompletionItem.ToObjectReturnType0 toObject();

  public native CompletionItem.ToObjectReturnType0 toObject(boolean includeInstance);
}
