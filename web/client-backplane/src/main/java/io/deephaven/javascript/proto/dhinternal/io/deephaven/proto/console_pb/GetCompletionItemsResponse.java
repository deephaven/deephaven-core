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
      public interface TexteditFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface RangeFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface StartFieldType {
            @JsOverlay
            static GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType
                    .TexteditFieldType.RangeFieldType.StartFieldType
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
          static GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TexteditFieldType
                  .RangeFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          Object getEnd();

          @JsProperty
          GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TexteditFieldType
                  .RangeFieldType.StartFieldType
              getStart();

          @JsProperty
          void setEnd(Object end);

          @JsProperty
          void setStart(
              GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TexteditFieldType
                      .RangeFieldType.StartFieldType
                  start);
        }

        @JsOverlay
        static GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TexteditFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TexteditFieldType
                .RangeFieldType
            getRange();

        @JsProperty
        String getText();

        @JsProperty
        void setRange(
            GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TexteditFieldType
                    .RangeFieldType
                range);

        @JsProperty
        void setText(String text);
      }

      @JsOverlay
      static GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType create() {
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
      GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TexteditFieldType
          getTextedit();

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
      void setTextedit(
          GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType.TexteditFieldType
              textedit);
    }

    @JsOverlay
    static GetCompletionItemsResponse.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType> getItemsList();

    @JsOverlay
    default void setItemsList(
        GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType[] itemsList) {
      setItemsList(
          Js
              .<JsArray<GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType>>
                  uncheckedCast(itemsList));
    }

    @JsProperty
    void setItemsList(
        JsArray<GetCompletionItemsResponse.ToObjectReturnType.ItemsListFieldType> itemsList);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ItemsListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface TexteditFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface RangeFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface StartFieldType {
            @JsOverlay
            static GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType
                    .TexteditFieldType.RangeFieldType.StartFieldType
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
          static GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TexteditFieldType
                  .RangeFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          Object getEnd();

          @JsProperty
          GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TexteditFieldType
                  .RangeFieldType.StartFieldType
              getStart();

          @JsProperty
          void setEnd(Object end);

          @JsProperty
          void setStart(
              GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TexteditFieldType
                      .RangeFieldType.StartFieldType
                  start);
        }

        @JsOverlay
        static GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TexteditFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TexteditFieldType
                .RangeFieldType
            getRange();

        @JsProperty
        String getText();

        @JsProperty
        void setRange(
            GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TexteditFieldType
                    .RangeFieldType
                range);

        @JsProperty
        void setText(String text);
      }

      @JsOverlay
      static GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType create() {
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
      GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TexteditFieldType
          getTextedit();

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
      void setTextedit(
          GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType.TexteditFieldType
              textedit);
    }

    @JsOverlay
    static GetCompletionItemsResponse.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType> getItemsList();

    @JsOverlay
    default void setItemsList(
        GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType[] itemsList) {
      setItemsList(
          Js
              .<JsArray<GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType>>
                  uncheckedCast(itemsList));
    }

    @JsProperty
    void setItemsList(
        JsArray<GetCompletionItemsResponse.ToObjectReturnType0.ItemsListFieldType> itemsList);
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

  public native Uint8Array serializeBinary();

  @JsOverlay
  public final void setItemsList(CompletionItem[] value) {
    setItemsList(Js.<JsArray<CompletionItem>>uncheckedCast(value));
  }

  public native void setItemsList(JsArray<CompletionItem> value);

  public native GetCompletionItemsResponse.ToObjectReturnType0 toObject();

  public native GetCompletionItemsResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
