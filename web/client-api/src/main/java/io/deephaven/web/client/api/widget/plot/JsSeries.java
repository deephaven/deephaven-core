package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.SeriesDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.SourceDescriptor;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.TableMap;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

import java.util.Arrays;
import java.util.Map;

@JsType
public class JsSeries {

    private final SeriesDescriptor descriptor;
    private final JsFigure jsFigure;

    private final SeriesDataSource[] sources;

    private boolean subscribed = true;
    private JsMultiSeries multiSeries;

    private DownsampleOptions downsample = DownsampleOptions.DEFAULT;

    private OneClick oneClick;

    @JsIgnore
    public JsSeries(SeriesDescriptor series, JsFigure jsFigure, Map<String, JsAxis> axes) {
        this.descriptor = series;
        this.jsFigure = jsFigure;

        this.sources = new SeriesDataSource[0];

        for (int i = 0; i < series.getDatasourcesList().length; i++) {
            SourceDescriptor dataSource = series.getDatasourcesList().getAt(i);
            sources[sources.length] = new SeriesDataSource(axes.get(dataSource.getAxis().getId()), dataSource);

            // set up oneclick if needed, make sure series make sense
            if (oneClick == null) {
                if (dataSource.hasOneclick()) {
                    assert i == 0;
                    oneClick = new OneClick(jsFigure, dataSource.getOneclick(), this);
                }
            } else {
                assert dataSource.hasOneclick();
                assert dataSource.getOneclick().equals(oneClick.getDescriptor());
            }
        }
        JsObject.freeze(sources);
    }

    /**
     * Post-construct initialization, once we have tables loaded, allowing js to get the type of the data that it will
     * be consuming. This is safe to do post-construction, since we don't actually return the JsFigure etc until tables
     * are loaded.
     */
    @JsIgnore
    public void initSources(Map<Integer, JsTable> tables, Map<Integer, TableMap> tableMaps) {
        Arrays.stream(sources).forEach(s -> s.initColumnType(tables));
        if (oneClick != null) {
            oneClick.setTableMap(tableMaps.get(sources[0].getDescriptor().getTablemapid()));
        }
    }

    /**
     * JS doesn't support method overloads, so we just ignore this one and mark the arg
     * as optional in the JS version.
     */
    @JsIgnore
    public void subscribe() {
        subscribe(null);
    }

    public void subscribe(@JsOptional DownsampleOptions forceDisableDownsample) {
        this.downsample = forceDisableDownsample == null ? DownsampleOptions.DEFAULT : forceDisableDownsample;
        subscribed = true;
        jsFigure.enqueueSubscriptionCheck();
    }
    public void unsubscribe() {
        markUnsubscribed();
        jsFigure.enqueueSubscriptionCheck();
    }

    @JsIgnore
    public void markUnsubscribed() {
        subscribed = false;
    }

    @JsIgnore
    public DownsampleOptions getDownsampleOptions() {
        return downsample;
    }

    @JsIgnore
    public boolean isSubscribed() {
        return subscribed;
    }

    @JsProperty
    @SuppressWarnings("unusable-by-js")
    public int getPlotStyle() {
        return descriptor.getPlotstyle();
    }

    @JsProperty
    public String getName() {
        return descriptor.getName();
    }

    @JsProperty(name = "isLinesVisible")
    public Boolean getLinesVisible() {
        return descriptor.getLinesvisible();
    }

    @JsProperty(name = "isShapesVisible")
    public Boolean getShapesVisible() {
        return descriptor.getShapesvisible();
    }

    @JsProperty
    public boolean isGradientVisible() {
        return descriptor.getGradientvisible();
    }

    @JsProperty
    public String getLineColor() {
        return descriptor.getLinecolor();
    }

    //TODO IDS-4139
//    @JsProperty
//    public String getLineStyle() {
//        return descriptor.getLineStyle();
//    }

    @JsProperty
    public String getPointLabelFormat() {
        return descriptor.getPointlabelformat();
    }

    @JsProperty
    public String getXToolTipPattern() {
        return descriptor.getXtooltippattern();
    }

    @JsProperty
    public String getYToolTipPattern() {
        return descriptor.getYtooltippattern();
    }

    @JsProperty
    public String getShapeLabel() {
        return descriptor.getShapelabel();
    }

    @JsProperty
    public Double getShapeSize() {
        return descriptor.getShapesize();
    }

    @JsProperty
    public String getShapeColor() {
        return descriptor.getShapecolor();
    }

    @JsProperty
    public String getShape() {
        return descriptor.getShape();
    }

    @JsProperty
    public SeriesDataSource[] getSources() {
        return sources;
    }

    @JsIgnore
    public SeriesDescriptor getDescriptor() {
        return descriptor;
    }

    @JsIgnore
    public void setMultiSeries(JsMultiSeries multiSeries) {
        this.multiSeries = multiSeries;
    }

    @JsProperty
    public JsMultiSeries getMultiSeries() {
        return multiSeries;
    }

    @JsProperty
    public OneClick getOneClick() {
        return oneClick;
    }
}
