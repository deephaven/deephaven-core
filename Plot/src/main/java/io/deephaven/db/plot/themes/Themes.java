/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.themes;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.gui.color.Color;
import io.deephaven.db.plot.Font;
import io.deephaven.gui.color.Paint;
import io.deephaven.db.plot.Theme;
import io.deephaven.gui.color.ColorPalette;
import io.deephaven.gui.color.ColorPaletteAlgo;
import io.deephaven.gui.color.ColorPaletteAlgorithms;
import io.deephaven.gui.color.ColorPaletteArray;
import io.deephaven.db.tables.utils.DBNameValidator;
import io.deephaven.util.files.ResourceResolution;
import io.deephaven.internal.log.LoggerFactory;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Constructs {@link Theme}s from {@code .theme} XML files.
 *
 * The constructed {@link Theme}s are held in a {@link Map}
 * with {@link Theme} names as keys.
 */
public class Themes implements Map<String, Theme> {
    private static final Logger log = LoggerFactory.getLogger(Themes.class);
    private static final String SUFFIX = ".theme";
    private static final String THEME_PROP_INTERNAL = "Plot.theme.internalPath";
    private static final String THEME_PROP_USER = "Plot.theme.resourcePath";
    private static Themes instance = null;
    private static Theme DEFAULT_THEME = null;

    /**
     * Gets the singleton map of themes.
     *
     * @return singleton map of themes
     */
    static synchronized Themes getInstance() {
        if(instance == null){
            instance = new Themes();
        }

        return instance;
    }

    /**
     * Defines the default {@link Theme} used when one isn't specified.
     *
     * @param defaultTheme default theme
     */
    public static void setDefaultTheme( final Theme defaultTheme){
        DEFAULT_THEME = defaultTheme;
    }

    /**
     * Returns a specified theme.
     *
     * @param name name of the theme
     * @return theme
     * @throws IllegalArgumentException no theme matching {@code name}
     */
    public static Theme theme(final String name) {
        Require.neqNull(name, "name");

        if(!getInstance().containsKey(name)){
            throw new IllegalArgumentException("No such theme: " + name);
        }

        return getInstance().get(name);
    }

    /**
     * Returns the default theme.  The deault is specified by the {@code Plot.theme.default} property.
     *
     * @return default theme
     */
    public static Theme theme() {
        if( DEFAULT_THEME != null){
            return DEFAULT_THEME;
        }

        final String defaultName = Configuration.getInstance().getProperty("Plot.theme.default");
        return theme(defaultName);
    }

    /**
     * Returns the names of all available themess
     *
     * @return names of all available themes
     */
    public static String[] themeNames() {
        return getInstance().keySet().stream().toArray(String[]::new);
    }




    private final Map<String, Theme> instances = new HashMap<>();


    private Themes() {
        final Configuration configuration = Configuration.getInstance();


        loadProperty(configuration, THEME_PROP_INTERNAL);

        if(configuration.hasProperty(THEME_PROP_USER)){
            loadProperty(configuration, THEME_PROP_USER);
        }
    }

    private void loadProperty(final Configuration configuration, final String property) {
        final String locations = configuration.getProperty(property);
        try {
            load(configuration, locations);
        } catch (NoSuchFileException e) {
            log.warn().append("Problem loading themes. locations=").append(locations).append(e).endl();
        }
    }

    private void load(final Configuration configuration, final String chartThemeLocations) throws NoSuchFileException {
        final ResourceResolution resourceResolution = new io.deephaven.util.files.ResourceResolution(configuration, ";", chartThemeLocations);

        final BiConsumer<URL, String> consumer = (URL, filePath) -> {
            try {
                InputStream inputStream = URL.openStream();
                if (inputStream != null) {
                    addTheme(parseTheme(inputStream, URL.getFile()));
                } else {
                    log.warn("Could not open " + URL);
                    throw new RuntimeException("Could not open " + URL);
                }
            } catch (IOException e) {
                log.error("Problem loading theme: locations=" + chartThemeLocations, e);
                throw new RuntimeException("Problem loading theme: locations=" + chartThemeLocations, e);
            }
        };


        try {
            resourceResolution.findResources(SUFFIX, consumer);
        } catch (NoSuchFileException e){
            log.warn("Problem loading theme: locations=" + chartThemeLocations, e);
            throw e;
        } catch (IOException e) {
            log.warn("Problem loading theme: locations=" + chartThemeLocations, e);
            throw new RuntimeException("Problem loading theme: locations=" + chartThemeLocations, e);
        }
    }

    /**
     * Adds a theme to the collection from the {@code filePath}
     *
     * @param filePath must be chart format
     */
    @SuppressWarnings("WeakerAccess")
    public void addThemeFromFile(String filePath) {
        addThemeFromFile(new File(filePath));
    }

    /**
     * Adds a theme to the collection from the {@code file}
     *
     * @param file must be chart format
     */
    @SuppressWarnings("WeakerAccess")
    public void addThemeFromFile(File file) {
        if (file.getAbsolutePath().endsWith(SUFFIX)) {
            Theme theme;
            try {
                theme = parseTheme(new FileInputStream(file), file.getName());
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Could not find file: " + file.getName());
            }
            log.info().append("Loading theme: ").append(theme.getName()).append(" from file: ").append(file.toString());
            addTheme(theme);
        } else {
            throw new UnsupportedOperationException("Theme file must be in " + SUFFIX + " format");
        }
    }

    private void addTheme(final Theme theme){
        final String name = theme.getName().toUpperCase();
        try {
            DBNameValidator.validateQueryParameterName(name);
        } catch(DBNameValidator.InvalidNameException e){
            throw new IllegalArgumentException("Invalid name for theme: name='" + name + "'" );
        }

        if(containsKey(name)){
            final Theme oldTheme = get(name);
            if(oldTheme.equals(theme)) {
                return;
            } else {
                throw new IllegalArgumentException("Multiple themes have the same name: name='" + name + ";");
            }
        }

        put(name, theme);
    }

    @Override
    public int size() {
        return instances.size();
    }

    @Override
    public boolean isEmpty() {
        return instances.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return key != null && key.getClass().equals(String.class) && instances.containsKey(((String) key).toUpperCase());
    }

    @Override
    public boolean containsValue(Object value) {
        return instances.containsValue(value);
    }

    @Override
    public Theme get(Object key) {
        if(key == null) {
            return null;
        }

        return instances.get(key.toString().toUpperCase());
    }

    @Override
    public Theme put(String key, Theme value) {
        return instances.put(key.toUpperCase(), value);
    }

    @Override
    public Theme remove(Object key) {
        return instances.remove(key);
    }

    @Override
    public void putAll(@NotNull Map<? extends String, ? extends Theme> m) {
        for(final Entry<? extends String, ? extends Theme> e : m.entrySet()) {
            if (e.getKey() == null) {
                instances.put(null, e.getValue());
            } else {
                instances.put(e.getKey().toUpperCase(), e.getValue());
            }
        }
    }

    @Override
    public void clear() {
        instances.clear();
    }

    @NotNull
    @Override
    public Set<String> keySet() {
        return instances.keySet();
    }

    @NotNull
    @Override
    public Collection<Theme> values() {
        return instances.values();
    }

    @NotNull
    @Override
    public Set<Entry<String, Theme>> entrySet() {
        return Collections.unmodifiableSet(instances.entrySet());
    }

    private static Theme parseTheme(InputStream inputStream, String fileName) {
        final ThemeImpl theme = new ThemeImpl();

        final Document doc;
        try {
            final SAXBuilder builder = new SAXBuilder();
            doc = builder.build(inputStream);

        } catch (JDOMException e) {
            throw new IllegalArgumentException("Could not initialize theme: Error parsing " + fileName);
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize theme: " + fileName + " could not be loaded");
        }
        final Element root = doc.getRootElement();


        //get the theme name
        final Element nameElement = getChild(root,"Name",true);
        theme.name(nameElement.getValue().trim());

        assignTextElements(root, theme);

        assignPlotElements(root, theme);

        return theme;
    }

    private static Element getChild(final Element parent, final String child, final boolean requireOne) {
        final List<Element> childElements = parent.getChildren(child);
        Element childElement = null;
        if(childElements.size() == 1) {
            childElement = childElements.get(0);
        }

        if(requireOne && childElement == null) {
            throw new IllegalArgumentException("Must be exactly one child " + child + " to parent element " + parent.getName());
        }

        return childElement;
    }

    private static void assignTextElements(final Element root, final ThemeImpl theme) {
        final Element textElement = getChild(root, "Text",true);
        makeFont(textElement, "FigureTextFont", theme::figureTextFont, theme::figureTextColor);
        makeFont(textElement, "FigureTitleFont", theme::figureTitleFont, theme::figureTitleColor);
        makeFont(textElement, "ChartTitleFont", theme::chartTitleFont, theme::chartTitleColor);
        makeFont(textElement, "AxisTitleFont", theme::axisTitleFont, theme::axisTitleColor);
        makeFont(textElement, "TickValuesFont", theme::axisTicksFont, theme::axisTickLabelColor);
        makeFont(textElement, "LegendFont", theme::legendFont, theme::legendTextColor);
        makeFont(textElement, "PointLabelFont", theme::pointLabelFont, theme::pointLabelColor);
    }

    private static void assignPlotElements(final Element root, final ThemeImpl theme) {
        final Element plotElement = getChild(root, "PlotElements", true);
        makeColorAux(plotElement, "Background", theme::chartBackgroundColor);
        makeColorAux(plotElement, "Axis", theme::axisColor);
        createGridLines(plotElement, theme);
        makeSeriesColor(plotElement, theme::seriesColorGenerator);
    }

    private static void createGridLines(Element plotElement, final ThemeImpl theme) {
        makeColorAux(plotElement, "Grid", theme::gridLineColor);
        createGridLineVisibility(plotElement, "Grid", theme);
    }

    private static void createGridLineVisibility(Element plotElement, String grid, ThemeImpl theme) {
        final Element gridElement = getChild(plotElement, grid, false);

        if(gridElement == null) {
            return;
        }

        final Element visibilityElement = getChild(gridElement, "Visibility", false);
        final String domainString = getNamedItem(visibilityElement, "x");
        final String rangeString = getNamedItem(visibilityElement, "y");

        //null would be interpreted as false, which we don't want
        if(domainString != null) {
            theme.xGridLinesVisible(domainString);
        }
        if(rangeString != null) {
            theme.yGridLinesVisible(rangeString);
        }
    }

    private static void makeSeriesColor(final Element eElement, final Consumer<ColorPalette> setColorAlgo) {
        final Element seriesElement = eElement.getChild("Series");

        if (seriesElement != null) {
            Element algo = seriesElement.getChild("ColorPaletteAlgo");
            if (algo != null) {
                String algoName = algo.getText().trim();
                setColorAlgo.accept(new ColorPaletteAlgo(ColorPaletteAlgorithms.colorPaletteAlgorithm(algoName)));
                return;
            }

            algo = seriesElement.getChild("ColorPaletteArray");
            if (algo != null) {
                String algoName = algo.getText().trim();
                setColorAlgo.accept(new ColorPaletteArray(algoName));
                return;
            }

            final List<Color> colors = new ArrayList<>();
            for (Element colorElement : seriesElement.getChildren("Color")) {
                colors.add(getColor(colorElement));
            }
            setColorAlgo.accept(new ColorPaletteArray(colors.toArray(new Color[colors.size()])));
        }
    }

    private static void makeFont(Element element, String tagName, Consumer<Font> assignFont, Consumer<Paint> assignPaint) {
        final Element fontElement = element.getChild(tagName);

        if(fontElement != null) {
            final String name = getTextContent(fontElement, "Name");
            final String style = getTextContent(fontElement, "Style");
            final String size = getTextContent(fontElement, "Size");

            if (name != null && style != null && size != null) {
                Font.FontStyle fontStyle = Font.fontStyle(style);
                int x = Integer.parseInt(size);
                assignFont.accept(new Font(name, fontStyle, x));
            }

            makeColor(fontElement, assignPaint);
        }
    }

    private static String getTextContent(final Element element, final String name) {
        final Element child = getChild(element, name, false);
        return child == null ? null : child.getText().trim();
    }

    private static void makeColorAux(Element element, String tagName, Consumer<Paint> assignPaint) {
        final Element colorElement = getChild(element, tagName, false);
        if (colorElement != null) {
            makeColor(colorElement, assignPaint);
        }
    }

    private static void makeColor(Element eElement, Consumer<Paint> assignPaint) {
        final Element color = getChild(eElement, "Color", false);
        assignPaint.accept(getColor(color));
    }

    private static Color getColor(Element node) {
        final String rString = getNamedItem(node, "r");
        final String gString = getNamedItem(node, "g");
        final String bString = getNamedItem(node, "b");
        final String aString = getNamedItem(node, "a");

        if (rString != null && gString != null && bString != null) {
            int r = Integer.parseInt(rString);
            int g = Integer.parseInt(gString);
            int b = Integer.parseInt(bString);

            if (aString != null) {
                int a = Integer.parseInt(aString);
                return new Color(r, g, b, a);
            } else {
                return new Color(r, g, b);
            }
        }

        return null;
    }

    private static String getNamedItem(Element node, String itemName) {
        if(node == null) {
            return null;
        }

        final Attribute attribute = node.getAttribute(itemName);
        return attribute == null ? null : attribute.getValue().trim();
    }
}
