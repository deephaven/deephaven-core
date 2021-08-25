package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import org.immutables.value.Value;
import oshi.hardware.Baseboard;

/**
 * The Baseboard represents the system board, also called motherboard, logic board, etc.
 */
@Value.Immutable
@ProcessStyle
public abstract class BaseboardOshi implements PropertySet {

    private static final String MANUFACTURER = "manufacturer";
    private static final String MODEL = "model";
    private static final String VERSION = "version";
    private static final String SERIAL_NUMBER = "serial";

    /**
     * Get the baseboard manufacturer.
     *
     * @return The manufacturer.
     */
    @Value.Parameter
    public abstract String getManufacturer();

    /**
     * Get the baseboard model.
     *
     * @return The model.
     */
    @Value.Parameter
    public abstract String getModel();

    /**
     * Get the baseboard version.
     *
     * @return The version.
     */
    @Value.Parameter
    public abstract String getVersion();

    /**
     * Get the baseboard serial number
     *
     * @return The serial number.
     */
    @Value.Parameter
    public abstract String getSerialNumber();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visit(MANUFACTURER, getManufacturer());
        visitor.visit(MODEL, getModel());
        visitor.visit(VERSION, getVersion());
        visitor.visit(SERIAL_NUMBER, getSerialNumber());
    }

    public static BaseboardOshi from(Baseboard baseboard) {
        return ImmutableBaseboardOshi.builder()
            .manufacturer(baseboard.getManufacturer())
            .model(baseboard.getModel())
            .version(baseboard.getVersion())
            .serialNumber(baseboard.getSerialNumber())
            .build();
    }

    /*
     * public static BaseboardOshi from(PropertySet properties) { return
     * Parser.INSTANCE.parse(properties); }
     * 
     * enum Parser implements PropertySetParser<BaseboardOshi> { INSTANCE;
     * 
     * @Override public BaseboardOshi parse(PropertySet properties) { final Visitor builder = new
     * Visitor(); builder.visitProperties(properties); return builder.build(); } }
     * 
     * static class Visitor implements PropertyVisitor {
     * 
     * private final ImmutableBaseboardOshi.Builder builder = ImmutableBaseboardOshi.builder();
     * 
     * BaseboardOshi build() { return builder.build(); }
     * 
     * @Override public void visit(String key, String value) { switch (key) { case MANUFACTURER:
     * builder.manufacturer(value); break; case MODEL: builder.model(value); break; case VERSION:
     * builder.version(value); break; case SERIAL_NUMBER: builder.serialNumber(value); break;
     * default: Error.INSTANCE.visit(key, value); } }
     * 
     * @Override public void visit(String key, int value) { Error.INSTANCE.visit(key, value); }
     * 
     * @Override public void visit(String key, long value) { Error.INSTANCE.visit(key, value); }
     * 
     * @Override public void visit(String key, boolean value) { Error.INSTANCE.visit(key, value); }
     * }
     */
}
