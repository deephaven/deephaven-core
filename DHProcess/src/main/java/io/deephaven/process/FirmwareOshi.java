package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import org.immutables.value.Value;
import oshi.hardware.Firmware;

/**
 * The Firmware represents the low level BIOS or equivalent
 */
@Value.Immutable
@ProcessStyle
public abstract class FirmwareOshi implements PropertySet {

    private static final String MANUFACTURER = "manufacturer";
    private static final String NAME = "name";
    private static final String DESCRIPTION = "description";
    private static final String VERSION = "version";
    private static final String RELEASE_DATE = "release-date";

    /**
     * Get the firmware manufacturer.
     *
     * @return the manufacturer
     */
    @Value.Parameter
    public abstract String getManufacturer();

    /**
     * Get the firmware name.
     *
     * @return the name
     */
    @Value.Parameter
    public abstract String getName();

    /**
     * Get the firmware description.
     *
     * @return the description
     */
    @Value.Parameter
    public abstract String getDescription();

    /**
     * Get the firmware version.
     *
     * @return the version
     */
    @Value.Parameter
    public abstract String getVersion();

    /**
     * Get the firmware release date.
     *
     * @return The date in ISO 8601 YYYY-MM-DD format.
     */
    @Value.Parameter
    public abstract String getReleaseDate();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visit(MANUFACTURER, getManufacturer());
        visitor.visit(NAME, getName());
        visitor.visit(DESCRIPTION, getDescription());
        visitor.visit(VERSION, getVersion());
        visitor.visit(RELEASE_DATE, getReleaseDate());
    }

    public static FirmwareOshi from(Firmware firmware) {
        return ImmutableFirmwareOshi.builder()
                .manufacturer(firmware.getManufacturer())
                .name(firmware.getName())
                .description(firmware.getDescription())
                .version(firmware.getVersion())
                .releaseDate(firmware.getReleaseDate())
                .build();
    }

    /*
     * public static FirmwareOshi from(PropertySet properties) { return Parser.INSTANCE.parse(properties); }
     * 
     * enum Parser implements PropertySetParser<FirmwareOshi> { INSTANCE;
     * 
     * @Override public FirmwareOshi parse(PropertySet properties) { final Visitor parser = new Visitor();
     * parser.visitProperties(properties); return parser.build(); } }
     * 
     * static class Visitor implements PropertyVisitor {
     * 
     * private final ImmutableFirmwareOshi.Builder builder = ImmutableFirmwareOshi.builder();
     * 
     * FirmwareOshi build() { return builder.build(); }
     * 
     * @Override public void visit(String key, String value) { switch (key) { case MANUFACTURER:
     * builder.manufacturer(value); break; case NAME: builder.name(value); break; case DESCRIPTION:
     * builder.description(value); break; case VERSION: builder.version(value); break; case RELEASE_DATE:
     * builder.releaseDate(value); break; default: Error.INSTANCE.visit(key, value); } }
     * 
     * @Override public void visit(String key, int value) { Error.INSTANCE.visit(key, value); }
     * 
     * @Override public void visit(String key, long value) { Error.INSTANCE.visit(key, value); }
     * 
     * @Override public void visit(String key, boolean value) { Error.INSTANCE.visit(key, value); } }
     */
}
