package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import org.immutables.value.Value;
import oshi.hardware.ComputerSystem;

/**
 * The ComputerSystem represents the physical hardware, of a computer system/product and includes BIOS/firmware and a
 * motherboard, logic board, etc.
 */
@Value.Immutable
@ProcessStyle
public abstract class ComputerSystemOshi implements PropertySet {

    private static final String MANUFACTURER = "manufacturer";
    private static final String MODEL = "model";
    private static final String FIRMWARE = "firmware";
    private static final String BASEBOARD = "baseboard";

    /**
     * Get the computer system manufacturer.
     *
     * @return The manufacturer.
     */
    @Value.Parameter
    public abstract String getManufacturer();

    /**
     * Get the computer system model.
     *
     * @return The model.
     */
    @Value.Parameter
    public abstract String getModel();

    // todo: getSerialNumber() - don't like the impl, it's weak

    /**
     * Get the computer system firmware/BIOS
     *
     * @return A {@link FirmwareOshi} object for this system
     */
    @Value.Parameter
    public abstract FirmwareOshi getFirmware();

    /**
     * Get the computer system baseboard/motherboard
     *
     * @return A {@link BaseboardOshi} object for this system
     */
    @Value.Parameter
    public abstract BaseboardOshi getBaseboard();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visit(MANUFACTURER, getManufacturer());
        visitor.visit(MODEL, getManufacturer());
        visitor.visitProperties(FIRMWARE, getFirmware());
        visitor.visitProperties(BASEBOARD, getBaseboard());
    }

    public static ComputerSystemOshi from(ComputerSystem computerSystem) {
        return ImmutableComputerSystemOshi.builder()
                .manufacturer(computerSystem.getManufacturer())
                .model(computerSystem.getModel())
                .firmware(FirmwareOshi.from(computerSystem.getFirmware()))
                .baseboard(BaseboardOshi.from(computerSystem.getBaseboard()))
                .build();
    }

    /*
     * public static ComputerSystemOshi from(PropertySet properties) { return Parser.INSTANCE.parse(properties); }
     * 
     * enum Parser implements PropertySetParser<ComputerSystemOshi> { INSTANCE;
     * 
     * @Override public ComputerSystemOshi parse(PropertySet properties) { final ComputerSystemOshi.Visitor collector =
     * new ComputerSystemOshi.Visitor(); collector.visitProperties(properties); return collector.build(); } }
     * 
     * private static class Visitor implements PropertyVisitor {
     * 
     * private final ImmutableComputerSystemOshi.Builder builder = ImmutableComputerSystemOshi .builder(); private final
     * BaseboardOshi.Visitor baseboardBuilder = new BaseboardOshi.Visitor(); private final FirmwareOshi.Visitor
     * firmwareBuilder = new FirmwareOshi.Visitor();
     * 
     * 
     * ComputerSystemOshi build() { return builder .baseboard(baseboardBuilder.build())
     * .firmware(firmwareBuilder.build()) .build(); }
     * 
     * @Override public void visit(String key, String value) { switch (key) { case MANUFACTURER:
     * builder.manufacturer(value); return;
     * 
     * case MODEL: builder.model(value); return; } if (key.startsWith(BASEBOARD)) {
     * baseboardBuilder.stripPrefix(BASEBOARD).visit(key, value); return; } if (key.startsWith(FIRMWARE)) {
     * firmwareBuilder.stripPrefix(FIRMWARE).visit(key, value); return; } Error.INSTANCE.visit(key, value); }
     * 
     * @Override public void visit(String key, int value) { if (key.startsWith(BASEBOARD)) {
     * baseboardBuilder.stripPrefix(BASEBOARD).visit(key, value); return; } if (key.startsWith(FIRMWARE)) {
     * firmwareBuilder.stripPrefix(FIRMWARE).visit(key, value); return; } Error.INSTANCE.visit(key, value); }
     * 
     * @Override public void visit(String key, long value) { if (key.startsWith(BASEBOARD)) {
     * baseboardBuilder.stripPrefix(BASEBOARD).visit(key, value); return; } if (key.startsWith(FIRMWARE)) {
     * firmwareBuilder.stripPrefix(FIRMWARE).visit(key, value); return; } Error.INSTANCE.visit(key, value); }
     * 
     * @Override public void visit(String key, boolean value) { if (key.startsWith(BASEBOARD)) {
     * baseboardBuilder.stripPrefix(BASEBOARD).visit(key, value); return; } if (key.startsWith(FIRMWARE)) {
     * firmwareBuilder.stripPrefix(FIRMWARE).visit(key, value); return; } Error.INSTANCE.visit(key, value); } }
     */
}
