package io.deephaven.numerics.derivatives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.util.QueryConstants;
import org.junit.Test;

/**
 * Most of the values for the testcases are taken from the book "The Complete Guide to Option Pricing Formula" (Chapter
 * 2 and 12)
 *
 */
public class TestBlackScholes extends BaseArrayTestCase {

    @Test
    public void testPrices() {
        // price
        assertEquals(12.43, Math.round(BlackScholes.price(true, 105, 100, 0.5, 0.1, 0, 0.36) * 100d) / 100d);
        assertEquals(7.68, Math.round(BlackScholes.price(false, 105, 100, 0.5, 0.1, 0, 0.36) * 100d) / 100d);
    }

    @Test
    public void testDelta() {
        // delta
        assertEquals(0.5946, Math.round(BlackScholes.delta(true, 105, 100, 0.5, 0.1, 0, 0.36) * 10000d) / 10000d);
        assertEquals(-0.3566, Math.round(BlackScholes.delta(false, 105, 100, 0.5, 0.1, 0, 0.36) * 10000d) / 10000d);
        // finite difference approximation
        assertEquals(Math.round(BlackScholes.delta(true, 105, 100, 0.5, 0.1, 0, 0.36) * 10000d) / 10000d,
                Math.round((BlackScholes.price(true, 105.0005, 100, 0.5, 0.1, 0, 0.36) -
                        BlackScholes.price(true, 105, 100, 0.5, 0.1, 0, 0.36)) * 2000d * 10000d) / 10000d);
        assertEquals(Math.round(BlackScholes.delta(false, 105, 100, 0.5, 0.1, 0, 0.36) * 10000d) / 10000d,
                Math.round((BlackScholes.price(false, 105.0005, 100, 0.5, 0.1, 0, 0.36) -
                        BlackScholes.price(false, 105, 100, 0.5, 0.1, 0, 0.36)) * 2000d * 10000d) / 10000d);
    }

    @Test
    public void testGamma() {
        // gamma
        assertEquals(0.0278, Math.round(BlackScholes.gamma(55, 60, 0.75, 0.1, 0.1, 0.3) * 10000d) / 10000d);
        assertEquals(Math.round(0.0278 * 55 * 10000) / 10000d,
                Math.round(BlackScholes.gammaP(55, 60, 0.75, 0.1, 0.1, 0.3) * 10000d) / 10000d, 2e-3);
        // finite difference approximation
        assertEquals(Math.round(BlackScholes.gamma(55, 60, 0.75, 0.1, 0.1, 0.3) * 10000d) / 10000d,
                Math.round((BlackScholes.delta(true, 55.0005, 60, 0.75, 0.1, 0.1, 0.3) -
                        BlackScholes.delta(true, 55, 60, 0.75, 0.1, 0.1, 0.3)) * 2000d * 10000d) / 10000d);
        assertEquals(Math.round(BlackScholes.gamma(55, 60, 0.75, 0.1, 0.1, 0.3) * 10000d) / 10000d,
                Math.round((BlackScholes.delta(false, 55.0005, 60, 0.75, 0.1, 0.1, 0.3) -
                        BlackScholes.delta(false, 55, 60, 0.75, 0.1, 0.1, 0.3)) * 2000d * 10000d) / 10000d);

        assertEquals(BlackScholes.gammaP(55, 60, 0.75, 0.1, 0.1, 0.3),
                (BlackScholes.delta(true, 55.005, 60, 0.75, 0.1, 0.1, 0.3) -
                        BlackScholes.delta(true, 55, 60, 0.75, 0.1, 0.1, 0.3)) / (Math.log(55.005) - Math.log(55.00)),
                1e-4);
        assertEquals(BlackScholes.gammaP(55, 60, 0.75, 0.1, 0.1, 0.3),
                (BlackScholes.delta(false, 55.005, 60, 0.75, 0.1, 0.1, 0.3) -
                        BlackScholes.delta(false, 55, 60, 0.75, 0.1, 0.1, 0.3)) / (Math.log(55.005) - Math.log(55.00)),
                1e-4);
    }

    @Test
    public void testVega() {
        // vega
        assertEquals(18.5027, Math.round(BlackScholes.vega(55, 60, 0.75, 0.105, 0.0695, 0.3) * 10000d) / 10000d);
        assertEquals(Math.round(18.5027 * 0.3 * 10000d) / 10000d,
                Math.round(BlackScholes.vegaP(55, 60, 0.75, 0.105, 0.0695, 0.3) * 10000d) / 10000d);
        // not found in online options pricing calculator
        assertEquals(-11.7142, Math.round(BlackScholes.vegaBleed(55, 60, 0.75, 0.105, 0.0695, 0.3) * 10000d) / 10000d);

        // finite difference approximation
        assertEquals(
                Math.round(BlackScholes.vega(55, 60, 0.75, 0.105, 0.0695, 0.3) * 1000d) / 1000d, Math
                        .round((BlackScholes.price(true, 55, 60, 0.75, 0.105, 0.0695, 0.3000000001) -
                                BlackScholes.price(true, 55, 60, 0.75, 0.105, 0.0695, 0.3)) * 10000000000d * 1000d)
                        / 1000d);
        assertEquals(
                Math.round(BlackScholes.vega(55, 60, 0.75, 0.105, 0.0695, 0.3) * 1000d) / 1000d, Math
                        .round((BlackScholes.price(false, 55, 60, 0.75, 0.105, 0.0695, 0.3000000001) -
                                BlackScholes.price(false, 55, 60, 0.75, 0.105, 0.0695, 0.3)) * 10000000000d * 1000d)
                        / 1000d);

        assertEquals(BlackScholes.vegaP(55, 60, 0.75, 0.105, 0.0695, 0.3),
                (BlackScholes.price(true, 55, 60, 0.75, 0.105, 0.0695, 0.30001) -
                        BlackScholes.price(true, 55, 60, 0.75, 0.105, 0.0695, 0.3))
                        / (Math.log(0.30001) - Math.log(0.30)),
                1e-3);
        assertEquals(BlackScholes.vegaP(55, 60, 0.75, 0.105, 0.0695, 0.3),
                (BlackScholes.price(false, 55, 60, 0.75, 0.105, 0.0695, 0.30001) -
                        BlackScholes.price(false, 55, 60, 0.75, 0.105, 0.0695, 0.3))
                        / (Math.log(0.30001) - Math.log(0.30)),
                1e-3);

        assertEquals(Math.round(BlackScholes.vegaBleed(55, 60, 0.75, 0.105, 0.0695, 0.3) * 10000d) / 10000d,
                Math.round((BlackScholes.vega(55, 60, 0.75, 0.105, 0.0695, 0.3) -
                        BlackScholes.vega(55, 60, 0.75001, 0.105, 0.0695, 0.3)) * 100000d * 10000d) / 10000d);
        assertEquals(Math.round(BlackScholes.vegaBleed(55, 60, 0.75, 0.105, 0.0695, 0.3) * 10000d) / 10000d,
                Math.round((BlackScholes.vega(55, 60, 0.75, 0.105, 0.0695, 0.3) -
                        BlackScholes.vega(55, 60, 0.75001, 0.105, 0.0695, 0.3)) * 100000d * 10000d) / 10000d);
    }

    @Test
    public void testVomma() {
        // vomma
        assertEquals(92.3444, Math.round(BlackScholes.vomma(90, 130, 0.75, 0.05, 0, 0.28) * 10000d) / 10000d);
        assertEquals(2.5856 * 10, BlackScholes.vommaP(90, 130, 0.75, 0.05, 0, 0.28), 1e-3);

        // finite difference approximation
        assertEquals(Math.round(BlackScholes.vomma(90, 130, 0.75, 0.05, 0, 0.28) * 10000d) / 10000d,
                Math.round((BlackScholes.vega(90, 130, 0.75, 0.05, 0, 0.2800001) -
                        BlackScholes.vega(90, 130, 0.75, 0.05, 0, 0.28)) * 10000000d * 10000d) / 10000d);
        assertEquals(Math.round(BlackScholes.vomma(90, 130, 0.75, 0.05, 0, 0.28) * 10000d) / 10000d,
                Math.round((BlackScholes.vega(90, 130, 0.75, 0.05, 0, 0.2800001) -
                        BlackScholes.vega(90, 130, 0.75, 0.05, 0, 0.28)) * 10000000d * 10000d) / 10000d);

        assertEquals(BlackScholes.vommaP(90, 130, 0.75, 0.05, 0, 0.28),
                (BlackScholes.vega(90, 130, 0.75, 0.05, 0, 0.28001) - BlackScholes.vega(90, 130, 0.75, 0.05, 0, 0.28))
                        / (Math.log(0.28001) - Math.log(0.28)),
                1e-3);

    }

    @Test
    public void testCharm() {
        // charm - not found in online options pricing calculator
        assertEquals(0.5052, Math.round(BlackScholes.charm(true, 105, 90, 0.25, 0.14, 0, 0.24) * 10000d) / 10000d);
        assertEquals(0.3700, Math.round(BlackScholes.charm(false, 105, 90, 0.25, 0.14, 0, 0.24) * 10000d) / 10000d);

        // finite difference approximation
        assertEquals(
                Math.round(BlackScholes.charm(true, 105, 90, 0.25, 0.14, 0, 0.24) * 10000d) / 10000d, Math
                        .round((BlackScholes.delta(true, 105, 90, 0.25, 0.14, 0, 0.24) -
                                BlackScholes.delta(true, 105, 90, 0.25001, 0.14, 0, 0.24)) * 100000d * 10000d)
                        / 10000d);
        assertEquals(
                Math.round(BlackScholes.charm(false, 105, 90, 0.25, 0.14, 0, 0.24) * 10000d) / 10000d, Math
                        .round((BlackScholes.delta(false, 105, 90, 0.25, 0.14, 0, 0.24) -
                                BlackScholes.delta(false, 105, 90, 0.25001, 0.14, 0, 0.24)) * 100000d * 10000d)
                        / 10000d);
    }

    @Test
    public void testTheta() {
        // theta
        assertEquals(-37.9669,
                Math.round(BlackScholes.theta(true, 430, 405, 0.0833, 0.07, 0.02, 0.2) * 10000d) / 10000d);
        assertEquals(-31.1924,
                Math.round(BlackScholes.theta(false, 430, 405, 0.0833, 0.07, 0.02, 0.2) * 10000d) / 10000d);
        assertEquals(-32.6214,
                Math.round(BlackScholes.driftlessTheta(430, 405, 0.0833, 0.07, 0.02, 0.2) * 10000d) / 10000d);

        // finite difference approximation
        assertEquals(Math.round(BlackScholes.theta(true, 430, 405, 0.0833, 0.07, 0.02, 0.2) * 1000d) / 1000d,
                Math.round((BlackScholes.price(true, 430, 405, 0.0833, 0.07, 0.02,
                        0.2) - BlackScholes.price(true, 430, 405, 0.083301, 0.07, 0.02, 0.2)) * 1000000d * 1000d)
                        / 1000d);
        assertEquals(Math.round(BlackScholes.theta(false, 430, 405, 0.0833, 0.07, 0.02, 0.2) * 1000d) / 1000d,
                Math.round((BlackScholes.price(false, 430, 405, 0.0833, 0.07, 0.02,
                        0.2) - BlackScholes.price(false, 430, 405, 0.083301, 0.07, 0.02, 0.2)) * 1000000d * 1000d)
                        / 1000d);
    }

    @Test
    public void testRho() {
        // rho
        assertEquals(38.7325, Math.round(BlackScholes.rho(true, 72, 75, 1, 0.09, 0.09, 0.19) * 10000d) / 10000d);
        assertEquals(-29.81, Math.round(BlackScholes.rho(false, 72, 75, 1, 0.09, 0.09, 0.19) * 100d) / 100d);

        // finite difference approximation
        assertEquals(Math.round(BlackScholes.rho(true, 72, 75, 1, 0.09, 0.09, 0.19) * 1000d) / 1000d,
                Math.round((BlackScholes.price(true, 72, 75, 1, 0.09000001, 0.09000001, 0.19) -
                        BlackScholes.price(true, 72, 75, 1, 0.09, 0.09, 0.19)) * 100000000d * 1000d) / 1000d);
        assertEquals(
                Math.round(BlackScholes.rho(false, 72, 75, 1, 0.09, 0.09, 0.19) * 1000d) / 1000d, Math
                        .round((BlackScholes.price(false, 72, 75, 1, 0.09000001, 0.09000001,
                                0.19) - BlackScholes.price(false, 72, 75, 1, 0.09, 0.09, 0.19)) * 100000000d * 1000d)
                        / 1000d);
    }

    @Test
    public void testCarryRho() {
        // carryRho - not found in online options pricing calculator
        assertEquals(81.2219,
                Math.round(BlackScholes.carryRho(true, 500, 490, 0.25, 0.08, 0.03, 0.15) * 10000d) / 10000d);
        assertEquals(-42.2254,
                Math.round(BlackScholes.carryRho(false, 500, 490, 0.25, 0.08, 0.03, 0.15) * 10000d) / 10000d);

        // finite difference approximation
        assertEquals(Math.round(BlackScholes.carryRho(true, 500, 490, 0.25, 0.08, 0.03, 0.15) * 1000d) / 1000d,
                Math.round((BlackScholes.price(true, 500, 490, 0.25, 0.08, 0.03000001,
                        0.15) - BlackScholes.price(true, 500, 490, 0.25, 0.08, 0.03, 0.15)) * 100000000d * 1000d)
                        / 1000d);
        assertEquals(Math.round(BlackScholes.carryRho(false, 500, 490, 0.25, 0.08, 0.03, 0.15) * 1000d) / 1000d,
                Math.round((BlackScholes.price(false, 500, 490, 0.25, 0.08,
                        0.03000001, 0.15) - BlackScholes.price(false, 500, 490, 0.25, 0.08, 0.03, 0.15)) * 100000000d
                        * 1000d) / 1000d);
    }

    @Test
    public void testStrikeDelta() {
        // strike-delta
        assertEquals(-0.5164,
                Math.round(BlackScholes.strikeDelta(true, 72, 75, 1, 0.09, 0.09, 0.19) * 10000d) / 10000d);
        assertEquals(0.3975,
                Math.round(BlackScholes.strikeDelta(false, 72, 75, 1, 0.09, 0.09, 0.19) * 10000d) / 10000d);

        // finite difference approximation
        assertEquals(Math.round(BlackScholes.strikeDelta(true, 72, 75, 1, 0.09, 0.09, 0.19) * 10000d) / 10000d,
                Math.round((BlackScholes.price(true, 72, 75.001, 1, 0.09, 0.09, 0.19) -
                        BlackScholes.price(true, 72, 75, 1, 0.09, 0.09, 0.19)) * 1000d * 10000d) / 10000d);
        assertEquals(
                Math.round(BlackScholes.strikeDelta(false, 72, 75, 1, 0.09, 0.09, 0.19) * 10000d) / 10000d, Math
                        .round((BlackScholes.price(false, 72, 75.001, 1, 0.09, 0.09,
                                0.19) - BlackScholes.price(false, 72, 75, 1, 0.09, 0.09, 0.19)) * 1000d * 10000d)
                        / 10000d);
    }

    @Test
    public void testStrikeFromDeltaBisect() {
        // strike from delta-bisect
        final double X = 1850;
        final double deltaCall = BlackScholes.delta(true, 1800, X, 0.25, 0.07, 0.04, 0.5);
        assertEquals(X,
                Math.round(BlackScholes.strikeFromDeltaBisect(deltaCall, true, 1800, 0.25, 0.07, 0.04, 0.5) * 10000d)
                        / 10000d);

        final double deltaPut = BlackScholes.delta(false, 1800, X, 0.25, 0.07, 0.04, 0.5);
        assertEquals(X,
                Math.round(BlackScholes.strikeFromDeltaBisect(deltaPut, false, 1800, 0.25, 0.07, 0.04, 0.5) * 10000d)
                        / 10000d);

        assertEquals(QueryConstants.NULL_DOUBLE,
                BlackScholes.strikeFromDeltaBisect(deltaCall, null, 59, 60, 0.25, 0.067, 0.067));
        assertEquals(QueryConstants.NULL_DOUBLE, BlackScholes.strikeFromDeltaBisect(deltaCall, true,
                QueryConstants.NULL_DOUBLE, 60, 0.25, 0.067, 0.067));
        assertEquals(QueryConstants.NULL_DOUBLE, BlackScholes.strikeFromDeltaBisect(deltaCall, true,
                QueryConstants.NULL_DOUBLE, 60, 0.25, 0.067, QueryConstants.NULL_DOUBLE));
        assertEquals(Double.NaN, BlackScholes.strikeFromDeltaBisect(deltaCall, true, 59, 60, Double.NaN, 0.067, 0.067));
        assertEquals(X, Math.round(
                BlackScholes.strikeFromDeltaBisect(deltaPut, false, 1800, 0.25, 0.07, 0.04, 0.5, 1e-3, 26) * 100d)
                / 100d);
    }

    @Test
    public void testImpliedVolatilityUsingBisection() {
        // implied volatility using bisection
        final double v = 0.25;
        final double priceCall = BlackScholes.price(true, 59, 60, 0.25, 0.067, 0.067, v);
        assertEquals(v, Math.round(BlackScholes.impliedVolBisect(priceCall, true, 59, 60, 0.25, 0.067, 0.067) * 10000d)
                / 10000d);


        final double pricePut = BlackScholes.price(false, 59, 60, 0.25, 0.067, 0.067, v);
        assertEquals(v, Math.round(BlackScholes.impliedVolBisect(pricePut, false, 59, 60, 0.25, 0.067, 0.067) * 10000d)
                / 10000d);
        assertEquals(v,
                Math.round(
                        BlackScholes.impliedVolBisect(pricePut, false, 59, 60, 0.25, 0.067, 0.067, 1e-4, 16) * 10000d)
                        / 10000d);

        assertEquals(QueryConstants.NULL_DOUBLE,
                BlackScholes.impliedVolBisect(pricePut, null, 59, 60, 0.25, 0.067, 0.067));
        assertEquals(QueryConstants.NULL_DOUBLE,
                BlackScholes.impliedVolBisect(pricePut, true, QueryConstants.NULL_DOUBLE, 60, 0.25, 0.067, 0.067));
        assertEquals(QueryConstants.NULL_DOUBLE, BlackScholes.impliedVolBisect(pricePut, true,
                QueryConstants.NULL_DOUBLE, 60, 0.25, 0.067, QueryConstants.NULL_DOUBLE));
        assertEquals(Double.NaN, BlackScholes.impliedVolBisect(pricePut, true, 59, 60, Double.NaN, 0.067, 0.067));
        assertEquals(0d, BlackScholes.impliedVolBisect(0, true, 59, 60, 0.25, 0.067, 0.067));
    }

    @Test
    public void testImpliedVolatilityUsingNewtonRaphson() {
        // implied volatility using Newton-Raphson
        final double v = 0.25;
        final double priceCall = BlackScholes.price(true, 59, 60, 0.25, 0.067, 0.067, v);
        assertEquals(v, Math.round(BlackScholes.impliedVolNewton(priceCall, true, 59, 60, 0.25, 0.067, 0.067) * 10000d)
                / 10000d);

        final double pricePut = BlackScholes.price(false, 59, 60, 0.25, 0.067, 0.067, v);
        assertEquals(v, Math.round(BlackScholes.impliedVolNewton(pricePut, false, 59, 60, 0.25, 0.067, 0.067) * 10000d)
                / 10000d);
        assertEquals(v,
                Math.round(BlackScholes.impliedVolNewton(pricePut, false, 59, 60, 0.25, 0.067, 0.067, 1e-4, 2) * 10000d)
                        / 10000d);

        assertEquals(QueryConstants.NULL_DOUBLE,
                BlackScholes.impliedVolBisect(pricePut, null, 59, 60, 0.25, 0.067, 0.067));
        assertEquals(QueryConstants.NULL_DOUBLE,
                BlackScholes.impliedVolNewton(pricePut, true, QueryConstants.NULL_DOUBLE, 60, 0.25, 0.067, 0.067));
        assertEquals(QueryConstants.NULL_DOUBLE, BlackScholes.impliedVolNewton(pricePut, true,
                QueryConstants.NULL_DOUBLE, 60, 0.25, 0.067, QueryConstants.NULL_DOUBLE));
        assertEquals(Double.NaN, BlackScholes.impliedVolNewton(pricePut, true, 59, 60, Double.NaN, 0.067, 0.067));
        assertEquals(0d, BlackScholes.impliedVolNewton(0, true, 59, 60, 0.25, 0.067, 0.067));
    }

    @Test
    public void testImpliedVolatilityUsingNewtonRaphsonP() {
        final double v = 0.25;
        final double priceCall = BlackScholes.price(true, 59, 60, 0.25, 0.067, 0.067, v);
        assertEquals(v, Math.round(BlackScholes.impliedVolNewtonP(priceCall, true, 59, 60, 0.25, 0.067, 0.067) * 10000d)
                / 10000d);
        assertEquals(v, Math.round(BlackScholes.impliedVolNewtonP(priceCall, true, 59, 60, 0.25, 0.067, 0.067) * 10000d)
                / 10000d);

        final double pricePut = BlackScholes.price(false, 59, 60, 0.25, 0.067, 0.067, v);
        assertEquals(v, Math.round(BlackScholes.impliedVolNewtonP(pricePut, false, 59, 60, 0.25, 0.067, 0.067) * 10000d)
                / 10000d);
        assertEquals(v,
                Math.round(
                        BlackScholes.impliedVolNewtonP(pricePut, false, 59, 60, 0.25, 0.067, 0.067, 1e-4, 3) * 10000d)
                        / 10000d);

        assertEquals(QueryConstants.NULL_DOUBLE,
                BlackScholes.impliedVolNewtonP(pricePut, null, 59, 60, 0.25, 0.067, 0.067));
        assertEquals(QueryConstants.NULL_DOUBLE,
                BlackScholes.impliedVolNewtonP(pricePut, true, QueryConstants.NULL_DOUBLE, 60, 0.25, 0.067, 0.067));
        assertEquals(QueryConstants.NULL_DOUBLE, BlackScholes.impliedVolNewtonP(pricePut, true,
                QueryConstants.NULL_DOUBLE, 60, 0.25, 0.067, QueryConstants.NULL_DOUBLE));
        assertEquals(Double.NaN, BlackScholes.impliedVolNewtonP(pricePut, true, 59, 60, Double.NaN, 0.067, 0.067));
        assertEquals(0d, BlackScholes.impliedVolNewtonP(0, true, 59, 60, 0.25, 0.067, 0.067));
    }
}
