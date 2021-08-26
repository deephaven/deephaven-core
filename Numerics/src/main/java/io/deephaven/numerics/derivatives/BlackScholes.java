package io.deephaven.numerics.derivatives;

import org.apache.commons.math3.distribution.NormalDistribution;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * A generalized Black-Scholes model for European options.
 */
public class BlackScholes {
    private BlackScholes() {}

    private static final double EPS = 1e-8;
    private static final int MAX_ITERS = 10000;

    /**
     * Computes the generalized Black-Scholes option price.
     *
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double price(final boolean isCall, final double S, final double X, final double T, final double r,
            final double b, final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        final double d2 = d1 - v * Math.sqrt(T);

        if (isCall) {
            return S * Math.exp((b - r) * T) * CND(d1) - X * Math.exp(-r * T) * CND(d2);
        } else {
            return X * Math.exp(-r * T) * CND(-d2) - S * Math.exp((b - r) * T) * CND(-d1);
        }
    }

    /**
     * Computes the generalized Black-Scholes delta (first order partial derivative of option price with respect to
     * stock price).
     *
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option delta
     */
    public static double delta(final boolean isCall, final double S, final double X, final double T, final double r,
            final double b, final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));

        if (isCall) {
            return Math.exp((b - r) * T) * CND(d1);
        } else {
            return Math.exp((b - r) * T) * (CND(d1) - 1);
        }
    }

    /**
     * Computes the generalized Black-Scholes gamma (second order partial derivative of option price with respect to
     * stock price).
     *
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double gamma(final double S, final double X, final double T, final double r, final double b,
            final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        return nd.density(d1) * Math.exp((b - r) * T) / (S * v * Math.sqrt(T));
    }

    /**
     * Computes the generalized Black-Scholes percentage gamma (first order partial derivative of delta with respect to
     * ln(stock price)).
     *
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double gammaP(final double S, final double X, final double T, final double r, final double b,
            final double v) {
        return gamma(S, X, T, r, b, v) * S;
    }

    /**
     * Computes the generalized Black-Scholes vega (first order partial derivative of option price with respect to
     * volatility).
     *
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double vega(final double S, final double X, final double T, final double r, final double b,
            final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        return S * Math.exp((b - r) * T) * nd.density(d1) * Math.sqrt(T);
    }

    /**
     * Computes the generalized Black-Scholes percentage vega (first order partial derivative of vega with respect to
     * ln(volatility)).
     *
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double vegaP(final double S, final double X, final double T, final double r, final double b,
            final double v) {
        return v * vega(S, X, T, r, b, v);
    }

    /**
     * Computes the generalized Black-Scholes vomma (second order partial derivative of option price with respect to
     * volatility).
     *
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double vomma(final double S, final double X, final double T, final double r, final double b,
            final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        final double d2 = d1 - (v * Math.sqrt(T));
        return vega(S, X, T, r, b, v) * d1 * d2 / v;
    }

    /**
     * Computes the generalized Black-Scholes percentage vomma (first order partial derivative of Vega with respect to
     * ln(volatility)).
     *
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double vommaP(final double S, final double X, final double T, final double r, final double b,
            final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        final double d2 = d1 - (v * Math.sqrt(T));
        return vegaP(S, X, T, r, b, v) * d1 * d2 / v;
    }

    /**
     * Computes the generalized Black-Scholes vegaBleed (first order partial derivative of Vega with respect to time to
     * expiry).
     *
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double vegaBleed(final double S, final double X, final double T, final double r, final double b,
            final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        final double d2 = d1 - (v * Math.sqrt(T));
        return vega(S, X, T, r, b, v) * (r - b + (b * d1 / (v * Math.sqrt(T))) - (1 + d1 * d2) / (2 * T));
    }

    /**
     * Computes the generalized Black-Scholes charm (first order partial derivative of Delta with respect to time to
     * expiry).
     *
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double charm(final boolean isCall, final double S, final double X, final double T, final double r,
            final double b, final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        final double d2 = d1 - (v * Math.sqrt(T));
        if (isCall) {
            return -Math.exp((b - r) * T)
                    * ((nd.density(d1) * (b / (v * Math.sqrt(T)) - d2 / (2 * T))) + (b - r) * CND(d1));
        } else {
            return -Math.exp((b - r) * T)
                    * ((nd.density(d1) * (b / (v * Math.sqrt(T)) - d2 / (2 * T))) - (b - r) * CND(-d1));
        }
    }

    /**
     * Computes the generalized Black-Scholes theta (first order partial derivative of option price with respect to time
     * to expiry).
     *
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double theta(final boolean isCall, final double S, final double X, final double T, final double r,
            final double b, final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        final double d2 = d1 - (v * Math.sqrt(T));
        if (isCall) {
            return ((-S * Math.exp((b - r) * T) * nd.density(d1) * v) / (2 * Math.sqrt(T)))
                    - ((b - r) * S * Math.exp((b - r) * T) * CND(d1)) - (r * X * Math.exp(-r * T) * CND(d2));
        } else {
            return ((-S * Math.exp((b - r) * T) * nd.density(d1) * v) / (2 * Math.sqrt(T)))
                    + ((b - r) * S * Math.exp((b - r) * T) * CND(-d1)) + (r * X * Math.exp(-r * T) * CND(-d2));
        }
    }

    /**
     * Computes the generalized Black-Scholes driftlessTheta (theta assuing the risk free rate and the cost of carry are
     * zero).
     *
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double driftlessTheta(final double S, final double X, final double T, final double r, final double b,
            final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        return -S * nd.density(d1) * v / (2 * Math.sqrt(T));
    }

    /**
     * Computes the generalized Black-Scholes rho (first order partial derivative of option price with respect to
     * risk-free rate).
     *
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double rho(final boolean isCall, final double S, final double X, final double T, final double r,
            final double b, final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        final double d2 = d1 - (v * Math.sqrt(T));
        if (isCall) {
            return T * X * Math.exp(-r * T) * CND(d2);
        } else {
            return -T * X * Math.exp(-r * T) * CND(-d2);
        }
    }

    /**
     * Computes the generalized Black-Scholes carryRho (first order partial derivative of option price with respect to
     * cost-of-carry).
     *
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double carryRho(final boolean isCall, final double S, final double X, final double T, final double r,
            final double b, final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        if (isCall) {
            return T * S * Math.exp((b - r) * T) * CND(d1);
        } else {
            return -T * S * Math.exp((b - r) * T) * CND(-d1);
        }
    }

    /**
     * Computes the generalized Black-Scholes strikeDelta (first order partial derivative of option price with respect
     * to strike price).
     *
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return theoretical option price
     */
    public static double strikeDelta(final boolean isCall, final double S, final double X, final double T,
            final double r, final double b, final double v) {
        final double d1 = (Math.log(S / X) + (b + v * v / 2) * T) / (v * Math.sqrt(T));
        final double d2 = d1 - (v * Math.sqrt(T));
        if (isCall) {
            return -Math.exp(-r * T) * CND(d2);
        } else {
            return Math.exp(-r * T) * CND(-d2);
        }
    }

    /**
     * Generalized Black-Scholes implied vol fitter using a bisection algorithm.
     *
     * @param P option price
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @return implied volatility
     */
    public static double impliedVolBisect(final double P, final Boolean isCall, final double S, final double X,
            final double T, final double r, final double b) {
        return impliedVolBisect(P, isCall, S, X, T, r, b, EPS, MAX_ITERS);
    }

    /**
     * Generalized Black-Scholes implied vol fitter using a bisection algorithm.
     *
     * @param P option price
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param eps volatility convergence tolerance
     * @param maxIters maximum number of optimization iterations
     * @return implied volatility
     */
    public static double impliedVolBisect(final double P, final Boolean isCall, final double S, final double X,
            final double T, final double r, final double b, final double eps, final int maxIters) {
        if (P == NULL_DOUBLE || isCall == null || S == NULL_DOUBLE || X == NULL_DOUBLE || T == NULL_DOUBLE
                || r == NULL_DOUBLE || b == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        if (P == 0) {
            return 0.0;
        }

        if (Double.isNaN(P * S * X * T * r * b)) {
            return Double.NaN;
        }

        double vLow = 1e-4;
        double vHigh = 10.0;

        for (int i = 0; i < maxIters; i++) {
            final double vi = vLow + 0.5 * (vHigh - vLow);
            final double pi = price(isCall, S, X, T, r, b, vi);

            if (P < pi) {
                vHigh = vi;
            } else {
                vLow = vi;
            }

            if (Math.abs(vHigh - vLow) < eps) {
                return vi;
            }
        }

        return 0.5 * (vLow + vHigh);
    }


    /**
     * Generalized Black-Scholes implied vol fitter using a newton algorithm.
     *
     * @param P option price
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @return implied volatility
     */
    public static double impliedVolNewton(final double P, final Boolean isCall, final double S, final double X,
            final double T, final double r, final double b) {
        return impliedVolNewton(P, isCall, S, X, T, r, b, EPS, MAX_ITERS);
    }

    /**
     * Generalized Black-Scholes implied vol fitter using a newton algorithm.
     *
     * @param P option price
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param eps volatility convergence tolerance
     * @param maxIters maximum number of optimization iterations
     * @return implied volatility
     */
    public static double impliedVolNewton(final double P, final Boolean isCall, final double S, final double X,
            final double T, final double r, final double b, final double eps, final int maxIters) {
        if (P == NULL_DOUBLE || isCall == null || S == NULL_DOUBLE || X == NULL_DOUBLE || T == NULL_DOUBLE
                || r == NULL_DOUBLE || b == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        if (P == 0) {
            return 0.0;
        }

        if (Double.isNaN(P * S * X * T * r * b)) {
            return Double.NaN;
        }

        double vol = 0.4;

        for (int i = 0; i < maxIters; i++) {
            final double price = price(isCall, S, X, T, r, b, vol);
            final double vega = vega(S, X, T, r, b, vol);
            final double dvol = -(price - P) / vega;

            final double adjDvol = Math.min(dvol, 0.5 * vol);

            vol += adjDvol;

            if (Math.abs(adjDvol) < eps) {
                return vol;
            }
        }

        return vol;
    }

    /**
     * Generalized Black-Scholes implied vol fitter using a newton algorithm based on log(vol).
     *
     * @param P option price
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @return implied volatility
     */
    public static double impliedVolNewtonP(final double P, final Boolean isCall, final double S, final double X,
            final double T, final double r, final double b) {
        return impliedVolNewtonP(P, isCall, S, X, T, r, b, EPS, MAX_ITERS);
    }

    /**
     * Generalized Black-Scholes implied vol fitter using a newton algorithm based on log(vol).
     *
     * @param P option price
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param X strike price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param eps volatility convergence tolerance
     * @param maxIters maximum number of optimization iterations
     * @return implied volatility
     */
    public static double impliedVolNewtonP(final double P, final Boolean isCall, final double S, final double X,
            final double T, final double r, final double b, final double eps, final int maxIters) {
        if (P == NULL_DOUBLE || isCall == null || S == NULL_DOUBLE || X == NULL_DOUBLE || T == NULL_DOUBLE
                || r == NULL_DOUBLE || b == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        if (P == 0) {
            return 0.0;
        }

        if (Double.isNaN(P * S * X * T * r * b)) {
            return Double.NaN;
        }

        double vol = 0.4;
        double lnvol = Math.log(vol);
        double volOld = 1000;

        for (int i = 0; i < maxIters; i++) {
            final double price = price(isCall, S, X, T, r, b, vol);
            final double vegaP = vol * vega(S, X, T, r, b, vol);
            final double dlnvol = -(price - P) / vegaP;

            final double adjDlnvol = Math.signum(dlnvol) * Math.min(Math.abs(dlnvol), 0.5);

            lnvol += adjDlnvol;

            vol = Math.exp(lnvol);

            if (Math.abs(vol - volOld) < eps) {
                return vol;
            }

            volOld = vol;
        }

        return vol;
    }

    /**
     * Finds the Generalized Black-Scholes strike from the delta.
     *
     * @param delta delta
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @return strike associated with the delta
     */
    public static double strikeFromDeltaBisect(final double delta, final Boolean isCall, final double S, final double T,
            final double r, final double b, final double v) {
        return strikeFromDeltaBisect(delta, isCall, S, T, r, b, v, EPS, MAX_ITERS);
    }

    /**
     * Finds the Generalized Black-Scholes strike from the delta.
     *
     * @param delta delta
     * @param isCall true for call; false for put.
     * @param S underlying stock price
     * @param T years to expiry
     * @param r risk-free rate
     * @param b cost-of-carry
     * @param v volatility
     * @param eps volatility convergence tolerance
     * @param maxIters maximum number of optimization iterations
     * @return strike associated with the delta
     */
    public static double strikeFromDeltaBisect(final double delta, final Boolean isCall, final double S, final double T,
            final double r, final double b, final double v, final double eps, final int maxIters) {
        if (delta == NULL_DOUBLE || isCall == null || S == NULL_DOUBLE || v == NULL_DOUBLE || T == NULL_DOUBLE
                || r == NULL_DOUBLE || b == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        if (Double.isNaN(delta * S * v * T * r * b)) {
            return Double.NaN;
        }

        double xLow = 1e-6;
        double xHigh = 1e6; // it is this big to handle BRK-A

        for (int i = 0; i < maxIters; i++) {
            final double xi = xLow + 0.5 * (xHigh - xLow);
            final double di = delta(isCall, S, xi, T, r, b, v);

            if (delta > di) {
                xHigh = xi;
            } else {
                xLow = xi;
            }

            if (Math.abs(xHigh - xLow) < eps) {
                return xi;
            }
        }

        return 0.5 * (xLow + xHigh);
    }


    private static final NormalDistribution nd = new NormalDistribution();

    /**
     * The cumulative normal distribution function.
     */
    private static double CND(final double X) {
        final double a1 = 0.31938153, a2 = -0.356563782, a3 = 1.781477937, a4 = -1.821255978, a5 = 1.330274429;

        final double L = Math.abs(X);
        final double K = 1.0 / (1.0 + 0.2316419 * L);
        final double w = 1.0 - 1.0 / Math.sqrt(2.0 * Math.PI) * Math.exp(-L * L / 2) * (a1 * K + a2 * K * K + a3
                * Math.pow(K, 3) + a4 * Math.pow(K, 4) + a5 * Math.pow(K, 5));

        if (X < 0.0) {
            return 1.0 - w;
        } else {
            return w;
        }
    }

}
