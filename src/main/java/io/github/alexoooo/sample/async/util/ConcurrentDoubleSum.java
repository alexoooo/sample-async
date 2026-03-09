package io.github.alexoooo.sample.async.util;


import java.util.concurrent.locks.ReentrantLock;


public class ConcurrentDoubleSum
{
    //-----------------------------------------------------------------------------------------------------------------
    private static final int defaultStripeCount = Runtime.getRuntime().availableProcessors();


    //-----------------------------------------------------------------------------------------------------------------
    private final double[] sums;
    private final double[] comps;
    private final ReentrantLock[] locks;


    //-----------------------------------------------------------------------------------------------------------------
    public ConcurrentDoubleSum() {
        this(defaultStripeCount);
    }

    public ConcurrentDoubleSum(int stripeCount) {
        sums = new double[stripeCount];
        comps = new double[stripeCount];
        locks = new ReentrantLock[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            locks[i] = new ReentrantLock();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    public void add(double value) {
        int slot = (int)(Thread.currentThread().threadId() % locks.length);
        ReentrantLock lock = locks[slot];
        lock.lock();
        try {
            double s = sums[slot];
            double t = s + value;
            if (Double.isFinite(t)) {
                comps[slot] += Math.abs(s) >= Math.abs(value)
                        ? (s - t) + value
                        : (value - t) + s;
            }
            sums[slot] = t;
        } finally {
            lock.unlock();
        }
    }

    public double sum() {
        for (ReentrantLock l : locks) {
            l.lock();
        }
        try {
            double[] merged = new double[2];
            for (int i = 0; i < locks.length; i++) {
                neumaierAdd(merged, sums[i]);
                neumaierAdd(merged, comps[i]);
            }
            return merged[0] + merged[1];
        }
        finally {
            for (ReentrantLock l : locks) {
                l.unlock();
            }
        }
    }

    private void neumaierAdd(double[] c, double value) {
        double t = c[0] + value;
        if (Double.isFinite(t)) {
            c[1] += Math.abs(c[0]) >= Math.abs(value)
                    ? (c[0] - t) + value
                    : (value - t) + c[0];
        }
        c[0] = t;
//        double t = c[0] + value;
//        if (Double.isInfinite(t)) {
//            // No meaningful compensation possible; just accumulate
//            c[0] = t;
//        } else {
//            c[1] += Math.abs(c[0]) >= Math.abs(value)
//                    ? (c[0] - t) + value
//                    : (value - t) + c[0];
//            c[0] = t;
//        }
    }


    public void reset() {
        for (ReentrantLock l : locks) {
            l.lock();
        }
        try {
            for (int i = 0; i < locks.length; i++) {
                sums[i]  = 0.0;
                comps[i] = 0.0;
            }
        }
        finally {
            for (ReentrantLock l : locks) {
                l.unlock();
            }
        }
    }
}
