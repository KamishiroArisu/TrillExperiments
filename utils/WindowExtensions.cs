using System;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

public static class WindowExtensions {
    public static IStreamable<Empty, T> TumblingWindowWithSize<T>(this IStreamable<Empty, T> stream, int size) {
        return stream.AlterEventLifetime(st => computeWindowStart(st, size), size);
    }

    private static long computeWindowStart(long st, long period) {
        return period <= 1 ? st : st - (st % period);
    }
}