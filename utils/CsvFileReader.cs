using System;
using System.Threading;
using System.IO;
using Microsoft.StreamProcessing;


namespace Csv {
    class CsvFileReader<T> {
        public static IStreamable<Empty, T> GetStartStreamable(string path, Func<string[], T> convert, 
                Func<T, long> extractStart) {
            return new CsvObservable(path, convert, InputEventType.Start, extractStart, null).ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1));
        }

        public static IStreamable<Empty, T> GetIntervalStreamable(string path, Func<string[], T> convert, 
                Func<T, long> extractStart, Func<T, long> extractEnd) {
            return new CsvObservable(path, convert, InputEventType.Interval, extractStart, extractEnd).ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1));
        }

        public static IStreamable<Empty, T> GetPointStreamable(string path, Func<string[], T> convert, 
                Func<T, long> extractPoint) {
            return new CsvObservable(path, convert, InputEventType.Point, extractPoint, null).ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1));
        }

        private sealed class CsvObservable : IObservable<StreamEvent<T>> {
            private string path;
            private Func<string[], T> convert;
            private InputEventType eventType;
            private Func<T, long> extract1;
            private Func<T, long> extract2;

            public CsvObservable(string path, Func<string[], T> convert, InputEventType eventType, Func<T, long> extract1, Func<T, long> extract2) {
                this.path = path;
                this.convert = convert;
                this.eventType = eventType;
                this.extract1 = extract1;
                this.extract2 = extract2;
            }

            public IDisposable Subscribe(IObserver<StreamEvent<T>> observer) {
                return new Subscription(path, convert, eventType, extract1, extract2, observer);
            }

            private sealed class Subscription : IDisposable {
                private string path;
                private Func<string[], T> convert;
                private InputEventType eventType;
                private Func<T, long> extract1;
                private Func<T, long> extract2;
                private IObserver<StreamEvent<T>> observer;
                private Thread readerThread;

                public Subscription(string path, Func<string[], T> convert, InputEventType eventType, Func<T, long> extract1, Func<T, long> extract2, 
                        IObserver<StreamEvent<T>> observer) {
                    this.path = path;
                    this.convert = convert;
                    this.eventType = eventType;
                    this.extract1 = extract1;
                    this.extract2 = extract2;
                    this.observer = observer;
                    
                    ThreadStart readerStart = new ThreadStart(ReaderThreadFun);
                    readerThread = new Thread(readerStart);
                    readerThread.Start();
                }

                public void Dispose() {
                    
                }

                private void ReaderThreadFun() {
                    foreach (string line in File.ReadLines(path)) { 
                        T t = convert(line.Split(","));

                        switch (eventType) {
                            case InputEventType.Start: {
                                this.observer.OnNext(StreamEvent.CreateStart(extract1(t), t));
                                break;
                            }
                            case InputEventType.Interval: {
                                this.observer.OnNext(StreamEvent.CreateInterval(extract1(t), extract2(t), t));
                                break;
                            }
                            case InputEventType.Point: {
                                this.observer.OnNext(StreamEvent.CreatePoint(extract1(t), t));
                                break;
                            }
                        }
                            
                    } 

                    this.observer.OnNext(StreamEvent.CreatePunctuation<T>(long.MaxValue));
                    this.observer.OnCompleted();
                }
            }
        }
    }
}