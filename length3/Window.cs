using System;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Csv;

namespace Experiments
{
    class Window {
        private static string INPUT_PATH = @"./resources/window/graph.csv";
        private static string OUTPUT_PATH = @"./resources/window/output.csv";

        public class WindowRow {
            public long point;
            public int src;
            public int dst;

            public override string ToString()
            {
                return new {point, src, dst}.ToString();
            }
        }

        public static WindowRow stringsToRow(string[] strings) {
            WindowRow row = new WindowRow();
            int i = int.Parse(strings[0]);
            row.point = i;
            row.src = int.Parse(strings[1]);
            row.dst = int.Parse(strings[2]);
            return row;
        }

        public static long extractPoint(WindowRow row) {
            return row.point;
        }

        public static string[] eventToStrings(StreamEvent<Length3Path> ev) {
            if (ev.IsInterval) {
                return new string[] { 
                    ev.StartTime.ToString(), 
                    ev.EndTime.ToString(), 
                    ev.Payload.src.ToString(), 
                    ev.Payload.via1.ToString(), 
                    ev.Payload.via2.ToString(), 
                    ev.Payload.dst.ToString(), 
                };
            } else if (ev.IsStart) {
                throw new Exception("Invalid event type: Start");
            } else if (ev.IsPunctuation) {
                return null;
            } else {
                throw new Exception("Invalid event: " + ev.ToString());
            }
        }

        public static void Execute() {
            var stream = CsvFileReader<WindowRow>.GetPointStreamable(INPUT_PATH, stringsToRow, extractPoint)
                    .TumblingWindowWithSize(200);
            
            var length2 = stream.Join(stream, l => l.dst, r => r.src, (l, r) => new {src = l.src, via = l.dst, dst = r.dst});
            var length3 = length2.Join(stream, l => l.dst, r => r.src, (l, r) => new Length3Path{src = l.src, via1 = l.via, via2 = l.dst, dst = r.dst});
            
            CsvFileWriter<Length3Path>.WriteToFile(OUTPUT_PATH, length3.ToStreamEventObservable(), eventToStrings);
        }
    }
}