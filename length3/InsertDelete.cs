using System;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Csv;

namespace Experiments
{
    class InsertDelete {
        private static string INPUT_PATH = @"./resources/insert-delete/graph.csv";
        private static string OUTPUT_PATH = @"./resources/insert-delete/output.csv";

        public class InsertDeleteRow {
            public int start;
            public int end;
            public int src;
            public int dst;
        }

        public static InsertDeleteRow stringsToRow(string[] strings) {
            InsertDeleteRow row = new InsertDeleteRow();
            row.start = int.Parse(strings[0]);
            row.end = int.Parse(strings[1]);
            row.src = int.Parse(strings[2]);
            row.dst = int.Parse(strings[3]);
            return row;
        }

        public static long extractStart(InsertDeleteRow row) {
            return row.start;
        }

        public static long extractEnd(InsertDeleteRow row) {
            return row.end;
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
            var stream = CsvFileReader<InsertDeleteRow>.GetIntervalStreamable(INPUT_PATH, stringsToRow, extractStart, extractEnd);

            var length2 = stream.Join(stream, l => l.dst, r => r.src, (l, r) => new {src = l.src, via = l.dst, dst = r.dst});
            var length3 = length2.Join(stream, l => l.dst, r => r.src, (l, r) => new Length3Path{src = l.src, via1 = l.via, via2 = l.dst, dst = r.dst});
            
            CsvFileWriter<Length3Path>.WriteToFile(OUTPUT_PATH, length3.ToStreamEventObservable(), eventToStrings);
        }
    }
}