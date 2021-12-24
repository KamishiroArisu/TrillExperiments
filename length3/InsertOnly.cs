using System;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Csv;

namespace Experiments
{
    class InsertOnly {
        private static string INPUT_PATH = @"./resources/insert-only/graph.csv";
        private static string OUTPUT_PATH = @"./resources/insert-only/output.csv";

        public class InsertOnlyRow {
            public int start;
            public int src;
            public int dst;
        }

        public static InsertOnlyRow stringsToRow(string[] strings) {
            InsertOnlyRow row = new InsertOnlyRow();
            row.start = int.Parse(strings[0]);
            row.src = int.Parse(strings[1]);
            row.dst = int.Parse(strings[2]);
            return row;
        }

        public static long extractStart(InsertOnlyRow row) {
            return row.start;
        }

        public static string[] eventToStrings(StreamEvent<Length3Path> ev) {
            if (ev.IsInterval) {
                throw new Exception("Invalid event type: Interval");
            } else if (ev.IsStart) {
                return new string[] { 
                    ev.StartTime.ToString(), 
                    ev.Payload.src.ToString(), 
                    ev.Payload.via1.ToString(), 
                    ev.Payload.via2.ToString(), 
                    ev.Payload.dst.ToString(), 
                };
            } else if (ev.IsPunctuation) {
                return null;
            } else {
                throw new Exception("Invalid event: " + ev.ToString());
            }
        }

        public static void Execute() {
            var stream = CsvFileReader<InsertOnlyRow>.GetStartStreamable(INPUT_PATH, stringsToRow, extractStart);
            
            var length2 = stream.Join(stream, l => l.dst, r => r.src, (l, r) => new {src = l.src, via = l.dst, dst = r.dst});
            var length3 = length2.Join(stream, l => l.dst, r => r.src, (l, r) => new Length3Path{src = l.src, via1 = l.via, via2 = l.dst, dst = r.dst});
            
            CsvFileWriter<Length3Path>.WriteToFile(OUTPUT_PATH, length3.ToStreamEventObservable(), eventToStrings);
        }
    }
}