using System;
using System.CommandLine;

namespace Experiments
{
    class Program
    {
        static int Main(string[] args) {
            var rootCommand = new RootCommand();

            var length3Command = new Command("length3", "get paths with length = 3 in a graph");

            var typeOpt = new Option<string>(new string[] 
                {"--type", "-t" }, 
                getDefaultValue: () => "insert-only", 
                "input type, should be insert-only, insert-delete, or window");

            length3Command.AddOption(typeOpt);
            length3Command.SetHandler((string t) => runLength3(t), typeOpt);
            rootCommand.AddCommand(length3Command);

            rootCommand.Description = "Trill Experiments.";
            rootCommand.TreatUnmatchedTokensAsErrors = true;

            return rootCommand.InvokeAsync(args).Result;
        }

        private static void runLength3(string t) {
            switch (t.ToLower()) {
                case "insert-only": {
                    Console.WriteLine("Run length3 with " + t);
                    InsertOnly.Execute();
                    break;
                }
                case "insert-delete": {
                    Console.WriteLine("Run length3 with " + t);
                    InsertDelete.Execute();
                    break;
                }
                case "window": {
                    Console.WriteLine("Run length3 with " + t);
                    Window.Execute();
                    break;
                }
                default: {
                    Console.WriteLine("Unrecognizable type " + t);
                    Console.WriteLine("should be insert-only, insert-delete, or window");
                    break;
                }
            }
        }
    }
}
