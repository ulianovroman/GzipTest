using System;
using System.Diagnostics;
using System.IO;

namespace GZipTest
{
    public static class Program
    {
        static void Main(string[] args)
        {
            var message = ValidateArgs(args);

            if(message != null)
            {
                Console.WriteLine(message);
                return;
            }

            if (File.Exists(args[2]))
            {
                string answer = string.Empty;
                while (answer.ToLower() != "y")
                {
                    Console.WriteLine("Warning! File {0} is already exists. Do you want to overwrite it? [y\\n]", args[2]);
                    answer = Console.ReadLine();

                    if (answer.ToLower() == "n")
                    {
                        return;
                    }
                }
            }

            var maxBlocksInRam = args.Length > 3 ? int.Parse(args[3]) : 1000;
 
            var processor = GetFileProcessor(args[0], args[1], args[2], maxBlocksInRam);

            Stopwatch sw = Stopwatch.StartNew();
            Console.WriteLine("Start processing");
            try
            {
                processor.Execute();
            }
            catch (InvalidDataException e)
            {
                Console.WriteLine("Failed!");

                if (args[0] == "decompress")
                    Console.WriteLine("Perhaps the file is not a gzip archive or was compressed not by this program");
                else
                    Console.WriteLine(e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed!");
                Console.WriteLine(e.Message);
                return;
            }

            sw.Stop();
            var time = sw.Elapsed.TotalSeconds;

            Console.WriteLine("Finished processing in " + time.ToString("#.####") + " seconds.");
        }

        private static string ValidateArgs(string[] args)
        {
            if(args == null || args.Length < 3)
            {
                return "Please, specify the arguments \"compress\\decompress input_file_name output_file_name blocks_in_RAM_count\"";
            }

            if(args.Length > 3 && (!int.TryParse(args[3], out int count) || count <= 0))
            {
                return "blocks_in_RAM_count is an optional argument, but it must be an integer greater than zero";
            }

            if(args[0] != "compress" && args[0] != "decompress")
            {
                return "Wrong operation! Please, specify the argument \"compress\\decompress\"";
            }

            if (!File.Exists(args[1]))
            {
                return "Inpute file is not found!";
            }

            return null;
        }

        private static FileProcessor GetFileProcessor(string mode, string inputFileName, string outputFileName, int maxBlocksCount)
        {
            if(mode == "compress")
            {
                return new FileCompressor(inputFileName, outputFileName, maxBlocksCount);
            }

            return new FileDecompressor(inputFileName, outputFileName, maxBlocksCount);
        }           
        
    }
}
