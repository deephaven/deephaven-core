using System;

public class Program
{
  public static void Main()
  {
    // string:
    Console.WriteLine("I said \"hello\". This is a backslash: \\");

    // Verbatim string. Quotes still need to be escaped (differently)
    Console.WriteLine(@"I said ""hello"". This is a backslash: \");

    // interpolation (but not verbatim)
    var text = "hello";
    Console.WriteLine($"I said \"{text}\". This is a backslash: \\");

    // Verbatim with interpolation
    Console.WriteLine($@"I said ""{text}"". This is a backslash: \");

    // Raw. Nothing needs to be escaped (but see below)
    Console.WriteLine("""I said "hello". This is a backslash: \""");

    // Raw with interpolation. Nothing needs to be escaped (but see below)
    Console.WriteLine($"""I said "{text}". This is a backslash: \""");

    // Raw with interpolation. But I want """ to be literal
    Console.WriteLine($""""I said "{text}". This is a triple quote: """ This is a backslash: \"""");

    // Raw with interpolation. But I want """ and brace to be literal 
    Console.WriteLine($$""""I said "{{text}}". This is a triple quote: """ This is a backslash: \. This is a brace {"""");

    // I want my raw string to end with a triple quote
    Console.WriteLine(""""
                      There are no newlines or leading spaces ih this output. Also this is a triple quote: """
                      """");
  }
}
