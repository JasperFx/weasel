using System.Runtime.CompilerServices;
using Oakton;
using Weasel.CommandLine;

[assembly:OaktonCommandAssembly(typeof(WeaselCommandLineExtension))]
[assembly: InternalsVisibleTo("Weasel.CommandLine.Tests")]
