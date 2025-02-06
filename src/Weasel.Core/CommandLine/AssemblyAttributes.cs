using System.Runtime.CompilerServices;
using JasperFx;
using JasperFx.CommandLine;
using Weasel.CommandLine;

[assembly:JasperFxAssembly(typeof(WeaselCommandLineExtension))]
[assembly: InternalsVisibleTo("Weasel.CommandLine.Tests")]
