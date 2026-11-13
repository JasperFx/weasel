using System.Reflection;
using System.Runtime.CompilerServices;
using Shouldly;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#816, Critter Stack plan decision D4: runtime-async is on by default for the net11.0 build of every
///     shipped Weasel assembly (<c>WeaselRuntimeAsync</c> in Directory.Build.props), and off on net10.0, which cannot
///     load runtime-async code. Modelled on Wolverine's <c>runtime_async_defaults_5150</c>.
/// </summary>
/// <remarks>
///     The switch does not trigger a recompile, so a stale build from before it changed can fail this test: clean
///     bin/ and obj/ and build again.
/// </remarks>
public class runtime_async_defaults
{
    // MethodImplAttributes.Async, the flag the compiler sets on a runtime-async method
    private const MethodImplAttributes AsyncFlag = (MethodImplAttributes)0x2000;

    [Theory]
    [InlineData("Weasel.Core")]
    [InlineData("Weasel.Storage")]
    [InlineData("Weasel.Postgresql")]
    [InlineData("Weasel.SqlServer")]
    [InlineData("Weasel.MySql")]
    [InlineData("Weasel.Oracle")]
    [InlineData("Weasel.Sqlite")]
    [InlineData("Weasel.Firebird")]
    public void the_assembly_is_compiled_with_runtime_async_only_on_net11(string assemblyName)
    {
        var types = Assembly.Load(assemblyName).GetTypes();

        var runtimeAsyncMethods = types
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                                          BindingFlags.Static | BindingFlags.DeclaredOnly))
            .Count(m => (m.MethodImplementationFlags & AsyncFlag) != 0);

        var stateMachines = types.Count(t =>
            typeof(IAsyncStateMachine).IsAssignableFrom(t) && t.IsDefined(typeof(CompilerGeneratedAttribute)));

        TestContext.Current.SendDiagnosticMessage(
            $"{assemblyName}: {runtimeAsyncMethods} runtime-async methods, {stateMachines} async state machines");

#if NET11_0_OR_GREATER
        runtimeAsyncMethods.ShouldBeGreaterThan(0);

        // Async iterators are not converted by runtime-async, so they keep their state machines
        stateMachines.ShouldBeLessThanOrEqualTo(asyncIteratorCount(types));
#else
        runtimeAsyncMethods.ShouldBe(0);
        stateMachines.ShouldBeGreaterThan(0);
#endif
    }

    private static int asyncIteratorCount(Type[] types)
    {
        return types
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                                          BindingFlags.Static | BindingFlags.DeclaredOnly))
            .Count(m => m.IsDefined(typeof(AsyncIteratorStateMachineAttribute)));
    }
}
