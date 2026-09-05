namespace Weasel.Core;

/// <summary>
///     Precomputed positional parameter names — "p0", "p1", … — for the builders that name
///     parameters by their position in the command. Naming by position is a per-parameter
///     string allocation on every append path; the overwhelming majority of commands bind
///     far fewer parameters than the table holds, so the common case allocates nothing
///     (weasel#556).
/// </summary>
/// <remarks>
///     The names are byte-for-byte what <c>"p" + position</c> produced before, and past the
///     end of the table that concatenation is exactly what happens, so no SQL text changes
///     at any position.
/// </remarks>
public static class ParameterNames
{
    private const int TableSize = 512;

    private static readonly string[] _names = precompute();

    private static string[] precompute()
    {
        var names = new string[TableSize];
        for (var i = 0; i < names.Length; i++)
        {
            names[i] = "p" + i;
        }

        return names;
    }

    /// <summary>
    ///     The name of the parameter at the given zero-based position: "p0", "p1", … Allocation-free
    ///     for the positions any sane command reaches; identical to <c>"p" + position</c> beyond them.
    /// </summary>
    /// <param name="position">
    ///     Zero-based position, typically the parameter collection's count at the moment of the add.
    /// </param>
    public static string ForPosition(int position)
    {
        return (uint)position < TableSize ? _names[position] : "p" + position;
    }
}
