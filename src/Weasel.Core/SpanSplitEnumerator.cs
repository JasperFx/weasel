using System.Buffers;
using System.Runtime.CompilerServices;

namespace Weasel.Core;

// Vendored from .NET 9 Preview. Requires APIs available in .NET 8+.

#if NET8_0

internal static class SpanSplitExtensions
{
    /// <summary>
    /// Returns a type that allows for enumeration of each element within a split span
    /// using the provided separator character.
    /// </summary>
    /// <typeparam name="T">The type of the elements.</typeparam>
    /// <param name="source">The source span to be enumerated.</param>
    /// <param name="separator">The separator character to be used to split the provided span.</param>
    /// <returns>Returns a <see cref="SpanSplitEnumerator{T}"/>.</returns>
    public static SpanSplitEnumerator<T> Split<T>(this ReadOnlySpan<T> source, T separator) where T : IEquatable<T> =>
        new SpanSplitEnumerator<T>(source, separator);

    /// <summary>
    /// Returns a type that allows for enumeration of each element within a split span
    /// using the provided separator span.
    /// </summary>
    /// <typeparam name="T">The type of the elements.</typeparam>
    /// <param name="source">The source span to be enumerated.</param>
    /// <param name="separator">The separator span to be used to split the provided span.</param>
    /// <returns>Returns a <see cref="SpanSplitEnumerator{T}"/>.</returns>
    public static SpanSplitEnumerator<T> Split<T>(this ReadOnlySpan<T> source, ReadOnlySpan<T> separator)
        where T : IEquatable<T> =>
        new SpanSplitEnumerator<T>(source, separator, treatAsSingleSeparator: true);

    /// <summary>
    /// Returns a type that allows for enumeration of each element within a split span
    /// using the provided <see cref="SpanSplitEnumerator{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the elements.</typeparam>
    /// <param name="source">The source span to be enumerated.</param>
    /// <param name="separators">The <see cref="SpanSplitEnumerator{T}"/> to be used to split the provided span.</param>
    /// <returns>Returns a <see cref="SpanSplitEnumerator{T}"/>.</returns>
    /// <remarks>
    /// Unlike <see cref="SplitAny{T}(ReadOnlySpan{T}, ReadOnlySpan{T})"/>, the <paramref name="separators"/> is not checked for being empty.
    /// An empty <paramref name="separators"/> will result in no separators being found, regardless of the type of <typeparamref name="T"/>,
    /// whereas <see cref="SplitAny{T}(ReadOnlySpan{T}, ReadOnlySpan{T})"/> will use all Unicode whitespace characters as separators if <paramref name="separators"/> is
    /// empty and <typeparamref name="T"/> is <see cref="char"/>.
    /// </remarks>
    public static SpanSplitEnumerator<T> SplitAny<T>(this ReadOnlySpan<T> source, SearchValues<T> separators)
        where T : IEquatable<T> =>
        new SpanSplitEnumerator<T>(source, separators);
}

internal static class SpanSplitConstants
{
    /// <summary>A <see cref="SearchValues{Char}"/> for all of the Unicode whitespace characters</summary>
    public static readonly SearchValues<char> WhiteSpaceChars =
        SearchValues.Create(
            "\t\n\v\f\r\u0020\u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000");
}

internal ref struct SpanSplitEnumerator<T> where T : IEquatable<T>
{
    /// <summary>The input span being split.</summary>
    private readonly ReadOnlySpan<T> _span;

    /// <summary>A single separator to use when <see cref="_splitMode"/> is <see cref="SpanSplitEnumeratorMode.SingleElement"/>.</summary>
    private readonly T _separator = default!;

    /// <summary>
    /// A separator span to use when <see cref="_splitMode"/> is <see cref="SpanSplitEnumeratorMode.Sequence"/> (in which case
    /// it's treated as a single separator) or <see cref="SpanSplitEnumeratorMode.Any"/> (in which case it's treated as a set of separators).
    /// </summary>
    private readonly ReadOnlySpan<T> _separatorBuffer;

    /// <summary>A set of separators to use when <see cref="_splitMode"/> is <see cref="SpanSplitEnumeratorMode.SearchValues"/>.</summary>
    private readonly SearchValues<T> _searchValues = default!;

    /// <summary>Mode that dictates how the instance was configured and how its fields should be used in <see cref="MoveNext"/>.</summary>
    private SpanSplitEnumeratorMode _splitMode;

    /// <summary>The inclusive starting index in <see cref="_span"/> of the current range.</summary>
    private int _startCurrent = 0;

    /// <summary>The exclusive ending index in <see cref="_span"/> of the current range.</summary>
    private int _endCurrent = 0;

    /// <summary>The index in <see cref="_span"/> from which the next separator search should start.</summary>
    private int _startNext = 0;

    /// <summary>Gets an enumerator that allows for iteration over the split span.</summary>
    /// <returns>Returns a <see cref="SpanSplitEnumerator{T}"/> that can be used to iterate over the split span.</returns>
    public SpanSplitEnumerator<T> GetEnumerator() => this;

    /// <summary>Gets the current element of the enumeration.</summary>
    /// <returns>Returns a <see cref="Range"/> instance that indicates the bounds of the current element withing the source span.</returns>
    public Range Current => new Range(_startCurrent, _endCurrent);

    /// <summary>Initializes the enumerator for <see cref="SpanSplitEnumeratorMode.SearchValues"/>.</summary>
    internal SpanSplitEnumerator(ReadOnlySpan<T> span, SearchValues<T> searchValues)
    {
        _span = span;
        _splitMode = SpanSplitEnumeratorMode.SearchValues;
        _searchValues = searchValues;
    }

    /// <summary>Initializes the enumerator for <see cref="SpanSplitEnumeratorMode.Any"/>.</summary>
    /// <remarks>
    /// If <paramref name="separators"/> is empty and <typeparamref name="T"/> is <see cref="char"/>, as an optimization
    /// it will instead use <see cref="SpanSplitEnumeratorMode.SearchValues"/> with a cached <see cref="SearchValues{Char}"/>
    /// for all whitespace characters.
    /// </remarks>
    internal SpanSplitEnumerator(ReadOnlySpan<T> span, ReadOnlySpan<T> separators)
    {
        _span = span;
        if (typeof(T) == typeof(char) && separators.Length == 0)
        {
            _searchValues = Unsafe.As<SearchValues<T>>(SpanSplitConstants.WhiteSpaceChars);
            _splitMode = SpanSplitEnumeratorMode.SearchValues;
        }
        else
        {
            _separatorBuffer = separators;
            _splitMode = SpanSplitEnumeratorMode.Any;
        }
    }

    /// <summary>Initializes the enumerator for <see cref="SpanSplitEnumeratorMode.Sequence"/> (or <see cref="SpanSplitEnumeratorMode.EmptySequence"/> if the separator is empty).</summary>
    /// <remarks><paramref name="treatAsSingleSeparator"/> must be true.</remarks>
    internal SpanSplitEnumerator(ReadOnlySpan<T> span, ReadOnlySpan<T> separator, bool treatAsSingleSeparator)
    {
        _span = span;
        _separatorBuffer = separator;
        _splitMode = separator.Length == 0 ? SpanSplitEnumeratorMode.EmptySequence : SpanSplitEnumeratorMode.Sequence;
    }

    /// <summary>Initializes the enumerator for <see cref="SpanSplitEnumeratorMode.SingleElement"/>.</summary>
    internal SpanSplitEnumerator(ReadOnlySpan<T> span, T separator)
    {
        _span = span;
        _separator = separator;
        _splitMode = SpanSplitEnumeratorMode.SingleElement;
    }

    /// <summary>
    /// Advances the enumerator to the next element of the enumeration.
    /// </summary>
    /// <returns><see langword="true"/> if the enumerator was successfully advanced to the next element; <see langword="false"/> if the enumerator has passed the end of the enumeration.</returns>
    public bool MoveNext()
    {
        // Search for the next separator index.
        int separatorIndex, separatorLength;
        switch (_splitMode)
        {
            case SpanSplitEnumeratorMode.None:
                return false;

            case SpanSplitEnumeratorMode.SingleElement:
                separatorIndex = _span.Slice(_startNext).IndexOf(_separator);
                separatorLength = 1;
                break;

            case SpanSplitEnumeratorMode.Any:
                separatorIndex = _span.Slice(_startNext).IndexOfAny(_separatorBuffer);
                separatorLength = 1;
                break;

            case SpanSplitEnumeratorMode.Sequence:
                separatorIndex = _span.Slice(_startNext).IndexOf(_separatorBuffer);
                separatorLength = _separatorBuffer.Length;
                break;

            case SpanSplitEnumeratorMode.EmptySequence:
                separatorIndex = -1;
                separatorLength = 1;
                break;

            default:
                separatorIndex = _span.Slice(_startNext).IndexOfAny(_searchValues);
                separatorLength = 1;
                break;
        }

        _startCurrent = _startNext;
        if (separatorIndex >= 0)
        {
            _endCurrent = _startCurrent + separatorIndex;
            _startNext = _endCurrent + separatorLength;
        }
        else
        {
            _startNext = _endCurrent = _span.Length;

            // Set _splitMode to None so that subsequent MoveNext calls will return false.
            _splitMode = SpanSplitEnumeratorMode.None;
        }

        return true;
    }
}

/// <summary>Indicates in which mode <see cref="SpanSplitEnumerator{T}"/> is operating, with regards to how it should interpret its state.</summary>
internal enum SpanSplitEnumeratorMode
{
    /// <summary>Either a default <see cref="SpanSplitEnumerator{T}"/> was used, or the enumerator has finished enumerating and there's no more work to do.</summary>
    None = 0,

    /// <summary>A single T separator was provided.</summary>
    SingleElement,

    /// <summary>A span of separators was provided, each of which should be treated independently.</summary>
    Any,

    /// <summary>The separator is a span of elements to be treated as a single sequence.</summary>
    Sequence,

    /// <summary>The separator is an empty sequence, such that no splits should be performed.</summary>
    EmptySequence,

    /// <summary>
    /// A <see cref="SearchValues{Char}"/> was provided and should behave the same as with <see cref="Any"/> but with the separators in the <see cref="SearchValues"/>
    /// instance instead of in a <see cref="ReadOnlySpan{Char}"/>.
    /// </summary>
    SearchValues
}

#endif
