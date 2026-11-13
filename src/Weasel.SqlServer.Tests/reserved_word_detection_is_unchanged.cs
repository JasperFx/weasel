using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#825 replaced the linear, culture-aware scan behind
///     <see cref="SqlServerIdentifierRules.IsReservedWord" /> with a hash lookup for ASCII names. That is
///     only a performance change if every answer stays the same, and an ordinal ignore-case comparison is
///     not the invariant culture's for every input — the invariant culture treats some characters as
///     ignorable, folds compatibility forms, and has its own idea of the Turkish i. So these tests pin
///     today's answers: an explicit table of the cases worth naming, and a sweep that compares the
///     implementation against the pre-#825 code, kept here verbatim as <see cref="today" />.
/// </summary>
/// <remarks>
///     The sweep compares against <see cref="today" /> rather than against hard-coded answers because
///     some of the culture-aware answers come from the ICU collation data on the machine (whether a
///     control character is ignorable, whether a full-width letter matches its ASCII form). Whatever the
///     platform says, the fast path has to say the same.
/// </remarks>
public class reserved_word_detection_is_unchanged
{
    /// <summary>The implementation before weasel#825, verbatim.</summary>
    private static bool today(string name)
        => SqlServerIdentifierRules.ReservedKeywords.Contains(name, StringComparer.InvariantCultureIgnoreCase);

    private static bool isReserved(string name) => SqlServerIdentifierRules.Instance.IsReservedWord(name);

    /// <summary>
    ///     Characters outside printable ASCII that a culture-aware comparison is known to treat
    ///     specially, plus a spread of ordinary non-ASCII letters.
    /// </summary>
    private static readonly char[] SpecialCharacters =
    [
        'İ', // İ  Latin capital I with dot above (Turkish)
        'ı', // ı  Latin small dotless i (Turkish)
        'ß', // ß
        'ẞ', // ẞ  capital sharp s
        'ſ', // ſ  long s
        'K', // K  Kelvin sign
        'Å', // Å
        'Å', // Å  Angstrom sign
        'é', // é
        '́', // combining acute accent
        '̇', // combining dot above
        '­', // soft hyphen
        '​', // zero-width space
        '‌', // zero-width non-joiner
        '‍', // zero-width joiner
        '﻿', // zero-width no-break space / BOM
        ' ', // no-break space
        '　', // ideographic space
        'Ａ', // Ａ full-width A
        'Ｓ', // Ｓ full-width S
        'ｅ', // ｅ full-width e
        'ﬀ', // ﬀ ligature
        'ﬁ', // ﬁ ligature
        'Ǆ', // Ǆ
        'Α', // Α Greek capital alpha
        'С', // С Cyrillic capital es
        'е', // е Cyrillic small ie
        '表', // 表
        '选', // 选
    ];

    [Fact]
    public void the_keyword_list_is_the_one_being_pinned()
    {
        SqlServerIdentifierRules.ReservedKeywords.Length.ShouldBe(186);
        SqlServerIdentifierRules.ReservedKeywords.ShouldAllBe(k => k.All(c => c >= ' ' && c <= '~'),
            "the fast path relies on every keyword being printable ASCII");
        SqlServerIdentifierRules.ReservedKeywords.ShouldContain("WITHIN GROUP");
        SqlServerIdentifierRules.ReservedKeywords.ShouldContain("IDENTITY_INSERT");
    }

    [Fact]
    public void every_keyword_in_every_casing_is_reserved()
    {
        foreach (var keyword in SqlServerIdentifierRules.ReservedKeywords)
        {
            var lower = keyword.ToLowerInvariant();
            string[] casings =
            [
                keyword,
                lower,
                char.ToUpperInvariant(lower[0]) + lower[1..],
                new string(keyword.Select((c, i) => i % 2 == 0 ? char.ToLowerInvariant(c) : c).ToArray()),
                new string(keyword.Select((c, i) => i % 2 == 1 ? char.ToLowerInvariant(c) : c).ToArray())
            ];

            foreach (var casing in casings)
            {
                isReserved(casing).ShouldBeTrue(casing);
                today(casing).ShouldBeTrue(casing);
            }
        }
    }

    [Theory]
    [InlineData("select", true)]
    [InlineData("SELECT", true)]
    [InlineData("Select", true)]
    [InlineData("sElEcT", true)]
    [InlineData("identity_insert", true)]
    [InlineData("within group", true)]
    [InlineData("WITHIN GROUP", true)]
    [InlineData("Rank", true)]
    [InlineData("table", true)]
    [InlineData("mt_doc_user", false)]
    [InlineData("dbo", false)]
    [InlineData("orders", false)]
    [InlineData("selects", false)]
    [InlineData("selec", false)]
    [InlineData("within  group", false)]
    [InlineData("withingroup", false)]
    [InlineData("within_group", false)]
    [InlineData("", false)]
    [InlineData(" ", false)]
    [InlineData(" select", false)]
    [InlineData("select ", false)]
    [InlineData("[select]", false)]
    [InlineData("İndex", false)]   // Turkish dotted capital I
    [InlineData("İNDEX", false)]
    [InlineData("ındex", false)]   // Turkish dotless small i
    [InlineData("INDEẊ", false)] // I + combining dot above at the end
    [InlineData("Straße", false)]
    [InlineData("Grüße", false)]
    [InlineData("séléct", false)]
    [InlineData("sélect", false)]
    [InlineData("Ünion", false)]
    [InlineData("表", false)]
    [InlineData("选择", false)]
    [InlineData("Αdd", false)]     // Greek capital alpha, not A
    [InlineData("Сase", false)]    // Cyrillic capital es, not C
    public void pinned_answers(string name, bool expected)
    {
        today(name).ShouldBe(expected, "the pre-#825 answer");
        isReserved(name).ShouldBe(expected);
    }

    [Fact]
    public void a_null_name_is_not_reserved()
    {
        today(null!).ShouldBeFalse();
        isReserved(null!).ShouldBeFalse();
    }

    [Fact]
    public void every_keyword_altered_by_one_character_answers_as_it_did()
    {
        // Latin-1 (which covers every ASCII control character) and the special characters,
        // inserted before, inside and after every keyword.
        var inserted = Enumerable.Range(0, 0x100).Select(i => (char)i).Concat(SpecialCharacters).ToArray();
        // Every ASCII character and the special characters, replacing each character of every keyword.
        var replacing = Enumerable.Range(0, 0x80).Select(i => (char)i).Concat(SpecialCharacters).ToArray();

        var mismatches = new List<string>();
        var cultureOnlyMatches = 0;

        void check(string candidate)
        {
            var expected = today(candidate);
            if (isReserved(candidate) != expected)
            {
                mismatches.Add(describe(candidate));
            }

            if (expected && !SqlServerIdentifierRules.ReservedKeywords.Contains(candidate, StringComparer.OrdinalIgnoreCase))
            {
                cultureOnlyMatches++;
            }
        }

        foreach (var keyword in SqlServerIdentifierRules.ReservedKeywords)
        {
            foreach (var form in new[] { keyword, keyword.ToLowerInvariant() })
            {
                foreach (var c in inserted)
                {
                    check(c + form);
                    check(form[..(form.Length / 2)] + c + form[(form.Length / 2)..]);
                    check(form + c);
                }

                for (var i = 0; i < form.Length; i++)
                {
                    foreach (var c in replacing)
                    {
                        check(form[..i] + c + form[(i + 1)..]);
                    }
                }
            }
        }

        mismatches.ShouldBeEmpty();

        // The sweep is only worth something if it reaches answers an ordinal comparison would get
        // wrong. Under ICU it does (the ignorable ASCII control characters, U+00AD, U+200B and more);
        // with invariant globalization every culture comparison is ordinal and there are none.
        if (!invariantGlobalization())
        {
            cultureOnlyMatches.ShouldBeGreaterThan(0);
        }
    }

    private static bool invariantGlobalization()
        => AppContext.TryGetSwitch("System.Globalization.Invariant", out var invariant) && invariant;

    [Fact]
    public void every_basic_multilingual_plane_character_answers_as_it_did()
    {
        // Short keywords give the widest room for one character to fold, expand or vanish into a
        // match: IF, IN, IS, OF, ON, OR, TO, BY, AS, OFF.
        var mismatches = new List<string>();

        for (var i = 0; i <= 0xFFFF; i++)
        {
            if (i is >= 0xD800 and <= 0xDFFF)
            {
                continue;
            }

            var c = (char)i;
            foreach (var candidate in new[] { c.ToString(), "O" + c, c + "F", "O" + c + "F", "OF" + c, c + "N" })
            {
                if (isReserved(candidate) != today(candidate))
                {
                    mismatches.Add(describe(candidate));
                }
            }
        }

        mismatches.ShouldBeEmpty();
    }

    private static string describe(string candidate)
        => $"\"{candidate}\" ({string.Join(" ", candidate.Select(c => $"U+{(int)c:X4}"))}): today={today(candidate)}";
}
