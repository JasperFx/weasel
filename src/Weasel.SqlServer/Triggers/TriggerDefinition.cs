using Weasel.Core;

namespace Weasel.SqlServer.Triggers;

internal sealed record TriggerDefinition(
    IReadOnlyList<string> Name,
    IReadOnlyList<string> Target,
    TriggerTiming Timing,
    TriggerEvents Events,
    string Body)
{
    public static TriggerDefinition? Parse(string definition)
    {
        var reader = new Reader(definition);

        if (!reader.Keyword("CREATE")) return null;
        if (reader.Keyword("OR") && !reader.Keyword("ALTER")) return null;
        if (!reader.Keyword("TRIGGER")) return null;

        var name = reader.MultipartName();
        if (name == null || !reader.Keyword("ON")) return null;

        var target = reader.MultipartName();
        if (target == null || reader.Peek("WITH")) return null;

        TriggerTiming timing;
        if (reader.Keyword("FOR") || reader.Keyword("AFTER"))
        {
            timing = TriggerTiming.After;
        }
        else if (reader.Keyword("INSTEAD") && reader.Keyword("OF"))
        {
            timing = TriggerTiming.InsteadOf;
        }
        else
        {
            return null;
        }

        var events = TriggerEvents.None;
        do
        {
            if (reader.Keyword("INSERT")) events |= TriggerEvents.Insert;
            else if (reader.Keyword("UPDATE")) events |= TriggerEvents.Update;
            else if (reader.Keyword("DELETE")) events |= TriggerEvents.Delete;
            else return null;
        } while (reader.Symbol(','));

        if (reader.Peek("WITH") || reader.Peek("NOT") || !reader.Keyword("AS")) return null;

        return new TriggerDefinition(name, target, timing, events, definition[reader.Position..]);
    }

    public bool Describes(Trigger trigger)
        => namesMatch(Name, trigger.Identifier)
           && namesMatch(Target, trigger.Target)
           && Timing == trigger.Timing
           && Events == trigger.Events
           && TriggerBase.NormalizeBody(Body) == TriggerBase.NormalizeBody(trigger.Body);

    private static bool namesMatch(IReadOnlyList<string> parts, DbObjectName name)
    {
        string[] expected = [name.Schema, name.Name];
        if (parts.Count > expected.Length) return false;

        return parts
            .Zip(expected.Skip(expected.Length - parts.Count))
            .All(x => string.Equals(x.First, x.Second, StringComparison.OrdinalIgnoreCase));
    }

    private sealed class Reader(string text)
    {
        public int Position { get; private set; }

        public bool Peek(string keyword)
        {
            var start = Position;
            var found = Keyword(keyword);
            Position = start;
            return found;
        }

        public bool Keyword(string keyword)
        {
            skipTrivia();

            var end = Position + keyword.Length;
            if (end > text.Length ||
                string.Compare(text, Position, keyword, 0, keyword.Length, StringComparison.OrdinalIgnoreCase) != 0 ||
                (end < text.Length && isWordCharacter(text[end])))
            {
                return false;
            }

            Position = end;
            return true;
        }

        public bool Symbol(char symbol)
        {
            skipTrivia();
            if (Position < text.Length && text[Position] == symbol)
            {
                Position++;
                return true;
            }

            return false;
        }

        public List<string>? MultipartName()
        {
            var parts = new List<string>();
            do
            {
                var part = identifier();
                if (part == null) return null;
                parts.Add(part);
            } while (Symbol('.'));

            return parts;
        }

        private string? identifier()
        {
            skipTrivia();
            if (Position >= text.Length) return null;

            var open = text[Position];
            if (open is '[' or '"')
            {
                var close = open == '[' ? ']' : '"';
                for (var i = Position + 1; i < text.Length; i++)
                {
                    if (text[i] != close) continue;

                    if (i + 1 < text.Length && text[i + 1] == close)
                    {
                        i++;
                        continue;
                    }

                    var quoted = text[(Position + 1)..i].Replace($"{close}{close}", close.ToString());
                    Position = i + 1;
                    return quoted;
                }

                return null;
            }

            var start = Position;
            while (Position < text.Length && isWordCharacter(text[Position])) Position++;

            return Position > start ? text[start..Position] : null;
        }

        private static bool isWordCharacter(char c) => char.IsLetterOrDigit(c) || c is '_' or '@' or '#' or '$';

        private void skipTrivia()
        {
            while (Position < text.Length)
            {
                if (char.IsWhiteSpace(text[Position]))
                {
                    Position++;
                }
                else if (startsWith("--"))
                {
                    var newline = text.IndexOf('\n', Position);
                    Position = newline < 0 ? text.Length : newline + 1;
                }
                else if (startsWith("/*"))
                {
                    skipBlockComment();
                }
                else
                {
                    return;
                }
            }
        }

        private void skipBlockComment()
        {
            var depth = 0;
            while (Position < text.Length)
            {
                if (startsWith("/*"))
                {
                    depth++;
                    Position += 2;
                }
                else if (startsWith("*/"))
                {
                    depth--;
                    Position += 2;
                    if (depth == 0) return;
                }
                else
                {
                    Position++;
                }
            }
        }

        private bool startsWith(string value) => string.CompareOrdinal(text, Position, value, 0, value.Length) == 0;
    }
}
