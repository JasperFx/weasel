using JasperFx.Core;

namespace Weasel.SqlServer;

internal static class StringTokenizer
{
    public static IEnumerable<string> Tokenize(string content)
    {
        var searchString = content.Trim();
        if (searchString.Length == 0)
        {
            return Array.Empty<string>();
        }

        var parser = new TokenParser();
        content.ToCharArray().Each(parser.Read);

        // Gotta force the parser to know it's done
        parser.Read('\n');

        return parser.Tokens;
    }
}

public class TokenParser
{
    private readonly List<string> _tokens = new();
    private List<char> _characters = null!;
    private IMode _mode;

    public TokenParser()
    {
        _mode = new Searching(this);
    }

    public IEnumerable<string> Tokens => _tokens;

    public void Read(char c)
    {
        _mode.Read(c);
    }

    private void addChar(char c)
    {
        _characters.Add(c);
    }

    private void startToken(IMode mode)
    {
        _mode = mode;
        _characters = new List<char>();
    }

    private void endToken()
    {
        var @string = new string(_characters.ToArray());
        _tokens.Add(@string);

        _mode = new Searching(this);
    }


    public interface IMode
    {
        void Read(char c);
    }

    public class Searching: IMode
    {
        private readonly TokenParser _parent;

        public Searching(TokenParser parent)
        {
            _parent = parent;
        }

        public void Read(char c)
        {
            if (char.IsWhiteSpace(c))
            {
                return;
            }

            if (c == '(')
            {
                _parent.startToken(new InsideParanthesesToken(_parent));
                _parent.addChar('(');
            }
            else
            {
                var normalToken = new InsideNormalToken(_parent);
                _parent.startToken(normalToken);
                normalToken.Read(c);
            }
        }
    }

    internal class InsideParanthesesToken: IMode
    {
        private readonly TokenParser _parent;
        private int _level;

        public InsideParanthesesToken(TokenParser parent)
        {
            _parent = parent;
            _level = 1;
        }


        public void Read(char c)
        {
            _parent.addChar(c);

            if (c == '(')
            {
                _level++;
            }
            else if (c == ')')
            {
                _level--;
                if (_level == 0)
                {
                    _parent.endToken();
                }
            }
        }
    }

    internal class InsideNormalToken: IMode
    {
        private readonly TokenParser _parent;

        public InsideNormalToken(TokenParser parent)
        {
            _parent = parent;
        }

        public void Read(char c)
        {
            if (char.IsWhiteSpace(c))
            {
                _parent.endToken();
            }
            else
            {
                _parent.addChar(c);
            }
        }
    }
}
