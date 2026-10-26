package attach

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	east "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// A reply is read aloud as the web client read it: its markdown without
// code blocks, images, footnotes, citation badges and bare links, keeping
// link labels, inline code and math, in pieces of about speechTarget
// characters that end on a sentence where they can.
const (
	speechTargetCharacters = 300
	maxSpeechCharacters    = 600
	MaxSpeechText          = 100000
)

var markdown = goldmark.New(goldmark.WithExtensions(extension.GFM, extension.Footnote),
	goldmark.WithParserOptions(parser.WithInlineParsers(util.Prioritized(mathParser{}, 500)))).Parser()

var (
	citationBadge = regexp.MustCompile(`^\[?\d+\]?$`)
	lineBreakTag  = regexp.MustCompile(`(?i)^<br\s*/?>$`)
	blankRun      = regexp.MustCompile(`[^\S\n]+`)
	newlineRun    = regexp.MustCompile(` *\n+ *`)
)

// speakableText is a reply's markdown as it is read aloud.
func speakableText(source string) string {
	data := []byte(source)
	spoken := readable(markdown.Parse(text.NewReader(data)), data)
	return strings.TrimSpace(newlineRun.ReplaceAllString(blankRun.ReplaceAllString(spoken, " "), "\n"))
}

func readable(n ast.Node, source []byte) string {
	switch node := n.(type) {
	case *ast.FencedCodeBlock, *ast.CodeBlock, *ast.Image, *ast.ThematicBreak, *east.FootnoteLink, *east.FootnoteBacklink, *east.Footnote, *east.FootnoteList, *east.TaskCheckBox:
		return ""
	case *ast.Text:
		value := plainText(node.Value(source))
		if node.IsRaw() {
			value = string(node.Value(source))
		}
		if node.SoftLineBreak() || node.HardLineBreak() {
			value += "\n"
		}
		return value
	case *ast.String:
		return plainText(node.Value)
	case *ast.HTMLBlock:
		return htmlBreak(strings.TrimSpace(string(node.Lines().Value(source))))
	case *ast.RawHTML:
		return htmlBreak(strings.TrimSpace(string(node.Segments.Value(source))))
	case *ast.AutoLink:
		if label := string(node.Label(source)); label != string(node.URL(source)) {
			return label
		}
		return ""
	case *ast.Link:
		label := children(node, source, "")
		if label == string(node.Destination) || citationBadge.MatchString(label) {
			return ""
		}
		return label
	case *ast.Document, *ast.Blockquote, *ast.List, *ast.ListItem, *east.Table, *east.TableHeader, *east.TableRow:
		return children(node, source, "\n")
	}
	return children(n, source, "")
}

func children(n ast.Node, source []byte, separator string) string {
	parts := []string{}
	for child := n.FirstChild(); child != nil; child = child.NextSibling() {
		parts = append(parts, readable(child, source))
	}
	return strings.Join(parts, separator)
}

func plainText(value []byte) string {
	return string(util.ResolveEntityNames(util.ResolveNumericReferences(util.UnescapePunctuations(value))))
}

// htmlBreak reads a line break tag as a new line and other markup as nothing.
func htmlBreak(tag string) string {
	if lineBreakTag.MatchString(tag) {
		return "\n"
	}
	return ""
}

// splitSpeech cuts text into pieces of at most maxSpeechCharacters, joining
// sentences until a piece reaches speechTargetCharacters and cutting a
// longer sentence at a space where it can.
func splitSpeech(text string) []string {
	pieces := []string{}
	current := ""
	flush := func() {
		if piece := strings.TrimSpace(current); piece != "" {
			pieces = append(pieces, piece)
		}
		current = ""
	}
	for _, sentence := range sentences(text) {
		points := []rune(strings.TrimSpace(sentence))
		for len(points) > maxSpeechCharacters {
			flush()
			end := maxSpeechCharacters
			for i := end; i > speechTargetCharacters; i-- {
				if unicode.IsSpace(points[i]) {
					end = i
					break
				}
			}
			pieces = append(pieces, strings.TrimSpace(string(points[:end])))
			points = points[end:]
		}
		remainder := strings.TrimSpace(string(points))
		if remainder == "" {
			continue
		}
		if utf8.RuneCountInString(current)+len(points)+1 > maxSpeechCharacters {
			flush()
		}
		if current != "" {
			current += " "
		}
		current += remainder
		if utf8.RuneCountInString(current) >= speechTargetCharacters {
			flush()
		}
	}
	flush()
	return pieces
}

// sentences cuts text after each sentence's closing punctuation that a space
// or the end follows.
func sentences(text string) []string {
	var cut []string
	runes := []rune(text)
	start := 0
	for i, r := range runes {
		if strings.ContainsRune(".!?。！？", r) && (i+1 == len(runes) || unicode.IsSpace(runes[i+1])) {
			cut = append(cut, string(runes[start:i+1]))
			start = i + 1
		}
	}
	if start < len(runes) {
		cut = append(cut, string(runes[start:]))
	}
	return cut
}

// mathNode is math between dollar signs, which is read as written.
type mathNode struct {
	ast.BaseInline
}

var kindMath = ast.NewNodeKind("Math")

func (n *mathNode) Kind() ast.NodeKind { return kindMath }
func (n *mathNode) Dump(source []byte, level int) {
	ast.DumpHelper(n, source, level, nil, nil)
}

// mathParser reads math as the web client's markdown reader does: a run
// of dollar signs opens it and a run of as many closes it, across lines.
type mathParser struct{}

func (mathParser) Trigger() []byte { return []byte{'$'} }

func (mathParser) Parse(_ ast.Node, block text.Reader, _ parser.Context) ast.Node {
	line, start := block.PeekLine()
	opener := 0
	for opener < len(line) && line[opener] == '$' {
		opener++
	}
	block.Advance(opener)
	l, pos := block.Position()
	node := &mathNode{}
	for {
		line, segment := block.PeekLine()
		if line == nil {
			block.SetPosition(l, pos)
			return ast.NewTextSegment(start.WithStop(start.Start + opener))
		}
		for i := 0; i < len(line); i++ {
			if line[i] != '$' {
				continue
			}
			run := i
			for i < len(line) && line[i] == '$' {
				i++
			}
			if i-run == opener {
				if inner := segment.WithStop(segment.Start + run); !inner.IsEmpty() {
					node.AppendChild(node, ast.NewRawTextSegment(inner))
				}
				block.Advance(i)
				if node.ChildCount() == 0 {
					block.SetPosition(l, pos)
					return ast.NewTextSegment(start.WithStop(start.Start + opener))
				}
				return node
			}
		}
		node.AppendChild(node, ast.NewRawTextSegment(segment))
		block.AdvanceLine()
	}
}
