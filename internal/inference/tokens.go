package inference

import (
	"sync"

	"github.com/tiktoken-go/tokenizer"
)

var encoder = sync.OnceValue(func() tokenizer.Codec {
	enc, err := tokenizer.Get(tokenizer.Cl100kBase)
	if err != nil {
		panic(err)
	}
	return enc
})

// Tokens estimates how many tokens a model reads of s. Models tokenize
// differently, so a budget built on it keeps a margin.
func Tokens(s string) int {
	count, err := encoder().Count(s)
	if err != nil {
		return len([]byte(s))
	}
	return count
}
