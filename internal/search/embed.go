package search

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
)

// embedBatch is how many chunks one call to the embedding service
// carries, within what the inference client accepts.
const embedBatch = 32

// embedChunks prefixes each chunk with the model's task and embeds them
// in batches; wait is whether the call may wait its turn at the gate.
func (s *Service) embedChunks(ctx context.Context, owner, prefix string, chunks []string, wait bool) ([][]float32, error) {
	out := make([][]float32, 0, len(chunks))
	for start := 0; start < len(chunks); start += embedBatch {
		end := min(start+embedBatch, len(chunks))
		batch := make([]string, 0, end-start)
		for _, chunk := range chunks[start:end] {
			batch = append(batch, prefix+chunk)
		}
		vectors, err := s.embed(ctx, owner, batch, wait)
		if err != nil {
			return nil, err
		}
		out = append(out, vectors...)
	}
	return out, nil
}

// embed is one call to the embedding service, admitted by the gate and
// bounded by the foreground or the background timeout.
func (s *Service) embed(ctx context.Context, owner string, inputs []string, wait bool) ([][]float32, error) {
	timeout := s.timeouts.foreground
	if wait {
		timeout = s.timeouts.background
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	release, err := s.gate.acquire(ctx, owner, len(inputs), wait)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.Embedder.Embed(ctx, inputs)
}

func quantizeAll(vectors [][]float32) []searchindex.Vector {
	if len(vectors) == 0 {
		return nil
	}
	out := make([]searchindex.Vector, len(vectors))
	for i, v := range vectors {
		out[i] = searchindex.Quantize(v)
	}
	return out
}
