package attach

import (
	"encoding/binary"
	"math"
)

// Read-aloud audio plays faster or slower than the voice speaks it by WSOLA
// (waveform similarity overlap-add): it is cut into overlapping frames that
// are laid back down stretchHop apart, each taken from where the speed puts
// it in the voice's audio, moved by up to stretchSearch samples to where it
// best continues the frame before it. A frame keeps its samples, so the
// voice keeps its pitch.
const (
	stretchHop    = 480 // 20 ms at 24 kHz, half a frame
	stretchFrame  = 2 * stretchHop
	stretchSearch = 240 // 10 ms at 24 kHz
)

// stretchWindow fades each frame in and out; two frames half a frame apart
// sum to one.
var stretchWindow = func() []float64 {
	w := make([]float64, stretchFrame)
	for i := range w {
		w[i] = 0.5 - 0.5*math.Cos(2*math.Pi*float64(i)/stretchFrame)
	}
	return w
}()

// stretcher plays one piece's audio at a speed as the audio arrives.
type stretcher struct {
	speed  float64
	input  []float64 // the piece's samples so far
	odd    []byte    // a sample whose second byte has yet to arrive
	frames int       // frames laid down so far
	from   int       // where in input the last frame was taken
	sum    []float64 // the output from the last frame's start on
}

func newStretcher(speed float64) *stretcher {
	return &stretcher{speed: speed, sum: make([]float64, stretchFrame)}
}

// push adds audio the voice spoke, and returns the audio that now plays.
func (s *stretcher) push(pcm []byte) []byte {
	data := append(s.odd, pcm...)
	whole := len(data) / 2
	for i := range whole {
		s.input = append(s.input, float64(int16(binary.LittleEndian.Uint16(data[2*i:]))))
	}
	s.odd = append([]byte(nil), data[2*whole:]...)
	return s.lay(false)
}

// flush returns the audio that plays once the voice has finished.
func (s *stretcher) flush() []byte { return s.lay(true) }

// lay lays down every frame the audio so far makes, or every frame left
// once it ended, and returns the output those frames finish.
func (s *stretcher) lay(ended bool) []byte {
	var out []byte
	for {
		at := int(math.Round(float64(s.frames) * stretchHop * s.speed))
		if ended && at >= len(s.input) {
			break
		}
		if !ended && max(at+stretchSearch, s.from+stretchHop)+stretchFrame > len(s.input) {
			break
		}
		start := at
		if s.frames > 0 {
			start = s.match(at)
		}
		for i := range stretchFrame {
			s.sum[i] += stretchWindow[i] * s.sample(start+i)
		}
		s.from = start
		s.frames++
		out = appendSamples(out, s.sum[:stretchHop])
		copy(s.sum, s.sum[stretchHop:])
		clear(s.sum[stretchHop:])
	}
	if ended && s.frames > 0 {
		out = appendSamples(out, s.sum[:stretchHop])
		clear(s.sum)
	}
	return out
}

// stretchCoarseStep is how far apart a frame's first search looks, over
// every stretchCoarseStep-th sample, before it looks closer around the best.
const stretchCoarseStep = 4

// match is where near at the next frame best continues the last one: the
// start whose first half is most like the audio that followed the last
// frame's first half in the voice's audio.
func (s *stretcher) match(at int) int {
	first, last := max(0, at-stretchSearch), at+stretchSearch
	coarse := s.search(first, last, stretchCoarseStep)
	return s.search(max(first, coarse-stretchCoarseStep+1), min(last, coarse+stretchCoarseStep-1), 1)
}

// search is the start from first to last, every step samples, whose first
// half, read every step samples, best continues the last frame.
func (s *stretcher) search(first, last, step int) int {
	follows := s.from + stretchHop
	best, bestScore := first, math.Inf(-1)
	for start := first; start <= last; start += step {
		var product, energy float64
		for i := 0; i < stretchHop; i += step {
			candidate := s.sample(start + i)
			product += candidate * s.sample(follows+i)
			energy += candidate * candidate
		}
		if score := product / math.Sqrt(energy+1); score > bestScore {
			best, bestScore = start, score
		}
	}
	return best
}

// sample is the voice's sample at i, or silence past either end.
func (s *stretcher) sample(i int) float64 {
	if i < 0 || i >= len(s.input) {
		return 0
	}
	return s.input[i]
}

// appendSamples appends samples to pcm as 16-bit little-endian audio.
func appendSamples(pcm []byte, samples []float64) []byte {
	for _, v := range samples {
		pcm = binary.LittleEndian.AppendUint16(pcm, uint16(int16(math.Round(min(max(v, math.MinInt16), math.MaxInt16)))))
	}
	return pcm
}
