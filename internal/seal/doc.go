// Package seal is the cryptography of what the harness stores for a caller
// under the caller's content key, which the harness holds only for the
// request that carries it: the envelope a row is sealed in, and the formats
// older clients sealed rows in, which are still read; the id a key is
// registered under; the proof of possession a change to a row carries to the
// control plane; the key a caller's search index is sealed under; and the
// sealing of a shared chat.
package seal
