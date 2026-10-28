// Package store reads and writes what sync keeps for a caller: the rows of
// chats, projects, documents and profiles under the caller's content key,
// the blobs of attachments, the key registry, and the grants that let a
// run or an archive write. It holds nothing between requests.
package store
