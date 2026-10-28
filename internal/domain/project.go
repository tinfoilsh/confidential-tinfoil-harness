package domain

// Project groups chats under shared instructions and documents.
type Project struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	Description        string  `json:"description"`
	SystemInstructions string  `json:"systemInstructions"`
	Color              *string `json:"color"`
	CreatedAt          string  `json:"createdAt,omitempty"`
	UpdatedAt          string  `json:"updatedAt,omitempty"`
}

// ProjectDocument is a document's extracted text, which every chat of its
// project reads.
type ProjectDocument struct {
	ID              string `json:"id"`
	ProjectID       string `json:"projectId"`
	Filename        string `json:"filename"`
	ContentType     string `json:"contentType"`
	SizeBytes       int    `json:"sizeBytes"`
	Content         string `json:"content,omitempty"`
	ThumbnailBase64 string `json:"thumbnailBase64,omitempty"`
	CreatedAt       string `json:"createdAt,omitempty"`
	UpdatedAt       string `json:"updatedAt,omitempty"`
}

// StoredProject and StoredDocument are the rows this harness writes. A row
// an import made says where it came from, which no client is sent.
type StoredProject struct {
	Project
	RecordVersion int           `json:"recordVersion"`
	ImportedFrom  *ImportOrigin `json:"_restore,omitempty"`
}

type StoredDocument struct {
	ProjectDocument
	RecordVersion int           `json:"recordVersion"`
	ImportedFrom  *ImportOrigin `json:"_restore,omitempty"`
}
