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
	// File is where the uploaded file the text was read from is kept. A
	// document stored before files were kept, or by an import, has none.
	// Its key never leaves the enclave.
	File *DocumentFile `json:"-"`
}

// DocumentFile is a document's uploaded file as buckets holds it: the
// object's id, and the key it is sealed under, base64.
type DocumentFile struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

// DocumentView is a document as its client reads it, which says whether its
// file can be downloaded.
type DocumentView struct {
	ProjectDocument
	HasFile bool `json:"hasFile"`
}

func (d ProjectDocument) View() DocumentView {
	return DocumentView{ProjectDocument: d, HasFile: d.File != nil}
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
	File          *DocumentFile `json:"file,omitempty"`
	RecordVersion int           `json:"recordVersion"`
	ImportedFrom  *ImportOrigin `json:"_restore,omitempty"`
}
