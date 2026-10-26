package attach

import (
	"encoding/base64"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// ModelCopies gives each image of an upgraded chat the copy and thumbnail an
// upload makes in place of its original, which its client keeps from the old
// chat.
func ModelCopies(t *domain.Thread) {
	t.EachAttachment(func(a *domain.Attachment) error {
		if a.IsImage() && a.Base64 != "" {
			ModelImageCopy(a)
		}
		return nil
	})
}

// ModelImageCopy gives an image the copy and thumbnail an upload makes in
// place of its original. An image that does not decode keeps no bytes, since
// no model reads it.
func ModelImageCopy(a *domain.Attachment) {
	original, err := base64.StdEncoding.DecodeString(a.Base64)
	a.Base64 = ""
	if err != nil {
		return
	}
	resized, thumb, err := ResizeImage(original)
	clear(original)
	if err == nil {
		a.Base64, a.Thumbnail = base64.StdEncoding.EncodeToString(resized), base64.StdEncoding.EncodeToString(thumb)
	}
}
