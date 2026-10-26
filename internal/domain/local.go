package domain

// A local transcript carries its attachments inline, so a turn may be larger
// than other requests. Uploads already accept files of up to 50 MiB.
const MaxTurnBody = 64 << 20

// DeviceThread is a local thread as its client stores it after a run. Image
// bytes and document page images are left out, so a scanned document cannot
// fill the run's log: the client holds them and puts them back by id.
func DeviceThread(t *Thread) Thread {
	thread := t.Clone()
	thread.RecordVersion = RecordVersion
	thread.CheckWidgets()
	thread.EachAttachment(func(a *Attachment) error {
		a.Base64, a.InferenceMimeType = "", ""
		for i := range a.Pages {
			a.Pages[i].Image = ""
		}
		return nil
	})
	return thread
}
