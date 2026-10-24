package main

// A local transcript carries its attachments inline, so a turn may be larger
// than other requests. Uploads already accept files of up to 50 MiB.
const maxTurnBody = 64 << 20

// deviceThread is a local thread as its client stores it after a run. Image
// bytes and document page images are left out, so a scanned document cannot
// fill the run's log: the client holds them and puts them back by id.
func deviceThread(t *Thread) Thread {
	thread := t.clone()
	thread.RecordVersion = recordVersion
	thread.checkWidgets()
	thread.eachAttachment(func(a *Attachment) error {
		a.Base64, a.inferenceMimeType = "", ""
		for i := range a.Pages {
			a.Pages[i].Image = ""
		}
		return nil
	})
	return thread
}
