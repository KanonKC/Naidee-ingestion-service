package processing

import "time"

// bookkeepingTimeout bounds the status writes that must still happen after the
// run context has been cancelled — a run cut short by shutdown should still
// close out its processing_runs row rather than being left stuck on `polling`.
const bookkeepingTimeout = 10 * time.Second
