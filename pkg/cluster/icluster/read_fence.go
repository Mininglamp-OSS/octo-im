package icluster

// SlotReadFence reports whether rows owned by a slot may be served from the
// local business database. CheckSlotReadable returns an error wrapping
// types.ErrSlotDiverged while the local replica of slotId is quarantined or
// fail-stopped.
type SlotReadFence interface {
	CheckSlotReadable(slotId uint32) error
}

// FencedRead reads rows owned by slotId: check, read, re-check. If the local
// replica is fenced at either check the rows are discarded and the fence
// error is returned, even when the read itself failed or found nothing, so a
// fenced read can never look like absence. A nil fence reads unfenced.
func FencedRead[T any](fence SlotReadFence, slotId uint32, read func() (T, error)) (T, error) {
	var zero T
	if fence == nil {
		return read()
	}
	if err := fence.CheckSlotReadable(slotId); err != nil {
		return zero, err
	}
	v, err := read()
	if ferr := fence.CheckSlotReadable(slotId); ferr != nil {
		return zero, ferr
	}
	return v, err
}

// FencedReadSlots is FencedRead for rows that are owned jointly by several
// slots: every slot must be readable at both checks.
func FencedReadSlots[T any](fence SlotReadFence, slotIds []uint32, read func() (T, error)) (T, error) {
	var zero T
	if fence == nil {
		return read()
	}
	check := func() error {
		for _, id := range slotIds {
			if err := fence.CheckSlotReadable(id); err != nil {
				return err
			}
		}
		return nil
	}
	if err := check(); err != nil {
		return zero, err
	}
	v, err := read()
	if ferr := check(); ferr != nil {
		return zero, ferr
	}
	return v, err
}
