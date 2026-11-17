package types

import "fmt"

// CheckLogPrefix compares the actual entry preceding the follower's next
// index. A truncation is only another probe, never proof of a matching prefix.
// The caller supplies its captured durable tail; an in-flight append cannot
// certify a prefix which storage has not written yet.
func CheckLogPrefix(next, stored uint64, followerTerm uint32,
	read func(uint64, uint64, uint64) ([]Log, error),
	termStart func(uint32) (uint64, error),
) (uint64, Reason, error) {
	if next == 0 {
		return 0, ReasonError, fmt.Errorf("invalid zero next log index")
	}
	last := next - 1
	if last == 0 {
		return 0, ReasonOk, nil
	}
	if last > stored {
		return stored, ReasonTruncate, nil
	}
	logs, err := read(last, next, 0)
	if err != nil {
		return 0, ReasonError, err
	}
	if len(logs) == 0 || logs[0].Index != last {
		return 0, ReasonError, fmt.Errorf("missing prefix log at index %d", last)
	}
	if logs[0].Term == followerTerm {
		return 0, ReasonOk, nil
	}
	cut := last - 1
	if logs[0].Term > followerTerm {
		start, err := termStart(logs[0].Term)
		if err != nil {
			return 0, ReasonError, err
		}
		if start > 0 && start <= last {
			cut = start - 1
		}
	}
	return cut, ReasonTruncate, nil
}

// RetainedTermStart reads the tail which remains after truncation. The old
// last term may have disappeared entirely and must not survive in memory or
// in the term-start index used by later recovery.
func RetainedTermStart(index uint64,
	read func(uint64, uint64, uint64) ([]Log, error),
	termStart func(uint32) (uint64, error),
) (*TermStartIndexInfo, error) {
	info := &TermStartIndexInfo{}
	if index == 0 {
		return info, nil
	}
	logs, err := read(index, index+1, 0)
	if err != nil {
		return nil, err
	}
	if len(logs) == 0 || logs[0].Index != index {
		return nil, fmt.Errorf("missing retained log at index %d", index)
	}
	info.Term = logs[0].Term
	info.Index, err = termStart(info.Term)
	if err != nil {
		return nil, err
	}
	return info, nil
}
