package types

// LogTermStarts includes every term transition in a replication batch. The
// first entry may continue a term that began in an earlier persisted batch.
func LogTermStarts(logs []Log) []TermStartIndexInfo {
	var starts []TermStartIndexInfo
	for i, log := range logs {
		if i == 0 || log.Term != logs[i-1].Term {
			starts = append(starts, TermStartIndexInfo{Term: log.Term, Index: log.Index})
		}
	}
	return starts
}
