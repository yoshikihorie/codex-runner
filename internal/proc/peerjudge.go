package proc

// JudgePeer applies the server-side resume conditions to trusted process data.
// If a process probe failed, the controlling-terminal condition fails closed.
func JudgePeer(hasControllingTerminal bool, ancestors []int, owned map[int]bool, supervisedMarker bool, valid bool) (isatty, supervised bool) {
	for _, pid := range ancestors {
		if owned[pid] {
			supervised = true
			break
		}
	}
	supervised = supervised || supervisedMarker
	return valid && hasControllingTerminal, supervised
}
