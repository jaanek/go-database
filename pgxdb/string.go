package pgxdb

// Contains tells whether a contains x.
func SliceContains(a []string, x string) bool {
	if a == nil {
		return false
	}
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}
