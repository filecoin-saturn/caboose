package state

type State struct {
	ActiveNodes any
	AllNodes    any
	PoolController
}

type PoolController interface {
	DoRefresh()
}
