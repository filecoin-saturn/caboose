package state

type State struct {
	ActiveNodes any
	AllNodes    any
	PoolController
}

type PoolController interface {
	DoRefresh()
}

type NodeInfo struct {
	ID            string  `json:"id"`
	IP            string  `json:"ip"`
	Distance      float32 `json:"distance"`
	Weight        int     `json:"weight"`
	ComplianceCid string  `json:"complianceCid"`
	Core          bool    `json:"core"`
}
