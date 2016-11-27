package index

type Doc struct {
	ID    uint32   `json:"id"`
	Terms []uint32 `json:"terms"`
}
