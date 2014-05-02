package sysbench

import (
	"bytes"
	"math/rand"
)

type Doc struct {
	Id  uint64 "_id"
	K   int    "k"
	C   string "c"
	Pad string "pad"
}

var ctemplate string = "###########-###########-###########-###########-###########-###########-###########-###########-###########-###########"
var padtemplate string = "###########-###########-###########-###########-###########"

func GenString(template string, randSource *rand.Rand) string {
	var buf bytes.Buffer
	alpha := "abcdefghijklmnopqrstuvwxyz"
	nums := "0123456789"
	for i := 0; i < len(template); i++ {
		if template[i] == '#' {
			buf.WriteByte(nums[randSource.Int31n(int32(len(nums)))])
		} else if template[i] == '@' {
			buf.WriteByte(alpha[randSource.Int31n(int32(len(alpha)))])
		} else {
			buf.WriteByte(template[i])
		}
	}
	return buf.String()
}

func CString(randSource *rand.Rand) string {
	return GenString(ctemplate, randSource)
}

func PadString(randSource *rand.Rand) string {
	return GenString(padtemplate, randSource)
}
