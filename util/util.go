package util

import (
	"math/rand"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
)

var (
	TIME_MIN   = time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	TIME_MAX   = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).Unix()
	TIME_DELTA = TIME_MAX - TIME_MIN
	TS_MIN     = time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC).Unix()
	TS_MAX     = time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC).Unix()
	TS_DELTA   = TS_MAX - TS_MIN
	HASH_LEN   = 10
	START_TIME = time.Now().Format("2006-01-02_15:04:05")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func RdRange(min, max int) int {
	if min == max {
		return min
	} else if min > max {
		min, max = max, min
	}
	if (max-min >= 0x7fffffffffffffff || max-min < 0) && min < 0 && max > 0 {
		return min + rand.Intn(2) + rand.Intn((-min)-1) + rand.Intn(max)
	}
	return min + rand.Intn(max-min)
}

func RdMoment() time.Time {
	sec := rand.Int63n(TIME_DELTA) + TIME_MIN
	return time.Unix(sec, 0)
}

func RdDate() time.Time {
	return RdMoment()
}

func RdDateTime() time.Time {
	return RdMoment()
}

func RdTimestamp() time.Time {
	sec := rand.Int63n(TS_DELTA) + TS_MIN
	return time.Unix(sec, 0)
}

func RdName() string {
	return namesgenerator.GetRandomName(RdRange(0, 1000))
}

func RdBool() bool {
	return rand.Intn(2) == 0
}

func RdBoolRatio(ratio float64) bool {
	return rand.Float64() < ratio
}

func RdHash() string {
	hash := make([]rune, HASH_LEN)
	for i := 0; i < HASH_LEN; i++ {
		// TODO: add number into random string
		hash[i] = rune(RdRange(0x61, 0x7a))
	}
	return string(hash)
}

func NowStr() string {
	return time.Now().Format("2006-01-02_15:04:05")
}
