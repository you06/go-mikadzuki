package util

import (
	"math/rand"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
)

var (
	TIME_MIN   = time.Date(1970, 1, 0, 0, 0, 1, 0, time.UTC).Unix()
	TIME_MAX   = time.Date(2100, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	TIME_DELTA = TIME_MAX - TIME_MIN
	HASH_LEN   = 10
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

func RdDate() string {
	return RdMoment().Format("2006-01-02")
}

func RdDateTime() string {
	return RdMoment().Format("2006-01-02 15:04:05")
}

func RdName() string {
	return namesgenerator.GetRandomName(0)
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
