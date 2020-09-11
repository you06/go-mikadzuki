package util

import (
	"fmt"
)

func AssertEQ(left, right interface{}) {
	if left != right {
		panic(fmt.Sprintf("%s(%T) != %s(%T)", left, left, right, right))
	}
}
func AssertNE(left, right interface{}) {
	if left == right {
		panic(fmt.Sprintf("%s(%T) == %s(%T)", left, left, right, right))
	}
}
