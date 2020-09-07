package util

import "fmt"

// Version information.
var (
	BuildTS = "None"
	BuildHash = "None"
)

func PrintVersion() {
	fmt.Println("MIKADZUKI 🌙")
	fmt.Printf("BuiltTS: %s\nBuildHash: %s\n", BuildTS, BuildHash)
}
