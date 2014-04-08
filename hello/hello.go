package main

import (
    "labix.org/v2/mgos"
    "fmt"
    "math"
)

func main() {
    fmt.Printf("Now you have %g problems.",
        math.Nextafter(2, 3))
}
