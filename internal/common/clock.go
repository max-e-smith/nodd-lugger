package common

import (
	"fmt"
	"math"
	"time"
)

func MinutesSince(start time.Time) float64 {
	seconds := time.Since(start).Minutes()
	return math.Trunc(seconds*100) / 100
}

func HoursSince(start time.Time) float64 {
	seconds := time.Since(start).Hours()
	return math.Trunc(seconds*100) / 100
}

func LogDownloadTime(start time.Time) {
	fmt.Printf("Download completed in %g hours.\n", HoursSince(start))
}
