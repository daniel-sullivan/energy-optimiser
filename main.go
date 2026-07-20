package main

import (
	_ "time/tzdata" // embed the IANA tz database so Asia/Tokyo always loads

	"energy-optimiser/cmd"
)

func main() {
	cmd.Execute()
}
