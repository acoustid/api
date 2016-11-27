// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package main

import (
	"gopkg.in/urfave/cli.v1"
	"log"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Name = "aindex"
	app.HelpName = os.Args[0]
	app.Usage = "AcoustID audio fingerprint index"
	app.HideVersion = true
	app.Commands = []cli.Command{
		serverCommand,
	}
	app.Before = func(ctx *cli.Context) error {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
		return nil
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
