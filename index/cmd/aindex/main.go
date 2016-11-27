// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package main

import (
	"gopkg.in/urfave/cli.v1"
	"log"
	"os"
	"github.com/pkg/errors"
	"runtime/pprof"
)

var version = ""

func main() {
	app := cli.NewApp()

	app.Name = "aindex"
	app.HelpName = "aindex"
	app.Usage = "AcoustID audio fingerprint index"
	app.Version = version

	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "cpuprofile", Usage: "write cpu profile to file", Hidden: true},
	}

	app.Commands = []cli.Command{
		serverCommand,
		importCommand,
		exportCommand,
		loadCommand,
	}

	app.Before = func(ctx *cli.Context) error {
		if ctx.GlobalIsSet("cpuprofile") {
			file, err := os.Create(ctx.GlobalString("cpuprofile"))
			if err != nil {
				return errors.Wrap(err, "unable to create file for cpu profile")
			}
			pprof.StartCPUProfile(file)
		}
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
		return nil
	}

	app.After = func(ctx *cli.Context) error {
		if ctx.GlobalIsSet("cpuprofile") {
			pprof.StopCPUProfile()
		}
		return nil
	}

	app.RunAndExitOnError()
}
