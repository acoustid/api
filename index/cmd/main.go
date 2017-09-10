// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package cmd

import (
	"gopkg.in/urfave/cli.v1"
)

var IndexCommand = cli.Command{
	Name:  "index",
	Usage: "AcoustID index",
	Subcommands: []cli.Command{
		exportCommand,
		importCommand,
		loadCommand,
		serverCommand,
		dumpOldSegmentCommand,
	},
}
