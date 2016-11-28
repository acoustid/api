package main

import (
	"google.golang.org/grpc"
	"gopkg.in/urfave/cli.v1"
	"log"
	"net"
	"strconv"
	"os/signal"
	"os"
	"golang.org/x/net/context"
)

var server2Command = cli.Command{
	Name:  "server2",
	Usage: "Runs the index server",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "host", Value: "localhost", Usage: "address on which to listen"},
		cli.IntFlag{Name: "port", Value: 7765, Usage: "port number on which to listen"},
	},
	Action: runServer2,
}

func runServer2(c *cli.Context) error {
	addr := net.JoinHostPort(c.String("host"), strconv.Itoa(c.Int("port")))
	log.Printf("listening on %v", addr)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv := grpc.NewServer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go func() {
		counter := 0
		for {
			select {
			case <-interrupt:
				counter++
				if counter == 1 {
					log.Print("gracefully stopping the server")
					go srv.GracefulStop()
				} else {
					log.Print("stopping the server")
					signal.Stop(interrupt)
					go srv.Stop()
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return srv.Serve(lis)
}
