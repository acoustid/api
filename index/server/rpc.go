package server

import (
	"github.com/acoustid/go-acoustid/index"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	_ "expvar"
	"github.com/acoustid/go-acoustid/index/server/pb"
)

type indexServer struct{
	db *index.DB
}

func (s *indexServer) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateReply, error) {
	err := s.db.RunInTransaction(func(batch index.Batch) error {
		for _, docID := range req.DocsToDelete {
			err := batch.Delete(docID)
			if err != nil {
				return err
			}
		}
		for _, doc := range req.DocsToAdd {
			err := batch.Add(doc.Id, doc.Terms)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return &pb.UpdateReply{}, err
}

func (s *indexServer) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	hits, err := s.db.Search(req.Terms)
	if err != nil {
		return nil, err
	}

	var reply pb.SearchReply
	reply.Hits = make([]*pb.SearchReply_Hit, 0, len(hits))
	for id, score := range hits {
		hit := pb.SearchReply_Hit{Id: id, Score: uint32(score)}
		reply.Hits = append(reply.Hits, &hit)
	}

	return &reply, nil
}

func Run(db *index.DB, host string, port int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := net.JoinHostPort(host, strconv.Itoa(port))

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("RPC server is listening on %v", lis.Addr())

	srv := grpc.NewServer()
	pb.RegisterIndexServer(srv, &indexServer{db: db})

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go func() {
		counter := 0
		for {
			select {
			case <-interrupt:
				counter++
				if counter == 1 {
					log.Print("gracefully stopping server")
					go srv.GracefulStop()
				} else {
					log.Print("stopping server")
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
