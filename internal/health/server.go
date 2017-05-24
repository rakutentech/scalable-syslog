package health

import (
	"log"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartServer(addr string) string {
	router := http.NewServeMux()
	router.Handle("/metrics", promhttp.Handler())

	server := http.Server{
		Addr:         addr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	server.Handler = router

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Unable to setup Health endpoint (%s): %s", addr, err)
	}

	go func() {
		log.Printf("Metrics endpoint is listening on %s", lis.Addr().String())
		log.Fatalf("Metrics server closing: %s", server.Serve(lis))
	}()
	return lis.Addr().String()
}
