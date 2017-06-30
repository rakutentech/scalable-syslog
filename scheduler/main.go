package main

import (
	"log"
	"net"
	"os"
	"time"

	"net/http"
	_ "net/http/pprof"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/scalable-syslog/internal/api"
	"code.cloudfoundry.org/scalable-syslog/scheduler/app"
)

func main() {
	cfg, err := app.LoadConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("Invalid config: %s", err)
	}

	apiTLSConfig, err := api.NewMutualTLSConfig(
		cfg.APICertFile,
		cfg.APIKeyFile,
		cfg.APICAFile,
		cfg.APICommonName,
	)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}
	apiTLSConfig.InsecureSkipVerify = cfg.APISkipCertVerify

	adapterTLSConfig, err := api.NewMutualTLSConfig(
		cfg.CertFile,
		cfg.KeyFile,
		cfg.CAFile,
		cfg.AdapterCommonName,
	)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}

	metricIngressTLS, err := api.NewMutualTLSConfig(
		cfg.CertFile,
		cfg.KeyFile,
		cfg.CAFile,
		cfg.MetricIngressCN,
	)
	if err != nil {
		log.Fatalf("Invalid Metric Ingress TLS config: %s", err)
	}

	loggClient, err := loggregator.NewIngressClient(
		metricIngressTLS,
		loggregator.WithStringTag("origin", "scalablesyslog.scheduler"),
		loggregator.WithAddr(cfg.MetricIngressAddr),
	)
	if err != nil {
		log.Fatalf("Couldn't connect to metric ingress server: %s", err)
	}

	// metric-documentation-v2: setup function
	metricClient := pulseemitter.New(
		loggClient,
		pulseemitter.WithPulseInterval(cfg.MetricEmitterInterval),
	)

	scheduler := app.NewScheduler(
		cfg.APIURL,
		cfg.AdapterAddrs,
		adapterTLSConfig,
		metricClient,
		app.WithOptIn(cfg.RequireOptIn),
		app.WithHealthAddr(cfg.HealthHostport),
		app.WithHTTPClient(api.NewHTTPSClient(apiTLSConfig, 5*time.Second)),
		app.WithBlacklist(cfg.Blacklist),
		app.WithPollingInterval(cfg.APIPollingInterval),
	)
	scheduler.Start()

	lis, err := net.Listen("tcp", cfg.PprofHostport)
	if err != nil {
		log.Printf("Error creating pprof listener: %s", err)
	}

	log.Printf("Starting pprof server on: %s", lis.Addr().String())
	log.Println(http.Serve(lis, nil))
}
