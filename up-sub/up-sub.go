package main

import (
	"bytes"
	"context"
	"flag"
	"os"
	"time"
	"syscall"
	"os/signal"
	"net/url"
	"net/http"

	"github.com/pkg/errors"

	"github.com/oklog/run"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/nats-io/nats.go"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"

	//consulapi "github.com/hashicorp/consul/api"
)

func main() {
	var (
		remoteWriterTimeout time.Duration = 1 * time.Second
	)
	opts := struct {
		Endpoint     string
		Subject      string
		RemoteWriter string
	}{}

	flag.StringVar(&opts.Endpoint, "endpoint", nats.DefaultURL, "the nats server URLs (separated by comma)")
	flag.StringVar(&opts.Subject, "subject", "subject", "the nats subject to connect to")
	flag.StringVar(&opts.RemoteWriter, "remote-writer", "http://localhost:9090/api/prom/receive", "the remote-writer endpoint")

	flag.Parse()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.WithPrefix(logger, "ts", log.DefaultTimestampUTC)
	logger = log.WithPrefix(logger, "caller", log.DefaultCaller)

	remotewriter, err := url.ParseRequestURI(opts.RemoteWriter)
	if err != nil {
		level.Error(logger).Log("msg", "-remote-writer is invalid", "err", err)
		return
	}

	var g run.Group
	{
		sig := make(chan os.Signal, 1)
		g.Add(func() error {
			signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
			<-sig
			return nil
		}, func(_ error) {
			level.Info(logger).Log("msg", "caught interrrupt")
			close(sig)
		})
	}
	{
		bg, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			level.Info(logger).Log("msg", "starting the up-sub")

			natsopts := []nats.Option{
				nats.Name("up-sub"),
				nats.PingInterval(20 * time.Second),
				nats.MaxReconnects(-1),
				nats.ReconnectWait(100 * time.Millisecond),
				nats.ClosedHandler(func(_conn *nats.Conn) {
					level.Debug(logger).Log("msg", "nats connection closed")
					if _conn != nil {
						lastErr := _conn.LastError()
						if lastErr != nil {
							level.Error(logger).Log("msg", "nats connection closed", "err", lastErr.Error())
						}
					}
				}),
				nats.DisconnectErrHandler(func(_conn *nats.Conn, _err error) {
					level.Debug(logger).Log("msg", "nats connection disconnected")
				}),
				nats.ReconnectHandler(func(_conn *nats.Conn) {
					level.Debug(logger).Log("msg", "nats connection reestablished")
				}),
				nats.DiscoveredServersHandler(func(_conn *nats.Conn) {
					level.Debug(logger).Log("msg", "nats connection discovered peers")
				}),
				nats.ErrorHandler(func(_conn *nats.Conn, sub *nats.Subscription, _err error) {
					if _err != nil {
						level.Error(logger).Log("msg", "nats asynchronous error", "err", _err.Error())
					}
				}),

			}

			nc, err := nats.Connect(opts.Endpoint, natsopts...)
			if err != nil {
				level.Error(logger).Log("msg", "failed to connect to nats", "err", err)
			}
			level.Info(logger).Log("msg", "nats connected", "server", opts.Endpoint)

			if _, err := nc.QueueSubscribe(opts.Subject, "worker", func(msg *nats.Msg) {
				decoded, err := validateMessage(msg)
				if err == nil {
					level.Info(logger).Log("msg", "msg received", "content", decoded.String())

					ctx, cancel := context.WithTimeout(bg, remoteWriterTimeout)
					encoded := snappy.Encode(nil, msg.Data)
					if err := postMessage(ctx, remotewriter, encoded); err != nil {
						level.Error(logger).Log("msg", "failed to make request", "err", err)
					}
					cancel()
				} else {
					level.Error(logger).Log("msg", "error decoding message", "err", err)
				}
			}); err != nil {
				level.Error(logger).Log("msg", "error receiving message", "err", err)
			}
			nc.Flush()

			if err := nc.LastError(); err != nil {
				level.Error(logger).Log("msg", "nats last error", "err", err)
			}

			level.Info(logger).Log("msg", "listening", "subject", opts.Subject)

			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt)
			<-c
			nc.Drain()
			level.Info(logger).Log("msg", "draining messages", "err", nil)


			<-bg.Done()
			return bg.Err()

			}, func(_ error) {
			cancel()
		})
	}

	if err := g.Run(); err != nil {
		level.Error(logger).Log("msg", "run error", "err", err)
	}
}


func validateMessage(m *nats.Msg) (prompb.WriteRequest, error) {
	var (
		buf prompb.WriteRequest
		err error
	)

	err = buf.Unmarshal(m.Data)
	if err != nil {
		return buf, errors.Wrap(err, "unmarshalling proto")
	}
	return buf, nil
}

func postMessage(ctx context.Context, remotewriter *url.URL, msg []byte) error {
	var (
		err error
		req *http.Request
		res *http.Response
	)

	req, err = http.NewRequest("POST", remotewriter.String(), bytes.NewBuffer(msg))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	res, err = http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "making request")
	}
	if res.StatusCode != http.StatusOK {
		err = errors.New(res.Status)
		return errors.Wrap(err, "non-200 status")
	}
	return nil
}

//func lookupConsul(serviceName string) (string, error) {
//	consul, err := consulapi.NewClient(consulapi.DefaultConfig())
//	if err != nil {
//		return "", err
//	}

//	services, err := consul.Agent().Services()
//	if err != nil {
//		return "", err
//	}

//	srvc := services[serviceName]
//	address := srvc.Address
//	port := srvc.Port
//	return fmt.Sprintf("http://%s:%v", address, port), nil
//}
