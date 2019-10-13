package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/golang/snappy"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

type labelArg []prompb.Label

func (la *labelArg) String() string {
	var ls []string
	for _, l := range *la {
		ls = append(ls, l.Name+"="+l.Value)
	}
	return strings.Join(ls, ", ")
}

func (la *labelArg) Set(v string) error {
	var lset []prompb.Label
	for _, l := range strings.Split(v, ",") {
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			return errors.Errorf("unrecognized label %q", l)
		}
		if !model.LabelName.IsValid(model.LabelName(string(parts[0]))) {
			return errors.Errorf("unsupported format for label %s", l)
		}
		val, err := strconv.Unquote(parts[1])
		if err != nil {
			return errors.Wrap(err, "unquote label value")
		}
		lset = append(lset, prompb.Label{Name: parts[0], Value: val})
	}
	*la = labelArg(lset)
	return nil
}

func main() {
	opts := struct {
		Endpoint string
		Labels   labelArg
		Name     string
		Interval string
		Subject  string
	}{}

	flag.StringVar(&opts.Endpoint, "endpoint", nats.DefaultURL, "the nats server URLs (separated by comma)")
	flag.Var(&opts.Labels, "labels", "the lables that should be applied to requests")
	flag.StringVar(&opts.Name, "name", "up", "the name of the metric to send")
	flag.StringVar(&opts.Interval, "interval", "15s", "the time to wait between requests")
	flag.StringVar(&opts.Subject, "subject", "subject", "the nats subject to connect to")

	flag.Parse()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.WithPrefix(logger, "ts", log.DefaultTimestampUTC)
	logger = log.WithPrefix(logger, "caller", log.DefaultCaller)

	period, err := time.ParseDuration(opts.Interval)
	if err != nil {
		level.Error(logger).Log("msg", "-interval is invalid", "err", err)
		return
	}
	opts.Labels = append(opts.Labels, prompb.Label{
		Name:  "__name__",
		Value: opts.Name,
	})

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
		t := time.NewTicker(period)
		bg, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			level.Info(logger).Log("msg", "starting up-pub")
			for {
				select {
				case <-t.C:
					ctx, cancel := context.WithTimeout(bg, period)
					if err := post(ctx, opts.Endpoint, generate(opts.Labels), opts.Subject, logger); err != nil {
						level.Error(logger).Log("msg", "failed to make request", "err", err)
					}
					cancel()
				case <-bg.Done():
					return nil
				}
			}
		}, func(_ error) {
			t.Stop()
			cancel()
		})
	}

	if err := g.Run(); err != nil {
		level.Error(logger).Log("msg", "run error", "err", err)
	}
}

func generate(labels []prompb.Label) *prompb.WriteRequest {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	w := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: labels,
				Samples: []prompb.Sample{
					{
						Value:     float64(now),
						Timestamp: now,
					},
				},
			},
		},
	}
	return &w
}

func post(ctx context.Context, endpoint string, wreq *prompb.WriteRequest, subject string, logger log.Logger) error {
	var (
		buf []byte
		err error
	)

	opts := []nats.Option{nats.Name("up-pub")}

	nc, err := nats.Connect(endpoint, opts...)
	if err != nil {
		level.Error(logger).Log("msg", "failed to connect to nats", "err", err)
		return err
	}
	defer nc.Close()

	buf, err = proto.Marshal(wreq)
	if err != nil {
		return errors.Wrap(err, "marshalling proto")
	}

	nc.Publish(subject, snappy.Encode(nil, buf))
	nc.Flush()

	if err := nc.LastError(); err != nil {
		level.Error(logger).Log("msg", "nats last error", "err", err)
		return err
	} else {
		level.Info(logger).Log("msg", "msg sent", "content", wreq)
	}
	return nil
}

