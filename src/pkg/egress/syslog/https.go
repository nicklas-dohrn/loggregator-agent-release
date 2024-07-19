package syslog

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	"code.cloudfoundry.org/go-loggregator/v9/rpc/loggregator_v2"
	metrics "code.cloudfoundry.org/go-metric-registry"
	"code.cloudfoundry.org/loggregator-agent-release/src/pkg/egress"
	"github.com/valyala/fasthttp"
)

type HTTPSWriter struct {
	hostname        string
	appID           string
	url             *url.URL
	client          *fasthttp.Client
	egressMetric    metrics.Counter
	syslogConverter *Converter
}

func NewHTTPSWriter(
	binding *URLBinding,
	netConf NetworkTimeoutConfig,
	tlsConf *tls.Config,
	egressMetric metrics.Counter,
	c *Converter,
) egress.WriteCloser {

	client := httpClient(netConf, tlsConf)
	return &HTTPSWriter{
		url:             binding.URL,
		appID:           binding.AppID,
		hostname:        binding.Hostname,
		client:          client,
		egressMetric:    egressMetric,
		syslogConverter: c,
	}
}

func (w *HTTPSWriter) sendHttpRequest(msg []byte, msgCount float64) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(w.url.String())
	req.Header.SetMethod("POST")
	req.Header.SetContentType("text/plain")
	req.Header.SetContentEncoding("gzip")

	var compressed bytes.Buffer
	zip, _ := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	zip.Write(msg)
	zip.Close()
	req.SetBody(compressed.Bytes())

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	w.client.Do(req, resp)

	if resp.StatusCode() < 200 || resp.StatusCode() > 299 {
		fmt.Printf("syslog Writer: Post responded with non-2xx status code")
		return
	}

	w.egressMetric.Add(msgCount)
}

func (w *HTTPSWriter) Write(env *loggregator_v2.Envelope) error {
	msgs, err := w.syslogConverter.ToRFC5424(env, w.hostname)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		go w.sendHttpRequest(msg, 1)
	}

	return nil
}

func (*HTTPSWriter) Close() error {
	return nil
}

func httpClient(_ NetworkTimeoutConfig, tlsConf *tls.Config) *fasthttp.Client {
	return &fasthttp.Client{
		ConnPoolStrategy:    fasthttp.LIFO,
		MaxIdleConnDuration: 10 * time.Second,
		TLSConfig:           tlsConf,
	}
}
