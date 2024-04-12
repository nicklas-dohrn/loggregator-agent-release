package syslog

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"code.cloudfoundry.org/go-loggregator/v10/rpc/loggregator_v2"
	metrics "code.cloudfoundry.org/go-metric-registry"
	"code.cloudfoundry.org/loggregator-agent-release/src/pkg/egress"
	"github.com/valyala/fasthttp"
)

const BATCHSIZE = 256 * 1024

type HTTPSWriter struct {
	hostname        string
	appID           string
	url             *url.URL
	client          *fasthttp.Client
	egressMetric    metrics.Counter
	syslogConverter *Converter
}

type BatchHTTPSWriter struct {
	HTTPSWriter
	msgBatch     bytes.Buffer
	batchSize    int
	sendInterval time.Duration
	sendTimer    TriggerTimer
	egrMsgCount  float64
}

func NewHTTPSWriter(
	binding *URLBinding,
	netConf NetworkTimeoutConfig,
	tlsConf *tls.Config,
	egressMetric metrics.Counter,
	c *Converter,
) egress.WriteCloser {

	client := httpClient(netConf, tlsConf)
	if binding.URL.Query().Get("batching") == "true" {
		return &BatchHTTPSWriter{
			HTTPSWriter: HTTPSWriter{
				url:             binding.URL,
				appID:           binding.AppID,
				hostname:        binding.Hostname,
				client:          client,
				egressMetric:    egressMetric,
				syslogConverter: c,
			},
			batchSize:    BATCHSIZE,
			sendInterval: time.Second,
			egrMsgCount:  0,
		}
	} else {
		return &HTTPSWriter{
			url:             binding.URL,
			appID:           binding.AppID,
			hostname:        binding.Hostname,
			client:          client,
			egressMetric:    egressMetric,
			syslogConverter: c,
		}
	}
}

func (w *BatchHTTPSWriter) sendMsgBatch() error {
	currentEgrCount := w.egrMsgCount
	currentMsg := w.msgBatch.Bytes()

	w.egrMsgCount = 0
	w.msgBatch.Reset()

	return w.sendHttpRequest(currentMsg, currentEgrCount)
}

// Modified Write function
func (w *BatchHTTPSWriter) Write(env *loggregator_v2.Envelope) error {
	msgs, err := w.syslogConverter.ToRFC5424(env, w.hostname)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		w.msgBatch.Write(msg)
		w.egrMsgCount += 1
		w.startAndTriggerSend()
	}
	return nil
}

// TODO: Error back propagation. Errors are not looked at further down the call chain
func (w *BatchHTTPSWriter) startAndTriggerSend() {
	if !w.sendTimer.Running() {
		w.sendTimer.Start(w.sendInterval, func() {
			w.sendMsgBatch()
		})
	}
	if w.msgBatch.Len() >= w.batchSize {
		w.sendTimer.Trigger()
	}
}

func (w *HTTPSWriter) sendHttpRequest(msg []byte, msgCount float64) error {
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(w.url.String())
	req.Header.SetMethod("POST")
	req.Header.SetContentType("text/plain")
	req.SetBody(msg)

	resp := fasthttp.AcquireResponse()

	err := w.client.Do(req, resp)
	if err != nil {
		return w.sanitizeError(w.url, err)
	}

	if resp.StatusCode() < 200 || resp.StatusCode() > 299 {
		return fmt.Errorf("syslog Writer: Post responded with %d status code", resp.StatusCode())
	}

	w.egressMetric.Add(msgCount)

	return nil
}

func (w *HTTPSWriter) Write(env *loggregator_v2.Envelope) error {
	msgs, err := w.syslogConverter.ToRFC5424(env, w.hostname)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		err = w.sendHttpRequest(msg, 1)
		if err != nil {
			return err
		}
	}

	return nil
}

func (*HTTPSWriter) sanitizeError(u *url.URL, err error) error {
	if u == nil || u.User == nil {
		return err
	}

	if user := u.User.Username(); user != "" {
		err = errors.New(strings.Replace(err.Error(), user, "<REDACTED>", -1))
	}

	if p, ok := u.User.Password(); ok {
		err = errors.New(strings.Replace(err.Error(), p, "<REDACTED>", -1))
	}
	return err
}

func (*HTTPSWriter) Close() error {
	return nil
}

func httpClient(netConf NetworkTimeoutConfig, tlsConf *tls.Config) *fasthttp.Client {
	return &fasthttp.Client{
		MaxConnsPerHost:     5,
		MaxIdleConnDuration: 90 * time.Second,
		TLSConfig:           tlsConf,
		ReadTimeout:         20 * time.Second,
		WriteTimeout:        20 * time.Second,
	}
}
