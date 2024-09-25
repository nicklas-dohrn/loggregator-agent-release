package syslog

import (
	"bytes"
	"crypto/tls"
	"log"
	"time"

	"code.cloudfoundry.org/go-loggregator/v10/rpc/loggregator_v2"
	metrics "code.cloudfoundry.org/go-metric-registry"
	"code.cloudfoundry.org/loggregator-agent-release/src/pkg/egress"
)

const BATCHSIZE = 1024 * 1024 // 1 Mbyte payload for batches

type HTTPSBatchWriter struct {
	HTTPSWriter
	msgs          chan []byte
	batchSize     int
	sendInterval  time.Duration
	egrMsgCount   float64
	retryDuration RetryDuration
	maxRetries    int
	binding       *URLBinding
}

func NewHTTPSBatchWriter(
	binding *URLBinding,
	netConf NetworkTimeoutConfig,
	tlsConf *tls.Config,
	egressMetric metrics.Counter,
	c *Converter,
	retryDuration RetryDuration,
	maxRetries int,

) egress.WriteCloser {
	client := httpClient(netConf, tlsConf)
	binding.URL.Scheme = "https" // reset the scheme for usage to a valid http scheme
	BatchWriter := &HTTPSBatchWriter{
		HTTPSWriter: HTTPSWriter{
			url:             binding.URL,
			appID:           binding.AppID,
			hostname:        binding.Hostname,
			client:          client,
			egressMetric:    egressMetric,
			syslogConverter: c,
		},
		batchSize:    BATCHSIZE,
		sendInterval: 1 * time.Second,
		egrMsgCount:  0,
		msgs:         make(chan []byte),
		binding:      binding,
	}
	go BatchWriter.startSender()
	return BatchWriter
}

// Modified Write function
func (w *HTTPSBatchWriter) Write(env *loggregator_v2.Envelope) error {
	msgs, _ := w.syslogConverter.ToRFC5424(env, w.hostname)

	for _, msg := range msgs {
		w.msgs <- msg
	}
	return nil
}

func (w *HTTPSBatchWriter) startSender() {
	t := time.NewTimer(w.sendInterval)
	var msgBatch bytes.Buffer
	var msgCount float64

	for {
		select {
		case msg := <-w.msgs:
			msgBatch.Write(msg)
			msgCount++
			if msgBatch.Len() >= w.batchSize {
				w.SendWithRetry(msgBatch.Bytes(), msgCount, 0)
				msgBatch.Reset()
				msgCount = 0
				t.Reset(w.sendInterval)
			}
		case <-t.C:
			if msgBatch.Len() > 0 {
				w.SendWithRetry(msgBatch.Bytes(), msgCount, 0)
				msgBatch.Reset()
				msgCount = 0
			}
			t.Reset(w.sendInterval)
		}
	}
}

func (w *HTTPSBatchWriter) SendWithRetry(batch []byte, msgcount float64, retryNr int) {
	logTemplate := "failed to write to %s, retrying in %s, err: %s"

	if retryNr >= w.maxRetries {
		return
	}

	err := w.sendHttpRequest(batch, msgcount)
	if err == nil {
		return
	}

	if egress.ContextDone(w.binding.Context) {
		return
	}
	sleepDuration := w.retryDuration(retryNr)
	log.Printf(logTemplate, w.binding.URL.Host, sleepDuration, err)
	go func() {
		time.Sleep(sleepDuration)
		w.SendWithRetry(batch, msgcount, retryNr)
	}()
}
