package syslog

import (
	"bytes"
	"context"
	"crypto/tls"
	"log"
	"sync"
	"time"

	"code.cloudfoundry.org/go-loggregator/v10/rpc/loggregator_v2"
	metrics "code.cloudfoundry.org/go-metric-registry"
	"code.cloudfoundry.org/loggregator-agent-release/src/pkg/egress"
	"github.com/valyala/fasthttp"
)

// InternalRetryWriter configures retry behavior for writers that support retries.
type InternalRetryWriter interface {
	ConfigureRetry(retryDuration RetryDuration, maxRetries int)
}

// RetryDuration defines a function to calculate retry delay based on attempt number.
type RetryDuration func(attempt int) time.Duration

const (
	defaultMaxRetries    = 5
	defaultBatchSize     = 256 * 1024 // 256KB
	defaultSendInterval  = 1 * time.Second
	defaultNumWorkers    = 4
	defaultMsgChanBuffer = 1024
)

// Retryer performs retry logic with backoff and max retries.
type Retryer struct {
	retryDuration RetryDuration
	maxRetries    int
	binding       *URLBinding
}

// NewRetryer returns a Retryer configured with binding, retry duration function, and max retries.
func NewRetryer(
	binding *URLBinding,
	retryDuration RetryDuration,
	maxRetries int,
) *Retryer {
	return &Retryer{
		retryDuration: retryDuration,
		maxRetries:    maxRetries,
		binding:       binding,
	}
}

// Retry executes the function with retries on failure, respecting context cancellation.
func (r *Retryer) Retry(ctx context.Context, batch []byte, msgCount float64, function func([]byte, float64) error) {
	const logTemplate = "failed to write to %s, retrying in %s, err: %v"

	var err error
	for i := 0; i <= r.maxRetries; i++ {
		err = function(batch, msgCount)
		if err == nil {
			return
		}

		select {
		case <-ctx.Done():
			log.Printf("Context cancelled for %s, aborting retries", r.binding.URL.Host)
			return
		default:
		}

		sleepDuration := r.retryDuration(i)
		log.Printf(logTemplate, r.binding.URL.Host, sleepDuration, err)
		time.Sleep(sleepDuration)
	}

	log.Printf("Exhausted retries for %s, dropping batch, err: %v", r.binding.URL.Host, err)
}

// HTTPSBatchWriter batches messages and sends them over HTTPS with retry logic.
type HTTPSBatchWriter struct {
	HTTPSWriter
	batchSize    int
	dispatcher   *dispatcher
	sendInterval time.Duration
	retryer      *Retryer
	msgChan      chan []byte
	quit         chan struct{}
	wg           sync.WaitGroup
	closeOnce    sync.Once
}

// ConfigureRetry updates retry parameters on the HTTPSBatchWriter.
func (w *HTTPSBatchWriter) ConfigureRetry(retryDuration RetryDuration, maxRetries int) {
	w.retryer.retryDuration = retryDuration
	w.retryer.maxRetries = maxRetries
}

// Option configures HTTPSBatchWriter options.
type Option func(*HTTPSBatchWriter)

// WithBatchSize sets the batch size limit for HTTPSBatchWriter.
func WithBatchSize(size int) Option {
	return func(w *HTTPSBatchWriter) {
		w.batchSize = size
	}
}

// WithSendInterval sets the send interval duration for HTTPSBatchWriter.
func WithSendInterval(interval time.Duration) Option {
	return func(w *HTTPSBatchWriter) {
		w.sendInterval = interval
	}
}

// NewHTTPSBatchWriter creates and initializes an HTTPSBatchWriter.
func NewHTTPSBatchWriter(
	binding *URLBinding,
	netConf NetworkTimeoutConfig,
	tlsConf *tls.Config,
	egressMetric metrics.Counter,
	c *Converter,
	options ...Option,
) egress.WriteCloser {
	client := httpBatchClient(netConf, tlsConf)
	binding.URL.Scheme = "https"

	writer := &HTTPSBatchWriter{
		HTTPSWriter: HTTPSWriter{
			url:             binding.URL,
			appID:           binding.AppID,
			hostname:        binding.Hostname,
			client:          client,
			egressMetric:    egressMetric,
			syslogConverter: c,
		},
		retryer: &Retryer{
			binding:       binding,
			retryDuration: defaultBackoff,
			maxRetries:    defaultMaxRetries,
		},
		batchSize:    defaultBatchSize,
		sendInterval: defaultSendInterval,
		msgChan:      make(chan []byte, defaultMsgChanBuffer),
		quit:         make(chan struct{}),
	}

	for _, opt := range options {
		opt(writer)
	}

	d := &dispatcher{}
	for i := 0; i < defaultNumWorkers; i++ {
		wrk := &worker{
			id:      i,
			input:   make(chan batch, 5), // buffered to prevent blocking dispatcher
			retryer: writer.retryer,
			writer:  writer,
		}
		go wrk.start()
		d.workers = append(d.workers, wrk)
	}
	writer.dispatcher = d

	writer.wg.Add(1)
	go writer.startSender()

	return writer
}

// Write converts an Envelope to syslog messages and queues them for batching.
// If the msgChan is full, messages are dropped with a warning.
func (w *HTTPSBatchWriter) Write(env *loggregator_v2.Envelope) error {
	msgs, err := w.syslogConverter.ToRFC5424(env, w.hostname)
	if err != nil {
		log.Printf("Failed to parse syslog, dropping message, err: %v", err)
		return nil
	}

	for _, msg := range msgs {
		select {
		case w.msgChan <- msg:
		default:
			log.Printf("msgChan full, dropping message")
			// Consider adding a metric for dropped messages here
		}
	}

	return nil
}

// startSender batches messages and dispatches them at intervals or when batch size limit is reached.
func (w *HTTPSBatchWriter) startSender() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.sendInterval)
	defer ticker.Stop()

	var msgBatch bytes.Buffer
	var msgCount float64

	sendBatch := func() {
		if msgBatch.Len() > 0 {
			batchCopy := make([]byte, msgBatch.Len())
			copy(batchCopy, msgBatch.Bytes())

			w.dispatcher.dispatch(batch{
				data:     batchCopy,
				msgCount: msgCount,
			})

			msgBatch.Reset()
			msgCount = 0
		}
	}

	for {
		select {
		case msg, ok := <-w.msgChan:
			if !ok {
				sendBatch()
				return
			}
			if _, err := msgBatch.Write(msg); err != nil {
				log.Printf("Failed to write to buffer, dropping batch of size %d, err: %v", msgBatch.Len(), err)
				msgBatch.Reset()
				msgCount = 0
				continue
			}
			msgCount++
			if msgBatch.Len() >= w.batchSize {
				sendBatch()
			}
		case <-ticker.C:
			sendBatch()
		case <-w.quit:
			sendBatch()
			return
		}
	}
}

// Close gracefully shuts down the HTTPSBatchWriter and stops all workers.
func (w *HTTPSBatchWriter) Close() error {
	w.closeOnce.Do(func() {
		// Close msgChan first to stop Write() from sending more messages
		close(w.msgChan)

		// Signal startSender to quit after draining remaining messages
		close(w.quit)

		// Wait for startSender to finish processing
		w.wg.Wait()

		if w.dispatcher != nil {
			w.dispatcher.stop()
		}
	})
	return nil
}

// httpBatchClient creates a fasthttp.Client with custom timeout configurations.
func httpBatchClient(netConf NetworkTimeoutConfig, tlsConf *tls.Config) *fasthttp.Client {
	client := httpClient(netConf, tlsConf)
	client.MaxIdleConnDuration = 30 * time.Second
	client.MaxConnDuration = 30 * time.Second
	return client
}

// batch represents a batch of bytes to send with message count.
type batch struct {
	data     []byte
	msgCount float64
}

// worker receives batches from input channel and sends them with retries.
type worker struct {
	id      int
	input   chan batch
	retryer *Retryer
	writer  *HTTPSBatchWriter
}

// start listens on input channel and sends batches with retry logic.
func (w *worker) start() {
	for b := range w.input {
		ctx := context.Background()
		if w.retryer.binding.Context != nil {
			ctx = w.retryer.binding.Context
		}
		w.retryer.Retry(ctx, b.data, b.msgCount, w.writer.sendHttpRequest)
	}
}

// dispatcher distributes batches to workers in a round-robin fashion.
type dispatcher struct {
	workers    []*worker
	nextWorker int
	mu         sync.Mutex
}

// dispatch sends a batch to the next worker in round-robin order.
func (d *dispatcher) dispatch(b batch) {
	d.mu.Lock()
	defer d.mu.Unlock()
	w := d.workers[d.nextWorker]
	d.nextWorker = (d.nextWorker + 1) % len(d.workers)
	w.input <- b
}

// stop closes all workers' input channels to stop processing.
func (d *dispatcher) stop() {
	for _, w := range d.workers {
		close(w.input)
	}
}

// defaultBackoff is a simple exponential backoff function used as default.
func defaultBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 100 * time.Millisecond
	}
	// Cap max backoff at 5 seconds
	backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
	if backoff > 5*time.Second {
		return 5 * time.Second
	}
	return backoff
}
