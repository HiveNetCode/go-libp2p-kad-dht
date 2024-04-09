package net

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multihash"

	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-msgio"

	//lint:ignore SA1019 TODO migrate away from gogo pb
	"github.com/libp2p/go-msgio/protoio"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"

	"github.com/libp2p/go-libp2p-kad-dht/internal"
	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

var dhtReadMessageTimeout = 5 * time.Second        //@Hive: Setting the timeout to 5s
var dhtMessageSenderTimeout = 1 * time.Second      //@Hive: Adding timeout when creating Sender
var dhtDefaultBackoffTime = 60 * time.Second       //@Hive: 1mn Backoff time for unresponsive peers
var SmallReadWriteInterval = 10 * time.Millisecond // 100 writes per second
var MaxWriteError = 3                              // Maximum write error before tagging a peer as unresponsive
const streamReuseTries = 3

// ErrReadTimeout is an error that occurs when no message is read within the timeout period.
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// ErrEearlyReadTimeout is an early timeout sent to unresponsive peers
var ErrEearlyReadTimeout = fmt.Errorf("early time out")

var logger = logging.Logger("dht")

// messageSenderImpl is responsible for sending requests and messages to peers efficiently, including reuse of streams.
// It also tracks metrics for sent requests and messages.
type messageSenderImpl struct {
	host      host.Host // the network services we need
	smlk      sync.Mutex
	strmap    map[peer.ID]*peerMessageSender
	protocols []protocol.ID
}

func NewMessageSenderImpl(h host.Host, protos []protocol.ID) pb.MessageSenderWithDisconnect {
	return &messageSenderImpl{
		host:      h,
		strmap:    make(map[peer.ID]*peerMessageSender),
		protocols: protos,
	}
}

func (m *messageSenderImpl) OnDisconnect(ctx context.Context, p peer.ID) {
	m.smlk.Lock()
	defer m.smlk.Unlock()
	ms, ok := m.strmap[p]
	if !ok {
		return
	}
	delete(m.strmap, p)

	//defer ms.writeMutex.Unlock()

	// Do this asynchronously as ms.lk can block for a while.
	go func() {
		if err := ms.lk.Lock(ctx); err != nil {
			return
		}
		ms.writeMutex.Lock()
		defer ms.lk.Unlock()
		defer ms.writeMutex.Unlock()
		ms.invalidate()

	}()

	// Close Infinite Reader and Writer goroutines
	ms.writeMutex.Lock()
	defer ms.writeMutex.Unlock()
	if ms.running {
		ms.infiniteRwClose()
		ms.running = false

	}

}

// @Hive: Forges early timeout if the remote peer has been flagged unresponsive for less than backoffTime
func (m *peerMessageSender) IsUnresponsivePeer() (err error, isUnresponsive bool) {
	if m.isResponsive {
		return
	} else {
		if time.Since(m.addTime).Seconds() <= m.backoffTime.Seconds() { // the peer has recently failed, so we consider it is still unresponsive
			err = ErrEearlyReadTimeout
			isUnresponsive = true
		}
	}
	return
}

// SendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (m *messageSenderImpl) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))
	//@Hive: adding timeout for opening message sender
	ctxfast, cancelfast := context.WithTimeout(ctx, dhtMessageSenderTimeout)
	defer cancelfast()
	ms, err := m.messageSenderForPeer(ctxfast, p)
	if err != nil {
		stats.Record(ctx,
			metrics.SentRequests.M(1),
			metrics.SentRequestErrors.M(1),
		)
		logger.Debugw("request failed to open message sender", "error", err, "to", p)
		return nil, err
	}

	start := time.Now()

	rpmes, err := ms.SendRequest(ctx, pmes)
	if err != nil {
		stats.Record(ctx,
			metrics.SentRequests.M(1),
			metrics.SentRequestErrors.M(1),
		)
		logger.Debugw("request failed", "error", err, "to", p)
		return nil, err
	}

	stats.Record(ctx,
		metrics.SentRequests.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
		metrics.OutboundRequestLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
	)
	m.host.Peerstore().RecordLatency(p, time.Since(start))
	return rpmes, nil

}

// SendMessage sends out a message
func (m *messageSenderImpl) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))
	//@Hive: adding timeout for opening message sender
	ctxfast, cancelfast := context.WithTimeout(ctx, dhtMessageSenderTimeout)
	defer cancelfast()
	ms, err := m.messageSenderForPeer(ctxfast, p)
	if err != nil {
		stats.Record(ctx,
			metrics.SentMessages.M(1),
			metrics.SentMessageErrors.M(1),
		)
		logger.Debugw("message failed to open message sender", "error", err, "to", p)
		return err
	}

	if err := ms.SendMessage(ctx, pmes); err != nil {
		stats.Record(ctx,
			metrics.SentMessages.M(1),
			metrics.SentMessageErrors.M(1),
		)
		logger.Debugw("message failed", "error", err, "to", p)
		return err
	}

	stats.Record(ctx,
		metrics.SentMessages.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
	)
	return nil

}

func (m *messageSenderImpl) messageSenderForPeer(ctx context.Context, p peer.ID) (*peerMessageSender, error) {
	m.smlk.Lock()
	defer m.smlk.Unlock()
	ms, ok := m.strmap[p]
	if ok { // return message sender only if the writer/Reader are running
		return ms, nil
	}
	infContext := context.Background()
	ReaderWriterContext, ReaderWriterCancel := context.WithCancel(infContext)
	ms = &peerMessageSender{p: p, m: m, lk: internal.NewCtxMutex(), isResponsive: true, addTime: time.Now(),
		backoffTime: dhtDefaultBackoffTime, chanMap: make(map[string]chan MetaMessage),
		chanrequest: make(chan MessageInfo), chanmessage: make(chan MessageInfo), running: false,
		infiniteRwCtx: ReaderWriterContext, infiniteRwClose: ReaderWriterCancel}
	m.strmap[p] = ms

	if err := ms.prepOrInvalidate(ctx); err != nil {
		if msCur, ok := m.strmap[p]; ok {
			// Changed. Use the new one, old one is invalid and
			// not in the map so we can just throw it away.
			if ms != msCur {
				return msCur, nil
			}
			// Not changed, remove the now invalid stream from the
			// map.
			delete(m.strmap, p)

		}
		// Invalid but not in map. Must have been removed by a disconnect.
		return nil, err
	}
	// All ready to go.
	// Launching the Infinite reader
	ms.writeMutex.Lock()
	defer ms.writeMutex.Unlock()
	if !ms.running {
		ms.runInfReaderAndWriter(ms.infiniteRwCtx)
		ms.running = true
	}

	return ms, nil
}

// peerMessageSender is responsible for sending requests and messages to a particular peer
type peerMessageSender struct {
	s            network.Stream
	r            msgio.ReadCloser
	lk           internal.CtxMutex
	p            peer.ID
	m            *messageSenderImpl
	invalid      bool
	addTime      time.Time
	backoffTime  time.Duration
	isResponsive bool
	/*Hive addons*/
	writeMutex      sync.Mutex
	chanMapMutex    sync.Mutex
	chanMap         map[string]chan MetaMessage
	messageId       int
	chanrequest     chan MessageInfo
	chanmessage     chan MessageInfo
	running         bool
	infiniteRwCtx   context.Context
	infiniteRwClose context.CancelFunc
	retryCount      int // count the number of non-time-out error (e.g., write error, etc.)
	singleMes       int
}

// invalidate is called before this peerMessageSender is removed from the strmap.
// It prevents the peerMessageSender from being reused/reinitialized and then
// forgotten (leaving the stream open).
func (ms *peerMessageSender) invalidate() {
	ms.invalid = true
	if ms.s != nil {
		_ = ms.s.Reset()
		ms.s = nil
	}
}

func (ms *peerMessageSender) prepOrInvalidate(ctx context.Context) error {
	if err := ms.lk.Lock(ctx); err != nil {
		return err
	}
	defer ms.lk.Unlock()

	if err := ms.prep(ctx); err != nil {
		ms.invalidate()
		return err
	}
	return nil
}

func (ms *peerMessageSender) prep(ctx context.Context) error {
	if ms.invalid {
		return fmt.Errorf("message sender has been invalidated")
	}
	if ms.s != nil {
		return nil
	}

	// We only want to speak to peers using our primary protocols. We do not want to query any peer that only speaks
	// one of the secondary "server" protocols that we happen to support (e.g. older nodes that we can respond to for
	// backwards compatibility reasons).
	nstr, err := ms.m.host.NewStream(ctx, ms.p, ms.m.protocols...)
	if err != nil {
		return err
	}

	ms.r = msgio.NewVarintReaderSize(nstr, network.MessageSizeMax)
	ms.s = nstr

	return nil
}

func (ms *peerMessageSender) SendMessage(ctx context.Context, pmes *pb.Message) error {
	// no need to create ID, messages don't have answers like requests.
	// The remote peer doesn't answer to messages.
	err := func() (err error) {
		ms.writeMutex.Lock()
		defer ms.writeMutex.Unlock()
		if !ms.running {
			err = fmt.Errorf("infinite writer is not running")
			return
		}
		var unresponsive bool
		if err, unresponsive = ms.IsUnresponsivePeer(); unresponsive {
			logger.Debugw("lookup patch", "error", err, "to", ms.p, "message type", pmes.GetType().String())
			return
		}
		return

	}()

	if err != nil {
		return err
	}
	isInfReaderWriterRunning := func() (running bool) {
		ms.writeMutex.Lock()
		defer ms.writeMutex.Unlock()
		return ms.running

	}()
	if isInfReaderWriterRunning {
		rcv := make(chan MetaMessage)
		//defer close(rcv)
		messageWithInfo := MessageInfo{
			message:  pmes,
			err:      nil,
			receiver: rcv,
			ctx:      ctx,
		}
		// Send packaged request to infinite writer via the request channel
		stopsend := time.NewTimer(2 * time.Second) // timeout for writing to the Writer channel
		defer stopsend.Stop()
		select {
		case <-ms.infiniteRwCtx.Done():
			close(rcv)
			return fmt.Errorf("infinite message channel has been closed")
		case <-stopsend.C:
			close(rcv)
			return fmt.Errorf("timed out while writing on message channel")
		case ms.chanmessage <- messageWithInfo:
		}

		//ms.chanmessage <- messageWithInfo
		t := time.NewTimer(dhtReadMessageTimeout)
		defer t.Stop()
		select {
		case ret := <-rcv: // non time out error
			if ret.err != nil {
				ms.writeMutex.Lock()
				ms.retryCount += 1                  // increment the write error count
				if ms.retryCount >= MaxWriteError { // if Max write error is reached
					ms.UpdateUnresponsiveMap()
					ms.retryCount = 0
				}
				ms.writeMutex.Unlock()
			}
			return ret.err
		case <-t.C:
			return ErrReadTimeout
		}

	} else {
		return fmt.Errorf("infinite message channel has been closed")
	}

}

// A call of this function marks ms.p as unresponsive if it wasn't already.
// The peer is marked unresponsive for a initial backoff time of 60s.
// The backoff time increases exponentially (*2) if the function is called after the backoff time.
func (ms *peerMessageSender) UpdateUnresponsiveMap() {
	if !ms.isResponsive {
		if time.Since(ms.addTime).Seconds() >= ms.backoffTime.Seconds() {
			ms.addTime = time.Now()
			ms.backoffTime *= 2
			logger.Debugw("lookup patch", "exponential backoff for peer", ms.p, "new backoff (s)", ms.backoffTime.Seconds())

		}
	} else {
		ms.addTime = time.Now()
		ms.backoffTime = dhtDefaultBackoffTime
		ms.isResponsive = false
		logger.Debugw("lookup patch", "peer", ms.p, "will be considered unresponsive for (s)", ms.backoffTime)
	}
}

func (ms *peerMessageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	requestID, err := func() (id string, err error) {
		ms.writeMutex.Lock()
		defer ms.writeMutex.Unlock()
		if !ms.running {
			err = fmt.Errorf("infinite writer is not running")
			return
		}
		var unresponsive bool
		if err, unresponsive = ms.IsUnresponsivePeer(); unresponsive {
			logger.Debugw("lookup patch", "error", err, "to", ms.p, "request type", pmes.GetType().String())
			return
		}
		ms.SetRequestId(pmes)
		id = GetRequestId(pmes)
		return

	}()

	if err != nil {
		return nil, err
	}

	isInfReaderWriterRunning := func() (running bool) {
		ms.writeMutex.Lock()
		defer ms.writeMutex.Unlock()
		return ms.running
	}()
	if isInfReaderWriterRunning {
		rcv := make(chan MetaMessage)
		defer func() {
			// Delete the request ID and the chan from the Map
			ms.chanMapMutex.Lock()
			defer ms.chanMapMutex.Unlock()
			delete(ms.chanMap, requestID)
			//close(rcv)
		}()
		messageWithInfo := MessageInfo{
			message:  pmes,
			err:      nil,
			receiver: rcv,
			ctx:      ctx,
		}
		// Send packaged request to infinite writer via the request channel
		stopsend := time.NewTimer(2 * time.Second) // timeout for writing to the Writer channel
		defer stopsend.Stop()
		select {
		case <-ms.infiniteRwCtx.Done():
			close(rcv)
			return nil, fmt.Errorf("infinite message channel has been closed")
		case <-stopsend.C:
			close(rcv)
			return nil, fmt.Errorf("timed out while writing on message channel")
		case ms.chanrequest <- messageWithInfo:
		}

		//ms.chanrequest <- messageWithInfo
		// Define a ReadTimeout of 5s
		t := time.NewTimer(dhtReadMessageTimeout)
		defer t.Stop()
		select {
		case ret := <-rcv:
			if ret.err != nil { // non time out error
				ms.writeMutex.Lock()
				ms.retryCount += 1
				if ms.retryCount >= MaxWriteError { // allow 3 retries
					ms.UpdateUnresponsiveMap()
					ms.retryCount = 0
				}
				ms.writeMutex.Unlock()
			}
			return ret.message, ret.err

		case <-t.C:
			// flag the peer as unresponsive sice a timeout occurs
			ms.writeMutex.Lock()
			ms.UpdateUnresponsiveMap()
			ms.writeMutex.Unlock()
			return nil, ErrReadTimeout
		}
	} else {
		return nil, fmt.Errorf("infinite request channel has been closed")
	}

}

func (ms *peerMessageSender) writeMsg(pmes *pb.Message) error {
	return WriteMsg(ms.s, pmes)
}

// The Protobuf writer performs multiple small writes when writing a message.
// We need to buffer those writes, to make sure that we're not sending a new
// packet for every single write.
type bufferedDelimitedWriter struct {
	*bufio.Writer
	protoio.WriteCloser
}

var writerPool = sync.Pool{
	New: func() interface{} {
		w := bufio.NewWriter(nil)
		return &bufferedDelimitedWriter{
			Writer:      w,
			WriteCloser: protoio.NewDelimitedWriter(w),
		}
	},
}

func WriteMsg(w io.Writer, mes *pb.Message) error {
	if mes != nil { // add a check to avoid nil pointer
		bw := writerPool.Get().(*bufferedDelimitedWriter)
		bw.Reset(w)

		err := bw.WriteMsg(mes)
		if err == nil {
			err = bw.Flush()
		}
		bw.Reset(nil)
		writerPool.Put(bw)
		return err
	}
	return fmt.Errorf("nil proto message")
}

func (w *bufferedDelimitedWriter) Flush() error {
	return w.Writer.Flush()
}

/*** HIVE PATCH *****/

// construct and return request ID based on message type, key and ClusterLevelRaw fields
func GetRequestId(pmes *pb.Message) string {
	requestID := ""

	if pmes.GetType() == pb.Message_GET_VALUE || pmes.GetType() == pb.Message_PUT_VALUE {
		requestID = pmes.GetType().String() + string(pmes.GetKey()) + fmt.Sprintf("%d", pmes.GetClusterLevel())
	} else if pmes.GetType() == pb.Message_FIND_NODE {
		requestID = pmes.GetType().String() + fmt.Sprintf("%d", pmes.GetClusterLevel())
	} else if pmes.GetType() == pb.Message_ADD_PROVIDER || pmes.GetType() == pb.Message_GET_PROVIDERS {
		requestID = pmes.GetType().String() + multihash.Multihash(pmes.GetKey()).String() + fmt.Sprintf("%d", pmes.GetClusterLevel())
	} else if pmes.GetType() == pb.Message_PING {
		requestID = pmes.GetType().String() + fmt.Sprintf("%d", pmes.GetClusterLevel())
	}
	return requestID
}

// This function insert a unique ID into the ClusterLevelRaw field of a message (1000<ID<11000).
// ClusterLevelRaw is set by default to 0, but is not used currently by kad-dht.
// This function is called before sending the message to the remote peer.
func (ms *peerMessageSender) SetRequestId(pmes *pb.Message) {
	ms.messageId = (ms.messageId + 1) % 10000
	pmes.SetClusterLevel(ms.messageId + 1000)
}

// This function restores the ClusterLevelRaw field of a message to its original value (0).
// This function is called when the response is received from the remote peer
func (ms *peerMessageSender) RestoreRequestInfo(pmes *pb.Message) {
	pmes.SetClusterLevel(0)
}

// intermediary struct to hold return channel and response
type MessageInfo struct {
	message  *pb.Message
	receiver chan MetaMessage
	err      error
	ctx      context.Context
}

type MetaMessage struct {
	message *pb.Message
	err     error
}

// Launch the InfiniteWriter and the InfiniteReader in two separate goroutines
func (ms *peerMessageSender) runInfReaderAndWriter(ctx context.Context) {
	go ms.runInfiniteWriter(ctx)
	go ms.runInfiniteReader(ctx)
}

// Receives messages and requests through chanrequest and chanmessage channels,
// then write them to the remote peer one at a time (using a Write Lock)
func (ms *peerMessageSender) runInfiniteWriter(ctx context.Context) {
	logger.Debugw("lookup patch", "infinite writer", "started", "for", ms.p.String())

	// Main loop for writing requests and messages and handling write errors
	for {
		time.Sleep(SmallReadWriteInterval) // time space between writes (pacing)
		select {
		// handle messages received from SendMessage().
		// messages don't need response
		case message := <-ms.chanmessage:
			if message.message != nil {
				ms.handleMessageWrite(message)
			}
		// handle requests received from SendRequest()
		case request := <-ms.chanrequest:
			if request.message != nil {
				ms.handleRequestWrite(request)
			}
			// close go routine when remote peer (ms.p) is stopped, based on signal received from OnDisconnect() function
		case <-ms.infiniteRwCtx.Done():
			logger.Debugw("lookup patch", "infinite writer", "stopped", "for", ms.p.String())
			return
		default:
			time.Sleep(SmallReadWriteInterval) // additional 10ms sleep since no activity

		}
	}

}

// Receive responses to requests from a remote peer,
// then deliver them to the right sendRequest calls. This is done by delivering each response via
// its corresponding receive channel.
func (ms *peerMessageSender) runInfiniteReader(ctx context.Context) {
	logger.Debugw("lookup patch", "infinite reader", "started", "for", ms.p.String())

	// Main loop for reading responses and handling delivery via receive channels
	for {
		time.Sleep(SmallReadWriteInterval) // time space between reads (pacing)
		select {
		// close go routine when remote peer (ms.p) is stopped, based on signal received from OnDisconnect() function
		case <-ms.infiniteRwCtx.Done():
			logger.Debugw("lookup patch", "infinite reader", "stopped", "for", ms.p.String())
			return
		default:
			ms.handleRequestRead()
		}

	}

}

// Return request response data or error via a receive channel.
// and delete request ID entry from the Map
func (ms *peerMessageSender) ReturnResponseViaChan(requestID string, rcv chan MetaMessage, data *pb.Message, errc error) {
	defer func() {
		close(rcv) // close channel only on receiver side
		ms.chanMapMutex.Lock()
		defer ms.chanMapMutex.Unlock()
		delete(ms.chanMap, requestID)

	}()
	// close the function after 2 Milliseconds
	// because rcv is not used concurrently, so the write should be instantaneous
	stopTimer := time.NewTimer(2 * time.Millisecond)
	defer stopTimer.Stop()
	select {
	case <-stopTimer.C:
		return
	case rcv <- MetaMessage{message: data, err: errc}:
		return
	}
}

// Return message status (nil or error) via a receive channel.
func (ms *peerMessageSender) ReturnMsgResponseViaChan(rcv chan MetaMessage, data *pb.Message, errc error) {
	// close the function after 2 Milliseconds
	// because rcv is not used concurrently, so the write should be instantaneous
	stopTimer := time.NewTimer(2 * time.Second)
	defer stopTimer.Stop()
	defer close(rcv) // closing the channel only on sender side
	select {
	case <-stopTimer.C:
		return
	case rcv <- MetaMessage{message: data, err: errc}:
		return
	}
}

// Write a message on a stream and return nil if ok or error otherwise.
// Messages don't have responses, so no need to implement a handleMessageRead() function
func (ms *peerMessageSender) handleMessageWrite(metaMessage MessageInfo) {
	ms.writeMutex.Lock()
	defer ms.writeMutex.Unlock()
	// Check if the peer was recently flagged as unresponsive
	if err, unresponsive := ms.IsUnresponsivePeer(); !unresponsive {
		retry := false
		for {
			if err := ms.prep(metaMessage.ctx); err != nil {
				ms.ReturnMsgResponseViaChan(metaMessage.receiver, nil, err)
				break
			} else if err := ms.writeMsg(metaMessage.message); err != nil {
				_ = ms.s.Reset()
				ms.s = nil

				// retry and stream reuse
				if retry {
					logger.Debugw("error writing request", "error", err)
					ms.ReturnMsgResponseViaChan(metaMessage.receiver, nil, err)
					// flag the peer as unresponsive since we observed a write error
					ms.UpdateUnresponsiveMap()
					logger.Debugw("lookup patch", "infinite writer", "error while writing message", "to", ms.p.String(), "error", err)
					break
				}
				logger.Debugw("error writing request", "error", err, "retrying", true)
				retry = true
				continue
			} else {
				// no error
				ms.ReturnMsgResponseViaChan(metaMessage.receiver, nil, nil)
				break
			}
		}
		if ms.singleMes > streamReuseTries {
			// don't close nil stream,
			// a new stream will be created in the next iteration by ms.prep()
			if ms.s != nil {
				err = ms.s.Close()
				if err != nil {
					logger.Debugw("lookup patch", "infinite writer", "error when closing stream", "to", ms.p.String(), "error", err)
				}
				ms.s = nil
			} else {
				logger.Debugw("lookup patch", "infinite writer", "ignoring nil stream", "to", ms.p.String())
			}
		} else if retry {
			ms.singleMes++
		}

	} else { // the peer is still considered unresponsive
		ms.ReturnMsgResponseViaChan(metaMessage.receiver, nil, err)
	}

}

// store request ID and receive channel in a Map, then
// write the request on a stream. Return nil if ok or error otherwise.
func (ms *peerMessageSender) handleRequestWrite(metaMessage MessageInfo) {
	ms.writeMutex.Lock()
	defer ms.writeMutex.Unlock()
	requestID := GetRequestId(metaMessage.message)
	// Check if the peer was recently flagged as unresponsive
	if err, unresponsive := ms.IsUnresponsivePeer(); !unresponsive {
		// store request ID and receive channel in order to deliver the response
		ms.chanMapMutex.Lock()
		ms.chanMap[requestID] = metaMessage.receiver
		ms.chanMapMutex.Unlock()
		retry := false
		for {
			// Allow retry and stream reuse
			if err := ms.prep(metaMessage.ctx); err != nil {
				// Return error and remove request ID and chan from the Map
				ms.ReturnResponseViaChan(requestID, metaMessage.receiver, nil, err)
				break

			} else if err := ms.writeMsg(metaMessage.message); err != nil {
				// allow stream reuse
				_ = ms.s.Reset()
				ms.s = nil
				// retry and stream reuse
				if retry {
					logger.Debugw("error writing request", "error", err)
					// Return error and remove request ID and chan from the Map
					ms.ReturnResponseViaChan(requestID, metaMessage.receiver, nil, err)
					// flag the peer as unresponsive since we observed a write error
					ms.UpdateUnresponsiveMap()
					logger.Debugw("lookup patch", "infinite writer", "error while writing request", "to", ms.p.String(), "error", err)
					break
				}
				logger.Debugw("error writing request", "error", err, "retrying", true)
				retry = true
				continue

			} else {
				// no error
				break
			}
		}
		if ms.singleMes > streamReuseTries {
			// don't close nil stream,
			// a new stream will be created in the next iteration by ms.prep()
			if ms.s != nil {
				err = ms.s.Close()
				if err != nil {
					logger.Debugw("lookup patch", "infinite writer", "error when closing stream", "to", ms.p.String(), "error", err)
				}
				ms.s = nil
			} else {
				logger.Debugw("lookup patch", "infinite writer", "ignoring nil stream", "to", ms.p.String())
			}
		} else if retry {
			ms.singleMes++
		}

	} else { // the peer is still considered unresponsive
		ms.ReturnResponseViaChan(requestID, metaMessage.receiver, nil, err)
	}
}

// Read response from a buffer. Reconstruct request ID and lookup
// the corresponding receive channel from the Map.
// Restore ClusterRaw field and deliver the response via the corresponding receive channel
func (ms *peerMessageSender) handleRequestRead() {
	if l, _ := ms.r.NextMsgLen(); l > 0 { // check if the buffer is not empty
		bytes, err := ms.r.ReadMsg()
		if err != nil {
			ms.r.ReleaseMsg(bytes)
			return
		}
		response := new(pb.Message)
		err = response.Unmarshal(bytes)
		if err != nil {
			ms.r.ReleaseMsg(bytes)
			return
		}
		// Retreive request ID from response
		requestID := GetRequestId(response)
		// Restore the ClusterRaw field to its original value
		ms.RestoreRequestInfo(response)
		// Retreive corresponding rcv channel from the chanMap
		if ok, rcvChan := ms.findReceiveChan(requestID); ok {
			// return response via rcv channel and delete requestID entry
			ms.ReturnResponseViaChan(requestID, rcvChan, response, err)
		}
		ms.r.ReleaseMsg(bytes)

	}

}

func (ms *peerMessageSender) findReceiveChan(id string) (found bool, rcv chan MetaMessage) {
	ms.chanMapMutex.Lock()
	defer ms.chanMapMutex.Unlock()
	rcv, found = ms.chanMap[id]
	return
}
