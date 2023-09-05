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

var dhtReadMessageTimeout = 5 * time.Second   //@Hive: Setting the timeout to 5s
var dhtMessageSenderTimeout = 1 * time.Second //@Hive: Adding timeout when creating Sender
var dhtDefaultBackoffTime = 300 * time.Second //@Hive: 5mn Backoff time for unresponsive peers

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

func NewMessageSenderImpl(h host.Host, protos []protocol.ID) pb.MessageSender {
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

	// Do this asynchronously as ms.lk can block for a while.
	go func() {
		if err := ms.lk.Lock(ctx); err != nil {
			return
		}
		defer ms.lk.Unlock()
		ms.closeSend <- struct{}{}
		ms.invalidate()
	}()
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

type peerInitInfo struct {
	sender *peerMessageSender
	errs   error
}

// SendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (m *messageSenderImpl) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))
	pschan := make(chan peerInitInfo)
	ms := new(peerMessageSender)
	go func(psc chan peerInitInfo) {
		ms, err := m.messageSenderForPeer(ctx, p)
		psc <- peerInitInfo{sender: ms, errs: err}
	}(pschan)
	t := time.NewTimer(dhtMessageSenderTimeout)
	defer t.Stop()
	select {
	case sinfo := <-pschan:
		if sinfo.errs != nil {
			stats.Record(ctx,
				metrics.SentRequests.M(1),
				metrics.SentRequestErrors.M(1),
			)
			logger.Debugw("request failed to open message sender", "error", sinfo.errs, "to", p)
			return nil, sinfo.errs
		} else {
			ms = sinfo.sender
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
	case <-t.C:
		return nil, fmt.Errorf("timed out openning message sender to: %s", p.String())

	}

}

// SendMessage sends out a message
func (m *messageSenderImpl) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))

	pschan := make(chan peerInitInfo)
	ms := new(peerMessageSender)
	go func(psc chan peerInitInfo) {
		ms, err := m.messageSenderForPeer(ctx, p)
		psc <- peerInitInfo{sender: ms, errs: err}
	}(pschan)
	t := time.NewTimer(dhtMessageSenderTimeout)
	defer t.Stop()
	select {
	case sinfo := <-pschan:
		if sinfo.errs != nil {
			stats.Record(ctx,
				metrics.SentRequests.M(1),
				metrics.SentRequestErrors.M(1),
			)
			logger.Debugw("request failed to open message sender", "error", sinfo.errs, "to", p)
			return sinfo.errs
		} else {
			ms = sinfo.sender
			start := time.Now()

			err := ms.SendMessage(ctx, pmes)
			if err != nil {
				stats.Record(ctx,
					metrics.SentRequests.M(1),
					metrics.SentRequestErrors.M(1),
				)
				logger.Debugw("request failed", "error", err, "to", p)

				return err
			}

			stats.Record(ctx,
				metrics.SentRequests.M(1),
				metrics.SentBytes.M(int64(pmes.Size())),
				metrics.OutboundRequestLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
			)
			m.host.Peerstore().RecordLatency(p, time.Since(start))
			return nil
		}
	case <-t.C:
		return fmt.Errorf("timed out openning message sender to: %s", p.String())

	}

}

func (m *messageSenderImpl) messageSenderForPeer(ctx context.Context, p peer.ID) (*peerMessageSender, error) {
	m.smlk.Lock()
	ms, ok := m.strmap[p]
	if ok {
		m.smlk.Unlock()
		return ms, nil
	}
	ms = &peerMessageSender{p: p, m: m, lk: internal.NewCtxMutex(), isResponsive: true, addTime: time.Now(),
		backoffTime: dhtDefaultBackoffTime, closeSend: make(chan struct{}), chanMap: make(map[string]chan MultiMessageResponse),
		chanrequest: make(chan MessageInfo), chanmessage: make(chan MessageInfo), explicitStop: make(chan struct{})}

	m.strmap[p] = ms
	m.smlk.Unlock()

	if err := ms.prepOrInvalidate(ctx); err != nil {
		m.smlk.Lock()
		defer m.smlk.Unlock()

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
	ctx2 := context.Background()
	go ms.InfiniteReader(ctx2)
	return ms, nil
}

// peerMessageSender is responsible for sending requests and messages to a particular peer
type peerMessageSender struct {
	s  network.Stream
	r  msgio.ReadCloser
	lk internal.CtxMutex
	p  peer.ID
	m  *messageSenderImpl

	invalid      bool
	singleMes    int
	addTime      time.Time
	backoffTime  time.Duration
	isResponsive bool
	/*Hive addons*/
	mu           sync.Mutex
	mu2          sync.Mutex
	chanMap      map[string]chan MultiMessageResponse
	closeSend    chan struct{}
	explicitStop chan struct{}
	messageId    int
	chanrequest  chan MessageInfo
	chanmessage  chan MessageInfo
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

// streamReuseTries is the number of times we will try to reuse a stream to a
// given peer before giving up and reverting to the old one-message-per-stream
// behaviour.
const streamReuseTries = 3 //instead of 3

func (ms *peerMessageSender) SendMessage(ctx context.Context, pmes *pb.Message) error {

	ms.mu.Lock()
	if err, unresponsive := ms.IsUnresponsivePeer(); unresponsive {

		logger.Debugw("lookup patch", "error", err, "to", ms.p, "message type", pmes.GetType().String())
		ms.mu.Unlock()
		return err
	}
	ms.mu.Unlock()
	rcv := make(chan MultiMessageResponse)
	messageWithInfo := MessageInfo{
		message:  pmes,
		err:      nil,
		receiver: rcv,
		ctx:      ctx,
	}
	ms.chanmessage <- messageWithInfo
	select {
	case ret := <-rcv:
		if ret.err != nil {
			ms.mu.Lock()
			ms.UpdateUnresponsiveMap()
			ms.mu.Unlock()
		}
		return ret.err
	}

}
func (ms *peerMessageSender) UpdateUnresponsiveMap() {
	if !ms.isResponsive {
		if time.Since(ms.addTime).Seconds() >= ms.backoffTime.Seconds() {
			ms.addTime = time.Now()
			ms.backoffTime *= 2
			logger.Debugw("lookup patch", "exponential backoff for peer", ms.p, "new backoff (s)", ms.backoffTime.Seconds())
			//logger.Infow("lookup patch", "new backoff time(s)", ms.backoffTime.Seconds())
		}
	} else {
		logger.Debugw("lookup patch", "adding new unresponsive peer", ms.p)
		ms.addTime = time.Now()
		ms.backoffTime = dhtDefaultBackoffTime
		ms.isResponsive = false
	}
}

func (ms *peerMessageSender) SendEarlyErrorToAll(err error) {
	ms.mu.Lock()
	for msid, rchan := range ms.chanMap {
		go func(rcv chan MultiMessageResponse, err error) {
			rcv <- MultiMessageResponse{message: nil, err: err}

		}(rchan, err)
		delete(ms.chanMap, msid)
	}
	ms.mu.Unlock()
}

func (ms *peerMessageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	ms.mu.Lock()
	if err, unresponsive := ms.IsUnresponsivePeer(); unresponsive {
		logger.Debugw("lookup patch", "error", err, "to", ms.p, "request type", pmes.GetType().String())
		ms.mu.Unlock()
		return nil, err
	}
	ms.mu.Unlock()
	rcv := make(chan MultiMessageResponse)
	messageWithInfo := MessageInfo{
		message:  pmes,
		err:      nil,
		receiver: rcv,
		ctx:      ctx,
	}
	go func() {
		ms.chanrequest <- messageWithInfo //:
	}()
	t := time.NewTimer(dhtReadMessageTimeout)
	defer t.Stop()
	select {
	case ret := <-rcv:
		if ret.err != nil {
			ms.mu.Lock()
			ms.UpdateUnresponsiveMap()
			ms.mu.Unlock()
		}
		return ret.message, ret.err

	case <-t.C:
		go func() {
			ms.explicitStop <- struct{}{}

		}()
		ms.mu.Lock()
		ms.UpdateUnresponsiveMap()
		ms.mu.Unlock()
		return nil, ErrReadTimeout
	}
}

func (ms *peerMessageSender) writeMsg(pmes *pb.Message) error {
	return WriteMsg(ms.s, pmes)
}

func (ms *peerMessageSender) ctxReadMsg(ctx context.Context, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r msgio.ReadCloser) {
		defer close(errc)
		bytes, err := r.ReadMsg()
		defer r.ReleaseMsg(bytes)
		if err != nil {
			errc <- err
			return
		}
		errc <- mes.Unmarshal(bytes)
	}(ms.r)

	t := time.NewTimer(dhtReadMessageTimeout)
	defer t.Stop()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}
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

func (w *bufferedDelimitedWriter) Flush() error {
	return w.Writer.Flush()
}

/*** HIVE PATCH *****/

func GenerateRequestId(pmes *pb.Message) string {
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

func (ms *peerMessageSender) SetRequestId(pmes *pb.Message) {
	ms.messageId = (ms.messageId + 1) % 1000000
	pmes.SetClusterLevel(ms.messageId + 1000)
}

func (ms *peerMessageSender) RestoreRequestInfo(pmes *pb.Message) {
	pmes.SetClusterLevel(0)
}

// intermediary struct to hold return channel and response
type MessageInfo struct {
	message  *pb.Message
	receiver chan MultiMessageResponse
	err      error
	format   string
	ctx      context.Context
}

type MultiMessageResponse struct {
	message *pb.Message
	err     error
}

func (ms *peerMessageSender) InfiniteReader(ctx context.Context) {
	logger.Debugw("lookup patch", "infinite writer", "started", "for", ms.p.String())
	sendNotif := make(chan struct{})
	readNotif := make(chan struct{})
	go func() {
		logger.Debugw("lookup patch", "infinite reader", "started", "for", ms.p.String())

		for {
			select {
			case <-sendNotif:
				logger.Debugw("lookup patch", "infinite reader", "stopped", "for", ms.p.String())
				ms.chanMap = nil
				ms.chanrequest = nil
				ms.chanmessage = nil
				ms.closeSend = nil
				return
			case <-readNotif:
				if l, _ := ms.r.NextMsgLen(); l > 0 {
					bytes, err := ms.r.ReadMsg()
					if err != nil {
						ms.r.ReleaseMsg(bytes)
						continue
					}
					mes := new(pb.Message)
					err = mes.Unmarshal(bytes)
					if err != nil {
						ms.r.ReleaseMsg(bytes)
						mes = nil
						continue
					}
					msid := GenerateRequestId(mes)
					ms.RestoreRequestInfo(mes)
					msgchan := make(chan MultiMessageResponse)
					ok := false
					ms.mu2.Lock()

					if msgchan, ok = ms.chanMap[msid]; ok {
						delete(ms.chanMap, msid)
					}
					ms.mu2.Unlock()
					if ok {
						go func(rchan chan MultiMessageResponse, msg *pb.Message) {
							defer close(rchan)
							rchan <- MultiMessageResponse{message: msg, err: nil}
						}(msgchan, mes)

					}
					ms.r.ReleaseMsg(bytes)

				}
			}

		}

	}()
	for {
		select {
		case reqinfo := <-ms.chanrequest:
			select {
			case <-ms.explicitStop:
				ms.mu.Lock()
				go func(rcv chan MultiMessageResponse, errc error) {
					defer close(rcv)
					rcv <- MultiMessageResponse{message: nil, err: errc}
				}(reqinfo.receiver, ErrEearlyReadTimeout)
				for msid, rch := range ms.chanMap {
					go func(rcv chan MultiMessageResponse, errc error) {
						defer close(rcv)
						rcv <- MultiMessageResponse{message: nil, err: errc}
					}(rch, ErrEearlyReadTimeout)
					ms.mu2.Lock()
					delete(ms.chanMap, msid)
					ms.mu2.Unlock()
				}
				ms.UpdateUnresponsiveMap()
				ms.mu.Unlock()

			default:
				ms.mu.Lock()
				if err, unresponsive := ms.IsUnresponsivePeer(); !unresponsive {
					ms.SetRequestId(reqinfo.message)
					msid := GenerateRequestId(reqinfo.message)
					ms.mu2.Lock()
					ms.chanMap[msid] = reqinfo.receiver
					ms.mu2.Unlock()
					if err := ms.prep(ctx); err != nil {
						go func(rcv chan MultiMessageResponse, errc error) {
							defer close(rcv)
							rcv <- MultiMessageResponse{message: nil, err: errc}
						}(reqinfo.receiver, err)
						ms.mu2.Lock()
						delete(ms.chanMap, msid)
						ms.mu2.Unlock()
					} else if err := ms.writeMsg(reqinfo.message); err != nil {
						_ = ms.s.Reset()
						ms.s = nil
						go func(rcv chan MultiMessageResponse, errc error) {
							defer close(rcv)
							rcv <- MultiMessageResponse{message: nil, err: errc}
						}(reqinfo.receiver, err)
						ms.UpdateUnresponsiveMap()
						logger.Debugw("lookup patch", "infinite writer", "error while writing request", "to", ms.p.String(), "error", err)
						ms.mu2.Lock()
						delete(ms.chanMap, msid)
						ms.mu2.Unlock()
					} else {
						go func() {
							readNotif <- struct{}{}

						}()
						// nothing for now
					}

				} else {
					go func(rcv chan MultiMessageResponse, errc error) {
						rcv <- MultiMessageResponse{message: nil, err: errc}
					}(reqinfo.receiver, err)
				}
				ms.mu.Unlock()
			}

		case minfo := <-ms.chanmessage:
			ms.mu.Lock()
			if err, unresponsive := ms.IsUnresponsivePeer(); !unresponsive {
				if err := ms.prep(minfo.ctx); err != nil {
					go func(rcv chan MultiMessageResponse, errc error) {
						rcv <- MultiMessageResponse{message: nil, err: errc}
					}(minfo.receiver, err)
				} else if err := ms.writeMsg(minfo.message); err != nil {
					_ = ms.s.Reset()
					ms.s = nil
					go func(rcv chan MultiMessageResponse, errc error) {
						rcv <- MultiMessageResponse{message: nil, err: errc}
					}(minfo.receiver, err)
					ms.UpdateUnresponsiveMap()
					logger.Debugw("lookup patch", "infinite writer", "error while writing message", "to", ms.p.String(), "error", err)
				} else {
					go func(rcv chan MultiMessageResponse, errc error) {
						rcv <- MultiMessageResponse{message: nil, err: errc}
					}(minfo.receiver, nil)
				}

			} else {
				go func(rcv chan MultiMessageResponse, errc error) {
					rcv <- MultiMessageResponse{message: nil, err: errc}
				}(minfo.receiver, err)
			}
			ms.mu.Unlock()
		case <-ms.closeSend:
			logger.Debugw("lookup patch", "infinite writer", "stopped", "for", ms.p.String())
			go func() {
				sendNotif <- struct{}{}
			}()
			return

		}
	}
}
