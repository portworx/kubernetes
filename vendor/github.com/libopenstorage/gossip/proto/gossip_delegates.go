package proto

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/memberlist"

	"github.com/libopenstorage/gossip/proto/state"
	"github.com/libopenstorage/gossip/types"
)

type GossipDelegate struct {
	// GossipstoreImpl implements the GossipStoreInterface
	GossipStoreImpl
	nodeId string
	// last gossip time
	lastGossipTsLock sync.Mutex
	lastGossipTs     time.Time
	history          *GossipHistory
	// channel to receive state change events
	stateEvent chan types.StateEvent
	// current State object
	currentState state.State
	// quorum timeout to change the quorum status of a node
	quorumTimeout      time.Duration
	timeoutVersion     uint64
	timeoutVersionLock sync.Mutex
}

func (gd *GossipDelegate) InitGossipDelegate(
	genNumber uint64,
	selfNodeId types.NodeId,
	gossipVersion string,
	quorumTimeout time.Duration,
) {
	gd.GenNumber = genNumber
	gd.nodeId = string(selfNodeId)
	gd.stateEvent = make(chan types.StateEvent)
	// We start with a NOT_IN_QUORUM status
	gd.InitStore(selfNodeId, gossipVersion, types.NODE_STATUS_NOT_IN_QUORUM)
	gd.quorumTimeout = quorumTimeout
	gd.history = NewGossipHistory(20)
}

func (gd *GossipDelegate) InitCurrentState(clusterSize int) {
	// Our initial state is NOT_IN_QUORUM
	gd.currentState = state.GetNotInQuorum(clusterSize, types.NodeId(gd.nodeId), gd.stateEvent)
	// Start the go routine which handles all the events
	// and changes state of the node
	go gd.handleStateEvents()
}

func (gd *GossipDelegate) updateGossipTs() {
	gd.lastGossipTsLock.Lock()
	defer gd.lastGossipTsLock.Unlock()
	gd.lastGossipTs = time.Now()
}

func (gd *GossipDelegate) convertToBytes(obj interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	if err != nil {
		return []byte{}, err
	}
	return buf.Bytes(), nil
}

func (gd *GossipDelegate) convertFromBytes(buf []byte, msg interface{}) error {
	msgBuffer := bytes.NewBuffer(buf)
	dec := gob.NewDecoder(msgBuffer)
	err := dec.Decode(msg)
	if err != nil {
		return err
	}
	return nil
}

func (gd *GossipDelegate) gossipVersionCheck(node *memberlist.Node) error {
	// Check the gossip version of other node
	var nodeMetaInfo types.NodeMetaInfo
	nodeMeta := node.Meta
	err := gd.convertFromBytes(nodeMeta, &nodeMetaInfo)
	if err != nil {
		err = fmt.Errorf("gossip: Error in unmarshalling peer's meta data. Error : %v", err.Error())
	} else {
		if nodeMetaInfo.GossipVersion != gd.GetGossipVersion() {
			// Version Mismatch
			// We do not add this node in our memberlist
			err = fmt.Errorf("gossip: Gossip Version mismatch with Node (%v):(%v)", node.Name, node.Addr)
		} else {
			// Version Match
			// Add this new node in our node map
			err = nil
		}
	}
	return err
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (gd *GossipDelegate) NodeMeta(limit int) []byte {
	msg := gd.MetaInfo()
	msgBytes, _ := gd.convertToBytes(msg)
	return msgBytes
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed.
// Note: Currently, we do not use broadcasts and hence this function does nothing
func (gd *GossipDelegate) NotifyMsg(data []byte) {
	var nodeId string
	json.Unmarshal(data, &nodeId)
	return
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
// Note: Currently, we do not use broadcasts and hence this function does nothing
func (gd *GossipDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	var test [][]byte
	s1, _ := json.Marshal(gd.nodeId)
	s2, _ := json.Marshal("test_string")
	test = append(test, s1)
	test = append(test, s2)
	return test
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (gd *GossipDelegate) LocalState(join bool) []byte {
	gd.updateSelfTs()

	// We don't know which node we are talking to.
	gs := NewGossipSessionInfo("", types.GD_ME_TO_PEER)
	gs.Op = types.LocalPush

	// We send our local state of nodeMap
	// The receiver will decide which nodes to merge and which to ignore
	localState := gd.GetLocalState()
	byteLocalState, err := gd.convertToBytes(&localState)
	if err != nil {
		gs.Err = fmt.Sprintf("gossip: Error in LocalState. Unable to unmarshal: %v", err.Error())
		logrus.Infof(gs.Err)
		byteLocalState = []byte{}
	}
	gs.Err = ""
	gd.updateGossipTs()
	gd.history.AddLatest(gs)
	return byteLocalState
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (gd *GossipDelegate) MergeRemoteState(buf []byte, join bool) {
	var remoteState types.NodeInfoMap
	if join == true {
		// NotifyJoin will take care of this info
		return
	}
	gd.updateSelfTs()

	gs := NewGossipSessionInfo("", types.GD_PEER_TO_ME)
	err := gd.convertFromBytes(buf, &remoteState)
	if err != nil {
		gs.Err = fmt.Sprintf("gossip: Error in unmarshalling peer's local data. Error : %v", err.Error())
		logrus.Infof(gs.Err)
	}

	gd.Update(remoteState)
	gs.Op = types.MergeRemote
	gs.Err = ""
	gd.updateGossipTs()
	gd.history.AddLatest(gs)
	return
}

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (gd *GossipDelegate) NotifyJoin(node *memberlist.Node) {
	// Ignore self NotifyJoin
	if node.Name == gd.nodeId {
		return
	}

	gs := NewGossipSessionInfo(node.Name, types.GD_PEER_TO_ME)
	gs.Op = types.NotifyJoin
	gd.updateGossipTs()

	// NotifyAlive should remove a node from memberlist if the
	// gossip version mismatches.
	// Nevertheless we are doing an extra check here.
	err := gd.gossipVersionCheck(node)
	if err != nil {
		gs.Err = err.Error()
		logrus.Infof(gs.Err)
	} else {
		gd.AddNode(types.NodeId(types.NodeId(node.Name)), types.NODE_STATUS_UP)
		gd.triggerStateEvent(types.NODE_ALIVE)
		gs.Err = ""
	}

	gd.history.AddLatest(gs)
}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (gd *GossipDelegate) NotifyLeave(node *memberlist.Node) {
	if node.Name == gd.nodeId {
		gd.triggerStateEvent(types.SELF_LEAVE)
	} else {
		err := gd.UpdateNodeStatus(types.NodeId(node.Name), types.NODE_STATUS_DOWN)
		if err != nil {
			logrus.Infof("gossip: Could not update status on NotifyLeave : %v", err.Error())
			return
		}
		gd.triggerStateEvent(types.NODE_LEAVE)
	}

	gs := NewGossipSessionInfo(node.Name, types.GD_PEER_TO_ME)
	gs.Err = ""
	gs.Op = types.NotifyLeave
	gd.updateGossipTs()
	gd.history.AddLatest(gs)
	return
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
// Note: Currently we do not use memberlists Node meta or modify it.
// Probably future use ?
func (gd *GossipDelegate) NotifyUpdate(node *memberlist.Node) {
	logrus.Infof("gossip: Update Notification from %v %v", node.Name, node.Addr)
}

// AliveDelegate is used to involve a client in processing a node "alive" message.
// TODO/Future-use : Check if we want to add this node in memberlist
func (gd *GossipDelegate) NotifyAlive(node *memberlist.Node) error {
	if node.Name == gd.nodeId {
		gd.triggerStateEvent(types.SELF_ALIVE)
		return nil
	}

	gs := NewGossipSessionInfo(node.Name, types.GD_PEER_TO_ME)
	gs.Op = types.NotifyAlive
	gs.Err = ""
	gd.updateGossipTs()

	diffNode, err := gd.GetLocalNodeInfo(types.NodeId(node.Name))
	if err != nil {
		// We found a new node!!
		// Check if gossip version matches
		err := gd.gossipVersionCheck(node)
		if err != nil {
			gs.Err = err.Error()
			logrus.Infof(gs.Err)
			gd.history.AddLatest(gs)
			// Do not add this node to the memberlist.
			// Returning a non-nil err value
			return err
		} else {
			gd.AddNode(types.NodeId(node.Name), types.NODE_STATUS_UP)
			gd.triggerStateEvent(types.NODE_ALIVE)
		}
	} else {
		if diffNode.Status != types.NODE_STATUS_UP {
			gd.UpdateNodeStatus(types.NodeId(node.Name), types.NODE_STATUS_UP)
			gd.triggerStateEvent(types.NODE_ALIVE)
		}
	}
	gd.history.AddLatest(gs)
	return nil
}

func (gd *GossipDelegate) triggerStateEvent(event types.StateEvent) {
	gd.stateEvent <- event
	return
}

func (gd *GossipDelegate) startQuorumTimer() {
	gd.timeoutVersionLock.Lock()
	localVersion := gd.timeoutVersion + 1
	gd.timeoutVersion = localVersion
	gd.timeoutVersionLock.Unlock()

	logrus.Infof("gossip: Starting Quorum Timer with version v%v. Waiting for quorum timeout of (%v)", localVersion, gd.quorumTimeout)
	time.Sleep(gd.quorumTimeout)

	gd.timeoutVersionLock.Lock()
	if localVersion == gd.timeoutVersion {
		gd.timeoutVersionLock.Unlock()
		gd.stateEvent <- types.TIMEOUT
		return
	} // else do not send an event. Another timer started
	gd.timeoutVersionLock.Unlock()
}

func (gd *GossipDelegate) handleStateEvents() {
	for {
		// We block here until we get an event
		event := <-gd.stateEvent
		previousStatus := gd.currentState.NodeStatus()
		switch event {
		case types.SELF_ALIVE:
			gd.currentState, _ = gd.currentState.SelfAlive(gd.GetLocalState())
		case types.NODE_ALIVE:
			gd.currentState, _ = gd.currentState.NodeAlive(gd.GetLocalState())
		case types.SELF_LEAVE:
			gd.currentState, _ = gd.currentState.SelfLeave()
		case types.NODE_LEAVE:
			gd.currentState, _ = gd.currentState.NodeLeave(gd.GetLocalState())
		case types.UPDATE_CLUSTER_SIZE:
			gd.currentState, _ = gd.currentState.UpdateClusterSize(gd.getClusterSize(), gd.GetLocalState())
		case types.TIMEOUT:
			newState, _ := gd.currentState.Timeout(gd.getClusterSize(), gd.GetLocalState())
			if newState.NodeStatus() != gd.currentState.NodeStatus() {
				logrus.Infof("gossip: Quorum Timeout. Waited for (%v)", gd.quorumTimeout)
			}
			gd.currentState = newState
		}
		newStatus := gd.currentState.NodeStatus()
		if previousStatus == types.NODE_STATUS_UP && newStatus == types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
			// Start a timer
			go gd.startQuorumTimer()
		}
		gd.UpdateSelfStatus(gd.currentState.NodeStatus())
	}
}
