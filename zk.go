package cluster

import (
	"encoding/json"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// ZK wraps a zookeeper connection
type ZK struct {
	conn       *zk.Conn
	chrootPath string
}

type zkConsumerConfig struct {
	Pattern      string         `json:"pattern,omitempty"`
	Subscription map[string]int `json:"subscription,omitempty"`
	Timestamp    string         `json:"timestamp,omitempty"`
	Version      int            `json:"version,omitempty"`
}

// NewZK creates a new connection instance
func NewZK(servers []string, chrootPath string, recvTimeout time.Duration) (*ZK, error) {
	conn, _, err := zk.Connect(servers, recvTimeout)
	if err != nil {
		return nil, err
	}
	return &ZK{conn: conn, chrootPath: chrootPath}, nil
}

/*******************************************************************
 * HIGH LEVEL API
 *******************************************************************/

// Consumers returns all active consumers within a group
func (z *ZK) Consumers(group string) ([]string, <-chan zk.Event, error) {
	root := z.chrootPath + "/consumers/" + group + "/ids"
	err := z.MkdirAll(root)
	if err != nil {
		return nil, nil, err
	}

	strs, _, ch, err := z.conn.ChildrenW(root)
	if err != nil {
		return nil, nil, err
	}
	sort.Strings(strs)
	return strs, ch, nil
}

// Claim claims a topic/partition ownership for a consumer ID within a group
func (z *ZK) Claim(group, topic string, partitionID int32, id string) (err error) {
	root := z.chrootPath + "/consumers/" + group + "/owners/" + topic
	if err = z.MkdirAll(root); err != nil {
		return err
	}

	node := root + "/" + strconv.Itoa(int(partitionID))
	tries := 0
	for {
		err := z.Create(node, []byte(id), true)
		if err == nil {
			break
		} else if err == zk.ErrNodeExists {
			if tries++; tries > 20 {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		} else {
			return err
		}
	}
	return nil
}

// Release releases a claim
func (z *ZK) Release(group, topic string, partitionID int32, id string) error {
	node := z.chrootPath + "/consumers/" + group + "/owners/" + topic + "/" + strconv.Itoa(int(partitionID))
	val, _, err := z.conn.Get(node)

	// Already deleted
	if err == zk.ErrNoNode {
		return nil
	}

	// Locked by someone else?
	if string(val) != id {
		return zk.ErrNotLocked
	}

	return z.DeleteAll(node)
}

// Commit commits an offset to a group/topic/partition
func (z *ZK) Commit(group, topic string, partitionID int32, offset int64) (err error) {
	root := z.chrootPath + "/consumers/" + group + "/offsets/" + topic
	if err = z.MkdirAll(root); err != nil {
		return err
	}

	node := root + "/" + strconv.Itoa(int(partitionID))
	data := []byte(strconv.FormatInt(offset, 10))
	_, stat, err := z.conn.Get(node)

	// Try to create new node
	if err == zk.ErrNoNode {
		return z.Create(node, data, false)
	} else if err != nil {
		return err
	}

	_, err = z.conn.Set(node, data, stat.Version)
	return
}

// Offset retrieves an offset to a group/topic/partition
func (z *ZK) Offset(group, topic string, partitionID int32) (int64, error) {
	node := z.chrootPath + "/consumers/" + group + "/offsets/" + topic + "/" + strconv.Itoa(int(partitionID))
	val, _, err := z.conn.Get(node)
	if err == zk.ErrNoNode {
		return 0, nil
	} else if err != nil {
		return -1, err
	}
	return strconv.ParseInt(string(val), 10, 64)
}

// RegisterGroup creates/updates a group directory
func (z *ZK) RegisterGroup(group string) error {
	return z.MkdirAll(z.chrootPath + "/consumers/" + group + "/ids")
}

// RegisterConsumer registers a new consumer within a group
func (z *ZK) RegisterConsumer(group, id, topic string) error {
	data, err := json.Marshal(&zkConsumerConfig{
		Pattern:      "white_list",
		Subscription: map[string]int{topic: 1},
		Timestamp:    strconv.FormatInt(time.Now().Unix(), 10),
		Version:      1,
	})
	if err != nil {
		return err
	}

	return z.Create(z.chrootPath+"/consumers/"+group+"/ids/"+id, data, true)
}

// DeleteConsumer deletes the consumer from registry
func (z *ZK) DeleteConsumer(group, id string) error {
	return z.conn.Delete(z.chrootPath+"/consumers/"+group+"/ids/"+id, 0)
}

/*******************************************************************
 * LOW LEVEL API
 *******************************************************************/

// Exists checks existence of a node
func (z *ZK) Exists(node string) (ok bool, err error) {
	ok, _, err = z.conn.Exists(node)
	return
}

// DeleteAll deletes a node recursively
func (z *ZK) DeleteAll(node string) (err error) {
	children, stat, err := z.conn.Children(node)
	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		return
	}

	for _, child := range children {
		if err = z.DeleteAll(path.Join(node, child)); err != nil {
			return
		}
	}

	return z.conn.Delete(node, stat.Version)
}

// MkdirAll creates a directory recursively
func (z *ZK) MkdirAll(node string) (err error) {
	parent := path.Dir(node)
	if parent != "/" {
		if err = z.MkdirAll(parent); err != nil {
			return
		}
	}

	_, err = z.conn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		err = nil
	}
	return
}

// Create stores a new value at node. Fails if already set.
func (z *ZK) Create(node string, value []byte, ephemeral bool) (err error) {
	if err = z.MkdirAll(path.Dir(node)); err != nil {
		return
	}

	flags := int32(0)
	if ephemeral {
		flags = zk.FlagEphemeral
	}
	_, err = z.conn.Create(node, value, flags, zk.WorldACL(zk.PermAll))
	return
}

func (z *ZK) Close() {
	z.conn.Close()
}
