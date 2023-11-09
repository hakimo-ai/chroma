package memberlist_manager

import (
	"fmt"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"k8s.io/client-go/util/workqueue"
)

// TODO:
/*
- Switch to camel case for all variables and methods
*/

// A memberlist manager is responsible for managing the memberlist for a
// coordinator. A memberlist is a CR in the coordinator's namespace that
// contains a list of all the members in the coordinator's cluster.
// The memberlist uses k8s watch to monitor changes to pods and then updates a CR

type IMemberlistManager interface {
	Start() error
	Stop() error
}

type MemberlistManager struct {
	workqueue       workqueue.RateLimitingInterface // workqueue for the coordinator
	nodeWatcher     IWatcher                        // node watcher for the coordinator
	memberlistStore IMemberlistStore                // memberlist store for the coordinator
}

func NewMemberlistManager(nodeWatcher IWatcher, memberlistStore IMemberlistStore) *MemberlistManager {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	return &MemberlistManager{
		workqueue:       queue,
		nodeWatcher:     nodeWatcher,
		memberlistStore: memberlistStore,
	}
}

func (m *MemberlistManager) Start() error {
	m.nodeWatcher.RegisterCallback(func(node_ip string) {
		m.workqueue.Add(node_ip)
	})
	err := m.nodeWatcher.Start()
	if err != nil {
		return err
	}
	go m.run()
	return nil
}

func (m *MemberlistManager) run() {
	for {
		key, shutdown := m.workqueue.Get()
		if shutdown {
			fmt.Println("Shutting down memberlist manager")
			break
		}
		nodeUpdate, err := m.nodeWatcher.GetStatus(key.(string))
		if err != nil {
			m.workqueue.Done(key)
			continue
		}
		err = m.reconcile(key.(string), nodeUpdate)
		if err != nil {
			// TODO: retry
			log.Error("Error reconciling", zap.Error(err))
		}
		m.workqueue.Done(key)
	}
}

func (m *MemberlistManager) reconcile(node_ip string, status Status) error {
	memberlist, resourceVersion, err := m.memberlistStore.GetMemberlist()
	if err != nil {
		return err
	}
	exists := false
	new_memberlist := Memberlist{}
	for _, node := range *memberlist {
		if node == node_ip {
			if status == Ready {
				new_memberlist = append(new_memberlist, node)
			}
			exists = true
		} else {
			// This update doesn't pertains to this node, so we just add it to the new memberlist
			new_memberlist = append(new_memberlist, node)
		}
	}
	if !exists && status == Ready {
		new_memberlist = append(new_memberlist, node_ip)
	}
	return m.memberlistStore.UpdateMemberlist(&new_memberlist, resourceVersion)
}

func (m *MemberlistManager) Stop() error {
	m.workqueue.ShutDown()
	return nil
}
