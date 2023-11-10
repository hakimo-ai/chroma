package grpccoordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/coordinator"
	"github.com/chroma/chroma-coordinator/internal/grpccoordinator/grpcutils"
	"github.com/chroma/chroma-coordinator/internal/memberlist_manager"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"github.com/chroma/chroma-coordinator/internal/utils"
	"github.com/pingcap/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"gorm.io/gorm"
)

type Config struct {
	// GRPC config
	BindAddress string

	// MetaTable config
	Username     string
	Password     string
	Address      string
	DBName       string
	MaxIdleConns int
	MaxOpenConns int

	// Config for testing
	Testing bool
}

// Server wraps Coordinator with GRPC services.
//
// When Testing is set to true, the GRPC services will not be intialzed. This is
// convenient for end-to-end property based testing.
type Server struct {
	coordinatorpb.UnimplementedSysDBServer
	coordinator  coordinator.ICoordinator
	grpcServer   grpcutils.GrpcServer
	healthServer *health.Server
}

func New(config Config) (*Server, error) {
	// dBConfig := dbcore.DBConfig{
	// 	Username:     config.Username,
	// 	Password:     config.Password,
	// 	Address:      config.Address,
	// 	DBName:       config.DBName,
	// 	MaxIdleConns: config.MaxIdleConns,
	// 	MaxOpenConns: config.MaxOpenConns,
	// }
	// db, err := dbcore.Connect(dBConfig)
	// if err != nil {
	// 	return nil, err
	// }
	return NewWithGrpcProvider(config, grpcutils.Default, nil)
}

func NewWithGrpcProvider(config Config, provider grpcutils.GrpcProvider, db *gorm.DB) (*Server, error) {
	ctx := context.Background()
	s := &Server{
		healthServer: health.NewServer(),
	}
	// assignmentPolicy := coordinator.NewSimpleAssignmentPolicy("test-tenant", "test-topic")
	// TODO: make this configuration, and make the pulsar tenant configuration too
	assignmentPolicy := coordinator.NewRendezvousAssignmentPolicy("default", "default")
	coordinator, err := coordinator.NewCoordinator(ctx, assignmentPolicy, db)
	if err != nil {
		return nil, err
	}
	s.coordinator = coordinator
	s.coordinator.Start()
	if !config.Testing {
		// TODO: Make this configuration
		log.Info("Starting memberlist manager")
		memberlist_name := "worker-memberlist"
		namespace := "chroma"
		clientset, err := utils.GetKubernetesInterface()
		if err != nil {
			return nil, err
		}
		dynamicClient, err := utils.GetKubernetesDynamicInterface()
		if err != nil {
			return nil, err
		}
		nodeWatcher := memberlist_manager.NewKubernetesWatcher(clientset, namespace, "worker")
		memberlistStore := memberlist_manager.NewCRMemberlistStore(dynamicClient, namespace, memberlist_name)
		memberlist_manager := memberlist_manager.NewMemberlistManager(nodeWatcher, memberlistStore)

		// Start the memberlist manager
		err = memberlist_manager.Start()
		if err != nil {
			return nil, err
		}

		s.grpcServer, err = provider.StartGrpcServer("coordinator", config.BindAddress, func(registrar grpc.ServiceRegistrar) {
			coordinatorpb.RegisterSysDBServer(registrar, s)
		})
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) Close() error {
	s.healthServer.Shutdown()
	return nil
}
