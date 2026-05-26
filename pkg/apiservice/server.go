package apiservice

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/pingcap-inc/tidb2dw/pkg/tenant"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type APIService struct {
	APIInfo *APIInfo
	router  *gin.Engine
}

func New() *APIService {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Recovery())

	apiInfo := NewAPIInfo()
	apiInfo.registerRouter(r)

	RegisterMetric(r)

	return &APIService{
		APIInfo: apiInfo,
		router:  r,
	}
}

func (service *APIService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	service.router.ServeHTTP(w, r)
}

func (service *APIService) RegisterTenantManager(manager *tenant.Manager) {
	RegisterTenantRoutes(service.router, service.APIInfo, manager)
}

// RegisterMetric registers the metric handler.
func RegisterMetric(router *gin.Engine) {
	router.GET("/metrics", func(c *gin.Context) {
		promhttp.Handler().ServeHTTP(c.Writer, c.Request)
	})
}

func (service *APIService) Serve(l net.Listener) {
	go func() {
		if err := service.router.RunListener(l); err != nil {
			log.Panic("Serve failed", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	s := <-quit
	log.Info("Received exit signal, shutting down API service ...", zap.String("signal", s.String()))

	_ = l.Close()
}
