package apiservice

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/tenant"
)

func RegisterTenantRoutes(router *gin.Engine, apiInfo *APIInfo, manager *tenant.Manager) {
	if manager == nil {
		return
	}

	router.GET("/api/v1/info", func(c *gin.Context) {
		info := apiInfo.Snapshot()
		c.JSON(http.StatusOK, ProcessInfoResponse{
			Status:            info.Status,
			ErrorMessage:      info.ErrorMessage,
			LoadedTenantCount: manager.LoadedTenantCount(),
			SchedulerStatus:   "ready",
		})
	})

	router.GET("/api/v1/tenants", func(c *gin.Context) {
		c.JSON(http.StatusOK, manager.ListTenants())
	})

	router.GET("/api/v1/tenants/:tenant_id", func(c *gin.Context) {
		status, err := manager.GetTenant(c.Param("tenant_id"))
		if err != nil {
			writeAPIError(c, http.StatusNotFound, err)
			return
		}
		c.JSON(http.StatusOK, status)
	})

	router.POST("/api/v1/tenants/:tenant_id/tasks", func(c *gin.Context) {
		var req model.CreateTaskRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeAPIError(c, http.StatusBadRequest, err)
			return
		}
		manifest, err := manager.CreateTask(c.Request.Context(), c.Param("tenant_id"), req)
		if err != nil {
			writeAPIError(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusCreated, manifest)
	})

	router.GET("/api/v1/tenants/:tenant_id/tasks", func(c *gin.Context) {
		manifests, err := manager.ListTasks(c.Request.Context(), c.Param("tenant_id"))
		if err != nil {
			writeAPIError(c, http.StatusNotFound, err)
			return
		}
		c.JSON(http.StatusOK, manifests)
	})

	router.GET("/api/v1/tenants/:tenant_id/tasks/:task_id", func(c *gin.Context) {
		manifest, err := manager.GetTask(c.Request.Context(), c.Param("tenant_id"), c.Param("task_id"))
		if err != nil {
			writeAPIError(c, http.StatusNotFound, err)
			return
		}
		c.JSON(http.StatusOK, manifest)
	})

	router.POST("/api/v1/tenants/:tenant_id/tasks/:task_action", func(c *gin.Context) {
		taskAction := c.Param("task_action")
		switch {
		case strings.HasSuffix(taskAction, ":pause"):
			taskID := strings.TrimSuffix(taskAction, ":pause")
			manifest, err := manager.PauseTask(c.Request.Context(), c.Param("tenant_id"), taskID)
			if err != nil {
				writeAPIError(c, http.StatusBadRequest, err)
				return
			}
			c.JSON(http.StatusOK, manifest)
		case strings.HasSuffix(taskAction, ":resume"):
			taskID := strings.TrimSuffix(taskAction, ":resume")
			manifest, err := manager.ResumeTask(c.Request.Context(), c.Param("tenant_id"), taskID)
			if err != nil {
				writeAPIError(c, http.StatusBadRequest, err)
				return
			}
			c.JSON(http.StatusOK, manifest)
		default:
			writeAPIError(c, http.StatusNotFound, errUnknownTaskAction(taskAction))
		}
	})

	router.DELETE("/api/v1/tenants/:tenant_id/tasks/:task_id", func(c *gin.Context) {
		if err := manager.TombstoneTask(c.Request.Context(), c.Param("tenant_id"), c.Param("task_id")); err != nil {
			writeAPIError(c, http.StatusBadRequest, err)
			return
		}
		c.Status(http.StatusNoContent)
	})
}

func writeAPIError(c *gin.Context, status int, err error) {
	c.JSON(status, gin.H{"error": err.Error()})
}

type errUnknownTaskAction string

func (e errUnknownTaskAction) Error() string {
	return "unknown task action " + string(e)
}
