package router

import (
	"github.com/Tencent/WeKnora/internal/handler"
	"github.com/gin-gonic/gin"
)

func RegisterPluginRoutes(r *gin.RouterGroup, h *handler.PluginHandler, g *rbacGuards) {
	if h == nil {
		return
	} // old router tests can omit the optional handler
	// JWT-only: API-key routes remain default-deny. Tenant/KB ownership is
	// independently checked by the handlers, using the existing datasource rules.
	p := r.Group("/plugins")
	p.GET("", g.Viewer(), h.List)
	p.POST("/builtins/:plugin_id/:action", g.SystemAdmin(), h.Builtin)
	p.POST("/datasources", g.Admin(), h.Create)
	p.GET("/datasources/:id", g.Viewer(), h.Status)
	p.POST("/datasources/:id/lifecycle/:action", g.Admin(), h.Lifecycle)
	p.POST("/datasources/:id/grants", g.SystemAdmin(), h.Grant)
	p.DELETE("/datasources/:id/grants/:grant_id", g.SystemAdmin(), h.Revoke)
	p.GET("/datasources/:id/audit", g.Viewer(), h.Audit)
}
