package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/plugin/controller"
	"github.com/Tencent/WeKnora/internal/plugin/store"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"github.com/gin-gonic/gin"
)

type PluginHandler struct {
	control *controller.Controller
	sources *DataSourceHandler
	audit   interfaces.AuditLogRepository
}

func NewPluginHandler(c *controller.Controller, sources *DataSourceHandler, audit interfaces.AuditLogRepository) *PluginHandler {
	return &PluginHandler{c, sources, audit}
}

func pluginBody(c *gin.Context, out any) bool {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, sdk.MaxConfigBytes)
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		c.JSON(400, gin.H{"error": "invalid plugin request"})
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		c.JSON(400, gin.H{"error": "trailing request JSON"})
		return false
	}
	return true
}
func pluginError(c *gin.Context, err error) {
	message := "plugin operation failed; inspect authorized instance status or contact administrator"
	if types.IsSystemAdminFromContext(c.Request.Context()) {
		message = err.Error()
	}
	c.JSON(http.StatusConflict, gin.H{"error": message})
}
func (h *PluginHandler) owned(c *gin.Context) (*types.DataSource, bool) {
	tenant := h.sources.getTenantID(c)
	if tenant == 0 {
		c.JSON(401, gin.H{"error": "workspace context required"})
		return nil, false
	}
	ds, status, message := h.sources.getOwnedDataSource(c.Request.Context(), tenant, c.Param("id"))
	if status != 200 {
		c.JSON(status, gin.H{"error": message})
		return nil, false
	}
	return ds, true
}

func (h *PluginHandler) List(c *gin.Context) {
	ctx := c.Request.Context()
	rows := []gin.H{}
	for _, d := range h.control.Catalog.List("") {
		row := gin.H{"definition": d}
		if d.Source == control.SourceBuiltin {
			state, _ := h.control.Manager.Status(d.ID)
			if state.LastError != "" && !types.IsSystemAdminFromContext(ctx) {
				state.LastError = "capability unavailable; contact administrator"
			}
			row["status"] = state
		}
		rows = append(rows, row)
	}
	installations, err := h.control.Store.ListInstallations(ctx)
	if err != nil {
		pluginError(c, err)
		return
	}
	if !types.IsSystemAdminFromContext(ctx) {
		for i := range installations {
			installations[i].LastError = ""
		}
	}
	c.JSON(200, gin.H{"data": rows, "installations": installations, "external_enabled": h.control.Config.Enabled})
}

func (h *PluginHandler) Builtin(c *gin.Context) {
	ctx := c.Request.Context()
	id := control.PluginID(c.Param("plugin_id"))
	definition, lookupErr := h.control.Catalog.Get(id)
	if lookupErr != nil || definition.Source != control.SourceBuiltin {
		c.JSON(404, gin.H{"error": "builtin plugin not found"})
		return
	}
	var err error
	switch c.Param("action") {
	case "start":
		err = h.control.Manager.Start(ctx, id)
	case "stop":
		err = h.control.Manager.Stop(ctx, id)
	case "health":
	default:
		c.JSON(400, gin.H{"error": "expected start, stop or health"})
		return
	}
	state := h.control.Manager.Health(ctx, id)
	actor, _ := types.UserIDFromContext(ctx)
	outcome := types.AuditOutcomeSuccess
	if err != nil {
		outcome = types.AuditOutcomeFailed
	}
	err = errors.Join(err, h.audit.Create(ctx, &types.AuditLog{ActorUserID: actor,
		Action: "plugin.builtin_" + types.AuditAction(c.Param("action")), TargetType: "plugin", TargetID: string(id),
		Outcome: outcome, CreatedAt: time.Now()}))
	if err != nil {
		pluginError(c, err)
		return
	}
	c.JSON(200, gin.H{"data": state})
}

func (h *PluginHandler) Create(c *gin.Context) {
	var req controller.CreateRequest
	if !pluginBody(c, &req) {
		return
	}
	tenant := h.sources.getTenantID(c)
	if tenant == 0 {
		c.JSON(401, gin.H{"error": "workspace context required"})
		return
	}
	if _, status, message := h.sources.getOwnedKnowledgeBase(c.Request.Context(), tenant, req.KnowledgeBaseID); status != 200 {
		c.JSON(status, gin.H{"error": message})
		return
	}
	ds, err := h.control.Create(c.Request.Context(), tenant, req)
	if err != nil {
		pluginError(c, err)
		return
	}
	c.JSON(201, gin.H{"data": ds, "message": "paused; directory grant and explicit enable required"})
}

func (h *PluginHandler) Status(c *gin.Context) {
	ds, ok := h.owned(c)
	if !ok {
		return
	}
	b, err := h.control.Store.GetBinding(c.Request.Context(), ds.ID)
	if err != nil {
		pluginError(c, err)
		return
	}
	g, err := h.control.Store.GetDirectoryGrant(c.Request.Context(), ds.ID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		pluginError(c, err)
		return
	}
	if errors.Is(err, store.ErrNotFound) {
		g = nil
	}
	if !types.IsSystemAdminFromContext(c.Request.Context()) && b.LastError != "" {
		b.LastError = "instance not ready; contact administrator"
	}
	c.JSON(200, gin.H{"binding": b, "grant": g, "desired_status": ds.Status})
}

func (h *PluginHandler) Lifecycle(c *gin.Context) {
	ds, ok := h.owned(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()
	if c.Param("action") == "health" {
		health, err := h.control.Health(ctx, ds.ID)
		if err != nil {
			pluginError(c, err)
			return
		}
		c.JSON(200, gin.H{"data": health})
		return
	}
	enabled := c.Param("action") == "enable"
	if !enabled && c.Param("action") != "disable" {
		c.JSON(400, gin.H{"error": "expected enable, disable or health"})
		return
	}
	err := h.control.SetEnabled(ctx, ds.ID, enabled)
	err = errors.Join(err, h.control.RecordManagement(ctx, ds, "plugin."+types.AuditAction(c.Param("action")), err))
	if err != nil {
		pluginError(c, err)
		return
	}
	h.Status(c)
}

func (h *PluginHandler) Grant(c *gin.Context) {
	ds, ok := h.owned(c)
	if !ok {
		return
	}
	var req struct {
		AllowRootID       string `json:"allow_root_id"`
		RelativeDirectory string `json:"relative_directory"`
	}
	if !pluginBody(c, &req) {
		return
	}
	g, err := h.control.Grant(c.Request.Context(), ds, req.AllowRootID, req.RelativeDirectory)
	if err != nil {
		pluginError(c, err)
		return
	}
	c.JSON(201, gin.H{"data": g})
}
func (h *PluginHandler) Revoke(c *gin.Context) {
	ds, ok := h.owned(c)
	if !ok {
		return
	}
	if err := h.control.Revoke(c.Request.Context(), ds, c.Param("grant_id")); err != nil {
		pluginError(c, err)
		return
	}
	h.Status(c)
}

func (h *PluginHandler) Audit(c *gin.Context) {
	ds, ok := h.owned(c)
	if !ok {
		return
	}
	after, err := strconv.ParseUint(c.DefaultQuery("after_id", "0"), 10, 64)
	if err != nil {
		c.JSON(400, gin.H{"error": "invalid after_id"})
		return
	}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if err != nil || limit < 1 || limit > 100 {
		c.JSON(400, gin.H{"error": "limit must be 1..100"})
		return
	}
	rows, err := h.audit.List(c.Request.Context(), ds.TenantID, &interfaces.AuditLogQuery{AfterID: after, Limit: limit, ScopeType: "data_source", ScopeID: ds.ID, Action: types.AuditAction(c.Query("action"))})
	if err != nil {
		pluginError(c, err)
		return
	}
	c.JSON(200, gin.H{"data": rows})
}
