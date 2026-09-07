package prototype

import "github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"

// Compatibility facades share validation with the formal backend.
type PathMapping = paths.PathMapping
type GrantSnapshot = paths.GrantSnapshot

var CreateGrant = paths.CreateGrant
var RevalidateGrant = paths.RevalidateGrant
