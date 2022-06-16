// biofseventlog creates usage events for biofs, a GRAIL-internal program. biofs has to be internal
// because it runs fsnodefuse with some fsnode.T's derived from other internal code, but it also
// uses github.com/grailbio packages like s3file.
package biofseventlog

import (
	"github.com/grailbio/base/config"
	"github.com/grailbio/base/eventlog"
	"github.com/grailbio/base/must"
)

const configName = "biofs/eventer"

func init() {
	config.Default(configName, "eventer/nop")
}

// UsedFeature creates an event for usage of the named feature.
func UsedFeature(featureName string) {
	var eventer eventlog.Eventer
	must.Nil(config.Instance(configName, &eventer))
	eventer.Event("usedFeature", "name", featureName)
}
