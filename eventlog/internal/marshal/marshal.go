// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package marshal

import (
	"encoding/json"
	"fmt"
)

// eventTypeFieldKey is the name of the JSON member holding the event type,
// passed as typ, in the JSON string returned by marshal. It is a reserved field
// key name.
const eventTypeFieldKey = "eventType"

// marshal marshal event information into a JSON string. Field keys must be
// unique, otherwise marshal returns an error.
func Marshal(typ string, fieldPairs []interface{}) (string, error) {
	if len(fieldPairs)%2 != 0 {
		return "", fmt.Errorf("len(fieldPairs) must be even; %d is not even", len(fieldPairs))
	}
	fields := make(map[string]interface{})
	for i := 0; i < len(fieldPairs); i++ {
		key, isString := fieldPairs[i].(string)
		if !isString {
			return "", fmt.Errorf("field key at fieldPairs[%d] must be a string: %v", i, fieldPairs[i])
		}
		if key == eventTypeFieldKey {
			return "", fmt.Errorf("field key at fieldPairs[%d] is '%s'; '%s' is reserved", i, eventTypeFieldKey, eventTypeFieldKey)
		}
		if _, dupKey := fields[key]; dupKey {
			return "", fmt.Errorf("key %q at fieldPairs[%d] already used; duplicate keys not allowed", key, i)
		}
		i++
		fields[key] = fieldPairs[i]
	}
	fields[eventTypeFieldKey] = typ
	bs, err := json.Marshal(fields)
	if err != nil {
		return "", fmt.Errorf("error marshaling fields to JSON: %v", err)
	}
	return string(bs), nil
}
