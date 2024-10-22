package conflictlog

import (
	"encoding/json"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/stretchr/testify/require"
)

type testSource struct {
	Ns base.CollectionNamespace
}

func newTestSource(scope, collection string) testSource {
	return testSource{Ns: base.CollectionNamespace{ScopeName: scope, CollectionName: collection}}
}

func (s testSource) Scope() string {
	return s.Ns.ScopeName
}

func (s testSource) Collection() string {
	return s.Ns.CollectionName
}

func Test_conflictMapper_Map(t *testing.T) {
	testData := []struct {
		name string
		// jsonStr is the json in string form which simulates the input
		// to the update settings. The test setup will parse this first.
		// and any error in parsing is a test fail as it is not the objective
		// of the test
		jsonStr string
		// for every conflicts[i], expectedTargets[i] is the expected output of Map()
		conflicts       []testSource
		expectedTargets []base.ConflictLogTarget
	}{
		{
			name: "[positive] scope and collection incomplete, should default to _default",
			jsonStr: `{
				"bucket":"B1",
				"collection": "S1.C1",
				"loggingRules": {
					"US.Ohio": {
						"bucket": "B2"
					}
				}
			}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B2", "_default", "_default"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
			},
		},
		{
			name: "[positive] just collection incomplete, should default to _default",
			jsonStr: `{
				"bucket":"B1",
				"collection": "S1.C1",
				"loggingRules": {
					"US.Ohio": {
						"bucket": "B2",
						"collection": "S2"
					}
				}
			}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B2", "S2", "_default"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
			},
		},
		{
			name: "[positive] fallback target collection missing, should default to _default",
			jsonStr: `{
				"bucket":"B1",
				"collection": "S1",
				"loggingRules": {
					"US.Ohio": {
						"bucket": "B2",
						"collection": "S2.C1"
					}
				}
			}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "S1", "_default"),
				base.NewConflictLogTarget("B1", "S1", "_default"),
				base.NewConflictLogTarget("B2", "S2", "C1"),
				base.NewConflictLogTarget("B1", "S1", "_default"),
			},
		},
		{
			name: "[positive] fallback target scope and collection missing, should default to _default",
			jsonStr: `{
				"bucket":"B1",
				"loggingRules": {
					"US.Ohio": {
						"bucket": "B2",
						"collection": "S2.C1"
					}
				}
			}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "_default", "_default"),
				base.NewConflictLogTarget("B1", "_default", "_default"),
				base.NewConflictLogTarget("B2", "S2", "C1"),
				base.NewConflictLogTarget("B1", "_default", "_default"),
			},
		},
		{
			name: "[positive] only default target",
			jsonStr: `{
				"bucket":"B1",
				"collection":"S1.C1"
			}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
			},
		},
		{
			name: "[positive] only default target but loggingRules=nil",
			jsonStr: `{
					"bucket":"B1",
					"collection":"S1.C1",
					"loggingRules": null
				}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
			},
		},
		{
			name: "[positive] only default target but loggingRules={}",
			jsonStr: `{
					"bucket":"B1",
					"collection":"S1.C1",
					"loggingRules": {}
				}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
			},
		},
		{
			name: "[positive] only scope in source key",
			jsonStr: `{
					"bucket":"B1",
					"collection": "S1.C1",
					"loggingRules": {
						"US": null,
						"US.Ohio": {
							"bucket": "B2",
							"collection": "S2.C2"
						}
					}
				}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B2", "S2", "C2"),
				base.BlacklistConflictLogTarget(),
			},
		},
		{
			name: "[positive] scope.collection in source key",
			jsonStr: `{
					"bucket":"B1",
					"collection": "S1.C1",
					"loggingRules": {
						"US": null,
						"India": {},
						"US.Ohio": {
							"bucket": "B2",
							"collection": "S2.C2"
						}
					}
				}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
				newTestSource("US", "Texas"),
				newTestSource("India", "Mumbai"),
				newTestSource("India", "Bengaluru"),
				newTestSource("India", "_default"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B2", "S2", "C2"),
				base.BlacklistConflictLogTarget(),
				base.BlacklistConflictLogTarget(),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
				base.NewConflictLogTarget("B1", "S1", "C1"),
			},
		},
		{
			name: "[positive] target collection missing, default to be used",
			jsonStr: `{
					"bucket":"B1",
					"collection": "S1",
					"loggingRules": {
						"US": null,
						"India": {},
						"US.Ohio": {
							"bucket": "B2",
							"collection": "S2"
						}
					}
				}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
				newTestSource("US", "Texas"),
				newTestSource("India", "Mumbai"),
				newTestSource("India", "Bengaluru"),
				newTestSource("India", "_default"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "S1", "_default"),
				base.NewConflictLogTarget("B1", "S1", "_default"),
				base.NewConflictLogTarget("B2", "S2", "_default"),
				base.BlacklistConflictLogTarget(),
				base.BlacklistConflictLogTarget(),
				base.NewConflictLogTarget("B1", "S1", "_default"),
				base.NewConflictLogTarget("B1", "S1", "_default"),
				base.NewConflictLogTarget("B1", "S1", "_default"),
			},
		},
		{
			name: "[positive] target collection and scope missing, default to be used",
			jsonStr: `{
					"bucket":"B1",
					"loggingRules": {
						"US": null,
						"India": {},
						"US.Ohio": {
							"bucket": "B2"
						}
					}
				}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("US", "Ohio"),
				newTestSource("US", "NY"),
				newTestSource("US", "Texas"),
				newTestSource("India", "Mumbai"),
				newTestSource("India", "Bengaluru"),
				newTestSource("India", "_default"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("B1", "_default", "_default"),
				base.NewConflictLogTarget("B1", "_default", "_default"),
				base.NewConflictLogTarget("B2", "_default", "_default"),
				base.BlacklistConflictLogTarget(),
				base.BlacklistConflictLogTarget(),
				base.NewConflictLogTarget("B1", "_default", "_default"),
				base.NewConflictLogTarget("B1", "_default", "_default"),
				base.NewConflictLogTarget("B1", "_default", "_default"),
			},
		},
		{
			name: "[positive] the one from design doc",
			jsonStr: `{
				"bucket": "bucket1",
				"collection": "scope2.collection3",
				"loggingRules": {
					"s1": {},
					"s1.privateCol": null,
					"privateScope": null,
					"privateScope.specialCol": {},
					"rbacScope": {
						"bucket": "customBucket",
						"collection": "customScope.customCollection"
					},
					"rbacScope.rbacCollection": {
						"bucket": "customBucket",
						"collection": "specialScope.specialCollection"
					},
					"rbacScope.rbacCollection2": {},
					"rbacScope.rbacCollection3": null
				}
			}`,
			conflicts: []testSource{
				newTestSource("_default", "_default"),
				newTestSource("S3", "C3"),
				newTestSource("s1", "privateCol1"),
				newTestSource("s1", "privateCol"),
				newTestSource("privateScope", "specialCol1"),
				newTestSource("privateScope", "specialCol"),
				newTestSource("rbacScope", "rbacCollection1"),
				newTestSource("rbacScope", "rbacCollection"),
				newTestSource("rbacScope", "rbacCollection2"),
				newTestSource("rbacScope", "rbacCollection3"),
			},
			expectedTargets: []base.ConflictLogTarget{
				base.NewConflictLogTarget("bucket1", "scope2", "collection3"),
				base.NewConflictLogTarget("bucket1", "scope2", "collection3"),
				base.NewConflictLogTarget("bucket1", "scope2", "collection3"),
				base.BlacklistConflictLogTarget(),
				base.BlacklistConflictLogTarget(),
				base.NewConflictLogTarget("bucket1", "scope2", "collection3"),
				base.NewConflictLogTarget("customBucket", "customScope", "customCollection"),
				base.NewConflictLogTarget("customBucket", "specialScope", "specialCollection"),
				base.NewConflictLogTarget("bucket1", "scope2", "collection3"),
				base.BlacklistConflictLogTarget(),
			},
		},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			j := map[string]interface{}{}
			err := json.Unmarshal([]byte(tt.jsonStr), &j)
			require.Nil(t, err)

			rules, err := base.ParseConflictLogRules(j)
			require.Nil(t, err)

			err = rules.Validate()
			require.Nil(t, err)

			cm := NewConflictMapper(log.NewLogger("test", log.DefaultLoggerContext))
			require.Equal(t, len(tt.conflicts), len(tt.expectedTargets))

			for i := 0; i < len(tt.conflicts); i++ {
				target, err := cm.Map(rules, tt.conflicts[i])
				require.Nil(t, err)

				require.Equal(t, target, tt.expectedTargets[i])
			}
		})
	}
}
