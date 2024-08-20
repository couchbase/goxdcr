package service_def

import (
	"encoding/json"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

var unitTestChangesLeft = GlobalStatsTable[CHANGES_LEFT_METRIC]

func TestStatsProperty_toMetaObj(t *testing.T) {
	type fields struct {
		MetricType        StatsUnit
		Cardinality       MetricCardinality
		VersionAdded      base.ServerVersion
		VersionDeprecated base.ServerVersion
		Description       string
		Notes             string
		Stability         StatsStability
		UiName            string
		Labels            []StatsLabel
	}
	tests := []struct {
		name   string
		fields fields
		want   *statsPropertyMetaObj
	}{
		{
			name: "[POSITIVE] changes_left",
			fields: fields{
				MetricType:        unitTestChangesLeft.MetricType,
				Cardinality:       unitTestChangesLeft.Cardinality,
				VersionAdded:      unitTestChangesLeft.VersionAdded,
				VersionDeprecated: unitTestChangesLeft.VersionDeprecated,
				Description:       unitTestChangesLeft.Description,
				Notes:             unitTestChangesLeft.Notes,
				Stability:         unitTestChangesLeft.Stability,
				UiName:            unitTestChangesLeft.UiName,
				Labels:            unitTestChangesLeft.Labels,
			},
			want: &statsPropertyMetaObj{
				Type:       unitTestChangesLeft.MetricType.Metric.String(),
				Help:       unitTestChangesLeft.Description,
				Added:      unitTestChangesLeft.VersionAdded.String(),
				UiName:     unitTestChangesLeft.UiName,
				Unit:       unitTestChangesLeft.MetricType.Unit.String(),
				Labels:     unitTestChangesLeft.Labels.ToGenerics(),
				Deprecated: unitTestChangesLeft.VersionDeprecated.String(),
				Notes:      unitTestChangesLeft.Notes,
				Stability:  unitTestChangesLeft.Stability.String(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StatsProperty{
				MetricType:        tt.fields.MetricType,
				Cardinality:       tt.fields.Cardinality,
				VersionAdded:      tt.fields.VersionAdded,
				VersionDeprecated: tt.fields.VersionDeprecated,
				Description:       tt.fields.Description,
				Notes:             tt.fields.Notes,
				Stability:         tt.fields.Stability,
				UiName:            tt.fields.UiName,
				Labels:            tt.fields.Labels,
			}
			if got := s.toMetaObj(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toMetaObj() = %v, want %v", got, tt.want)
			}
		})
	}
}

var unitTestChangesLeftMetaObj = unitTestChangesLeft.toMetaObj()

func Test_statsPropertyMetaObj_ToStatsProperty(t *testing.T) {
	type fields struct {
		Type       string
		Help       string
		Added      string
		UiName     string
		Unit       string
		Labels     []interface{}
		Deprecated string
		Notes      string
		Stability  string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *StatsProperty
		wantErr bool
	}{
		{
			name: "changes_left from metaObj",
			fields: fields{
				Type:       unitTestChangesLeftMetaObj.Type,
				Help:       unitTestChangesLeftMetaObj.Help,
				Added:      unitTestChangesLeftMetaObj.Added,
				UiName:     unitTestChangesLeftMetaObj.UiName,
				Unit:       unitTestChangesLeftMetaObj.Unit,
				Labels:     unitTestChangesLeftMetaObj.Labels,
				Deprecated: unitTestChangesLeftMetaObj.Deprecated,
				Notes:      unitTestChangesLeftMetaObj.Notes,
				Stability:  unitTestChangesLeftMetaObj.Stability,
			},
			want:    &unitTestChangesLeft,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &statsPropertyMetaObj{
				Type:       tt.fields.Type,
				Help:       tt.fields.Help,
				Added:      tt.fields.Added,
				UiName:     tt.fields.UiName,
				Unit:       tt.fields.Unit,
				Labels:     tt.fields.Labels,
				Deprecated: tt.fields.Deprecated,
				Notes:      tt.fields.Notes,
				Stability:  tt.fields.Stability,
			}
			got, err := s.ToStatsProperty()
			if (err != nil) != tt.wantErr {
				t.Errorf("ToStatsProperty() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			//if !reflect.DeepEqual(got, tt.want) {
			if !got.SameAs(*tt.want) {
				t.Errorf("ToStatsProperty() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// This is just to ensure marshalling works and no panics ensue
func Test_GlobalStatsMarshaller(t *testing.T) {
	assert := assert.New(t)

	out, err := json.Marshal(GlobalStatsTable)
	assert.Nil(err)
	assert.NotNil(out)
	//fmt.Printf("%s\n", out)
}
