package tstorage

import (
	"math"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_storage_Select(t *testing.T) {
	tests := []struct {
		name    string
		storage storage
		metric  string
		labels  []Label
		start   int64
		end     int64
		want    []*DataPoint
		wantErr bool
	}{
		{
			name:   "select from single partition",
			metric: "metric1",
			start:  1,
			end:    4,
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []*DataPoint{
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
			},
		},
		{
			name:   "select from three partitions",
			metric: "metric1",
			start:  1,
			end:    10,
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				part2 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part2.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 4}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 5}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 6}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				part3 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part3.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 7}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 8}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 9}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				list.insert(part2)
				list.insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []*DataPoint{
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
				{Timestamp: 4},
				{Timestamp: 5},
				{Timestamp: 6},
				{Timestamp: 7},
				{Timestamp: 8},
				{Timestamp: 9},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.storage.Select(tt.metric, tt.labels, tt.start, tt.end)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_storage_LastTimestamp(t *testing.T) {
	tests := []struct {
		name    string
		storage storage
		want    int64
	}{
		{
			name: "no partitions",
			storage: func() storage {
				list := newPartitionList()
				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: 0,
		},
		{
			name: "single partition",
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: 3,
		},
		{
			name: "three partitions",
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				part2 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part2.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 4}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 5}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 6}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				part3 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part3.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 7}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 8}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 9}, Metric: "metric1"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				list.insert(part2)
				list.insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: 9,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.storage.LastTimestamp()
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_storage_ListMetrics(t *testing.T) {
	tests := []struct {
		name    string
		storage storage
		want    []string
		wantErr bool
	}{
		{
			name: "no partitions",
			storage: func() storage {
				list := newPartitionList()
				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			wantErr: true,
		},
		{
			name: "single partition",
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric2"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric3"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []string{"metric1", "metric2", "metric3"},
		},
		{
			name: "three partitions",
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric2"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric3"},
				})
				if err != nil {
					panic(err)
				}
				part2 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part2.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 4}, Metric: "metric4"},
					{DataPoint: DataPoint{Timestamp: 5}, Metric: "metric5"},
					{DataPoint: DataPoint{Timestamp: 6}, Metric: "metric6"},
				})
				if err != nil {
					panic(err)
				}
				part3 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part3.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 7}, Metric: "metric7"},
					{DataPoint: DataPoint{Timestamp: 8}, Metric: "metric8"},
					{DataPoint: DataPoint{Timestamp: 9}, Metric: "metric9"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				list.insert(part2)
				list.insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []string{"metric1", "metric2", "metric3", "metric4", "metric5", "metric6", "metric7", "metric8", "metric9"},
		},
		{
			name: "three partitions with duplicates",
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric2"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric3"},
				})
				if err != nil {
					panic(err)
				}
				part2 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part2.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 4}, Metric: "metric4"},
					{DataPoint: DataPoint{Timestamp: 5}, Metric: "metric5"},
					{DataPoint: DataPoint{Timestamp: 6}, Metric: "metric6"},
				})
				if err != nil {
					panic(err)
				}
				part3 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part3.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 7}, Metric: "metric7"},
					{DataPoint: DataPoint{Timestamp: 8}, Metric: "metric8"},
					{DataPoint: DataPoint{Timestamp: 9}, Metric: "metric9"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				list.insert(part2)
				list.insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []string{"metric1", "metric2", "metric3", "metric4", "metric5", "metric6", "metric7", "metric8", "metric9"},
		},
		{
			name: "three partitions with duplicates and different metrics",
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric2"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric3"},
				})
				if err != nil {
					panic(err)
				}
				part2 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part2.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 4}, Metric: "metric4"},
					{DataPoint: DataPoint{Timestamp: 5}, Metric: "metric5"},
					{DataPoint: DataPoint{Timestamp: 6}, Metric: "metric6"},
				})
				if err != nil {
					panic(err)
				}
				part3 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part3.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 7}, Metric: "metric7"},
					{DataPoint: DataPoint{Timestamp: 8}, Metric: "metric8"},
					{DataPoint: DataPoint{Timestamp: 9}, Metric: "metric9"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				list.insert(part2)
				list.insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []string{"metric1", "metric2", "metric3", "metric4", "metric5", "metric6", "metric7", "metric8", "metric9"},
		},
		{
			name: "three partitions with duplicates and different metrics and different timestamps",
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: "metric2"},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: "metric3"},
				})
				if err != nil {
					panic(err)
				}
				part2 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part2.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 4}, Metric: "metric4"},
					{DataPoint: DataPoint{Timestamp: 5}, Metric: "metric5"},
					{DataPoint: DataPoint{Timestamp: 6}, Metric: "metric6"},
				})
				if err != nil {
					panic(err)
				}
				part3 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part3.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 7}, Metric: "metric7"},
					{DataPoint: DataPoint{Timestamp: 8}, Metric: "metric8"},
					{DataPoint: DataPoint{Timestamp: 9}, Metric: "metric9"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				list.insert(part2)
				list.insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []string{"metric1", "metric2", "metric3", "metric4", "metric5", "metric6", "metric7", "metric8", "metric9"},
		},
		{
			name: "three partitions with duplicates and different metrics and different timestamps and different values",
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1, Value: 1}, Metric: "metric1"},
					{DataPoint: DataPoint{Timestamp: 2, Value: 2}, Metric: "metric2"},
					{DataPoint: DataPoint{Timestamp: 3, Value: 3}, Metric: "metric3"},
				})
				if err != nil {
					panic(err)
				}
				part2 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part2.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 4, Value: 4}, Metric: "metric4"},
					{DataPoint: DataPoint{Timestamp: 5, Value: 5}, Metric: "metric5"},
					{DataPoint: DataPoint{Timestamp: 6, Value: 6}, Metric: "metric6"},
				})
				if err != nil {
					panic(err)
				}
				part3 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part3.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 7, Value: 7}, Metric: "metric7"},
					{DataPoint: DataPoint{Timestamp: 8, Value: 8}, Metric: "metric8"},
					{DataPoint: DataPoint{Timestamp: 9, Value: 9}, Metric: "metric9"},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				list.insert(part2)
				list.insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []string{"metric1", "metric2", "metric3", "metric4", "metric5", "metric6", "metric7", "metric8", "metric9"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.storage.ListMetrics()
			// Sort metrics to make sure that order is the same
			sort.Strings(got)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, got)
		})
	}

}
