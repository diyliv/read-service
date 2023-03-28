package models

import "time"

type Response struct {
	AssetId    string      `json:"asset_id"`
	TagType    string      `json:"tag_type"`
	TagValue   interface{} `json:"tag_value"`
	TagQuality int16       `json:"quality"`
	ReadAt     time.Time   `json:"read_at"`
}
