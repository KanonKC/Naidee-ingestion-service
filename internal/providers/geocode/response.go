package geocode

// searchResult is one entry of the Nominatim /search jsonv2 array. Nominatim
// returns lat/lon as strings, not numbers.
type searchResult struct {
	Lat         string `json:"lat"`
	Lon         string `json:"lon"`
	DisplayName string `json:"display_name"`
}
