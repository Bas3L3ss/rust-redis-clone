use std::f64::consts::PI;

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

const EARTH_RADIUS_METERS: f64 = 6372797.560856;

fn spread_int32_to_int64(v: u32) -> u64 {
    let mut result = v as u64;
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2)) & 0x3333333333333333;
    (result | (result << 1)) & 0x5555555555555555
}

fn interleave(x: u32, y: u32) -> u64 {
    let x_spread = spread_int32_to_int64(x);
    let y_spread = spread_int32_to_int64(y);
    let y_shifted = y_spread << 1;
    x_spread | y_shifted
}

pub fn encode(latitude: f64, longitude: f64) -> u64 {
    // Normalize to the range 0-2^26
    let normalized_latitude = 2.0_f64.powi(26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
    let normalized_longitude = 2.0_f64.powi(26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;

    // Truncate to integers
    let lat_int = normalized_latitude as u32;
    let lon_int = normalized_longitude as u32;

    interleave(lat_int, lon_int)
}

fn compact_int64_to_int32(v: u64) -> u32 {
    let mut result = v & 0x5555555555555555;
    result = (result | (result >> 1)) & 0x3333333333333333;
    result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result >> 4)) & 0x00FF00FF00FF00FF;
    result = (result | (result >> 8)) & 0x0000FFFF0000FFFF;
    ((result | (result >> 16)) & 0x00000000FFFFFFFF) as u32 // Cast to u32
}

fn convert_grid_numbers_to_coordinates(
    grid_latitude_number: u32,
    grid_longitude_number: u32,
) -> (f64, f64) {
    // Calculate the grid boundaries
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / 2.0_f64.powi(26));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2.0_f64.powi(26));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / 2.0_f64.powi(26));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2.0_f64.powi(26));

    // Calculate the center point of the grid cell
    let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;

    (latitude, longitude)
}

pub fn decode(geo_code: u64) -> (f64, f64) {
    // Align bits of both latitude and longitude to take even-numbered position
    let y = geo_code >> 1;
    let x = geo_code;

    // Compact bits back to 32-bit ints
    let grid_latitude_number = compact_int64_to_int32(x);
    let grid_longitude_number = compact_int64_to_int32(y);

    convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
}

pub fn validate_longitude(lon: f64) -> bool {
    lon >= -180.0 && lon <= 180.0
}

pub fn validate_latitude(lat: f64) -> bool {
    lat >= -85.05112878 && lat <= 85.05112878
}

pub fn geo_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let to_radians = |deg: f64| deg * PI / 180.0;

    let lat1_rad = to_radians(lat1);
    let lat2_rad = to_radians(lat2);
    let delta_lat = to_radians(lat2 - lat1);
    let delta_lon = to_radians(lon2 - lon1);

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_METERS * c
}

pub fn bounding_box(lat: f64, lon: f64, radius_m: f64) -> (f64, f64, f64, f64) {
    let d_lat = (radius_m / EARTH_RADIUS_METERS).to_degrees();
    let lat_rad = lat.to_radians();
    let cos_lat = lat_rad.cos();
    let adjusted_cos_lat = if cos_lat.abs() < 1e-9 { 1e-9 } else { cos_lat };
    let d_lon = (radius_m / (EARTH_RADIUS_METERS * adjusted_cos_lat)).to_degrees();

    (lat - d_lat, lat + d_lat, lon - d_lon, lon + d_lon) // min_lat, max_lat, min_lon, max_lon
}

pub fn haversine(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_METERS * a.sqrt().asin()
}
