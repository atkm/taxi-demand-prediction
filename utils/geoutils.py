
LAT_NORTH = 41.0
LAT_SOUTH = 40.5
LON_EAST = -73.7
LON_WEST = -74.05

LAT_HEIGHT = LAT_NORTH - LAT_SOUTH
LON_WIDTH = LON_EAST - LON_WEST

# TODO: make this a variable. Need to make a class.
NGRID_X = 150 # the number of grids in east-west
DELTA_X = LON_WIDTH/NGRID_X
NGRID_Y = 100 # in north-south
DELTA_Y = LAT_HEIGHT/NGRID_Y


def is_in_nyc(lat, lon):
    return LON_WEST <= lon and lon <= LON_EAST and LAT_SOUTH <= lat and lat <= LAT_NORTH

# convert a lat-lon pair to a grid ID
def to_grid_cell_id(lat, lon):
    xIdx = _get_grid_cell_x(lon)
    yIdx = _get_grid_cell_y(lat)
    cell_id = NGRID_X * yIdx + xIdx
    #assert 0 <= cell_id and cell_id < NGRID_X*(NGRID_Y+1) # why NGRID_Y+1 ?
    return cell_id

def to_grid_coord(lat, lon):
    xIdx = _get_grid_cell_x(lon)
    yIdx = _get_grid_cell_y(lat)
    return (xIdx, yIdx)

def _get_grid_cell_x(lon):
    return int( (lon - LON_WEST) / DELTA_X )

def _get_grid_cell_y(lat):
    return int( (LAT_NORTH - lat) / DELTA_Y )

# converts a grid ID to a lat-lon pair
# needed to plot grids
def get_grid_center_lon(grid_id):
    pass

def get_grid_center_lat(grid_id):
    pass
