from .geoutils import *

# Issue: when NGRID_X = NGRID_Y = 100, the number of cells is 10100.
# In general, the number of cells is NGRID_X * (NGRID_Y + 1).

print(LAT_HEIGHT, LON_WIDTH)
print(to_grid_cell_id(LAT_NORTH + 1e-5, LON_WEST + 1e-5), to_grid_cell_id(LAT_SOUTH - 1e-5, LON_EAST - 1e-5))
