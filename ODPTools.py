from pyproj import Proj, transform

def lon_lat_to_x_y (lon, lat, proj_desc):
    p1 = Proj(init='epsg:4326')
    p2 = Proj(proj_desc)
    x,y = transform(p1,p2,lon,lat)
    return x, y

    
def lon_lat_to_grid_cell (lon,lat,xmin,xmax,ymin,ymax,xstep,ystep,proj_desc):
    size_x = (xmax-xmin)/xstep
    size_y = (ymax-ymin)/ystep
    
    x,y = lon_lat_to_x_y(lon,lat,proj_desc)
    if (x<xmin) or (x>xmax) or (y<ymin) or (y>ymax):
        #print 'point not in grid'
        return {'row':-1,'col':-1}
    col = int(((x-xmin)/xstep))
    row = size_y - int((y-ymin)/ystep) - 1
    return {'row':row,'col':col}
