from ECHO_modules.get_data import get_echo_data
import pdb
import pandas as pd
from IPython.display import display

from ipyleaflet import Map, basemaps, basemap_to_tiles, DrawControl

focus_year = "2022"  # In the future, make this interactive
name = "Chicago"  # In the future, make this interactive
# ### 6. Get the data only.
# Ask the database for ECHO_EXPORTER records for facilities in area
# * echo_data stores all records.
# * echo_active is all records in echo_data identified as active.

watercolor = basemap_to_tiles(basemaps.Stamen.Watercolor)

m = Map(layers=(watercolor,), center=(42, -88), zoom=10)


global shapes
shapes = set()


def handle_draw(action, geo_json):
    global shapes
    polygon = []
    for coords in geo_json["geometry"]["coordinates"][0][:-1][:]:
        polygon.append(tuple(coords))
    polygon = tuple(polygon)
    if action == "created":
        shapes.add(polygon)
        shapes.pop()
        print("eh")
    elif action == "deleted":
        shapes.discard(polygon)


draw_control = DrawControl()

draw_control.rectangle = {
    "shapeOptions": {"fillColor": "#fca45d", "color": "#fca45d", "fillOpacity": 1.0}
}
draw_control.on_draw(handle_draw)
m.add_control(draw_control)
display(m)
echo_data = {}
echo_active = {}
test = shapes.pop()


# Get data
sql = """
SELECT *
FROM "ECHO_EXPORTER"
WHERE ST_WITHIN("wkb_geometry", ST_GeomFromText('POLYGON(({} {},{} {}, {} {},{} {},{} {}))', 4269) );
""".format(
    test[0][0],
    test[0][1],
    test[1][0],
    test[1][1],
    test[2][0],
    test[2][1],
    test[3][0],
    test[3][1],
    test[0][0],
    test[0][1],
)

try:
    echo_data = get_echo_data(sql, "REGISTRY_ID")
    echo_active = echo_data.loc[echo_data["FAC_ACTIVE_FLAG"] == "Y"]
    display(echo_data)
except pd.errors.EmptyDataError:
    print("no data")
