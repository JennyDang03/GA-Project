import osrm



client = osrm.Client(host='http://localhost:5000')





# -15.777522777636888, -47.782175979050564

# -15.759681042590067, -47.74578377036943

response = client.route(
    coordinates=[[-47.782175979050564, -15.777522777636888], [-47.74578377036943, -15.759681042590067]],
    overview=osrm.overview.full)

for route in response['routes']:
    assert route['distance'] > 0
    assert route['distance'] < 100000
    print(route['distance'])


