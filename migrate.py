import requests
import json 

Helium Comedy Club  - 1

Helium Comedy Club Portland - 5

Helium Portland Store - 6

Helium Philly Store - 7

Helium & Elements Restaurant - 21

Helium Buffalo Store - 23

Goodnights & Factory Restaurant - 53

Goodnights Store - 63

Levé's Annual Charity Ball Sponsoring New Avenues For Youth  - 131

Helium & Elements Restaurant - 133

venue_ids = []
show_id = ""
user_id = ""

get_shows_url = "/api/venues/%s/shows" % venue_id
get_willcall_url = "seatengine.com/api/venues/%s/shows/%s/willcall" % (venue_id, show_id)
