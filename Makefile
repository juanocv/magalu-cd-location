OSRM_PBF=data/osm/nordeste-latest.osm.pbf

up:
	docker compose up -d osrm

preprocess:
	docker compose run --rm osrm-preprocess

down:
	docker compose down

logs:
	docker compose logs -f osrm
