module cobber.com/pgmaven

go 1.20

require (
	cobber.com/dbinfo v0.0.0-00010101000000-000000000000
	github.com/elliotchance/sshtunnel v1.6.1
	github.com/lib/pq v1.10.9
	golang.org/x/crypto v0.17.0
)

require golang.org/x/sys v0.15.0 // indirect

replace cobber.com/dbinfo => ./dbinfo
