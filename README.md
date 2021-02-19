# Simpleg

Simple, lightweight, Activerecord like, embeded graph database written in go.


## Why Simpleg?

Graph databases are cool (check [here](https://dzone.com/articles/what-are-the-pros-and-cons-of-using-a-graph-databa) to know more) but they are quite expensive to use, Google and Facebook can spin up hundreds of 64GB memory servers to handle their graph database but i doubt if there is anyone out there willing to setup an 8GB memory server to host the database for a pet project. This is where Simpleg comes in removing the overkill features like accepting data over a network, dedicated interaction language and multiple server coordination features to give you a database you can host in your 1GB VPS without any issues while providing all the necessary graph database features. Simpleg uses [Badger](https://github.com/dgraph-io/badger/) as it's internal key value store thereby bringing the power of one of the top key value databases (also used by [Dgraph](https://dgraph.io/) internally) out there. Simpleg is also built with a flexible and easily extensible core giving you the opprtunity to extend with functionalities that suite your needs without modifications to the core!


## Installation

Add Simpleg to your `go.mod` file:

```
module github.com/x/y

go 1.14

require (
        github.com/timmytune/simpleg/simpleg latest
)
```


## Features

-   Clean API
-   Fast (>1k request/sec on a single core)
-   Manages request delays and maximum concurrency per domain
-   Automatic cookie and session handling
-   Sync/async/parallel scraping
-   Caching
-   Automatic encoding of non-unicode responses
-   Robots.txt support
-   Distributed scraping
-   Configuration via environment variables
-   Extensions

## Example

```go
func main() {
	c := colly.NewCollector()

	// Find and visit all links
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		e.Request.Visit(e.Attr("href"))
	})

	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL)
	})

	c.Visit("http://go-colly.org/")
}
```

See [examples folder](https://github.com/gocolly/colly/tree/master/_examples) for more detailed examples.

## Installation

Add colly to your `go.mod` file:

```
module github.com/x/y

go 1.14

require (
        github.com/gocolly/colly/v2 latest
)
```

## Bugs

Bugs or suggestions? Visit the [issue tracker](https://github.com/gocolly/colly/issues) or join `#colly` on freenode

## Other Projects Using Colly

Below is a list of public, open source projects that use Colly:

-   [greenpeace/check-my-pages](https://github.com/greenpeace/check-my-pages) Scraping script to test the Spanish Greenpeace web archive.
-   [altsab/gowap](https://github.com/altsab/gowap) Wappalyzer implementation in Go.
-   [jesuiscamille/goquotes](https://github.com/jesuiscamille/goquotes) A quotes scrapper, making your day a little better!
-   [jivesearch/jivesearch](https://github.com/jivesearch/jivesearch) A search engine that doesn't track you.
-   [Leagify/colly-draft-prospects](https://github.com/Leagify/colly-draft-prospects) A scraper for future NFL Draft prospects.
-   [lucasepe/go-ps4](https://github.com/lucasepe/go-ps4) Search playstation store for your favorite PS4 games using the command line.
-   [yringler/inside-chassidus-scraper](https://github.com/yringler/inside-chassidus-scraper) Scrapes Rabbi Paltiel's web site for lesson metadata.
-   [gamedb/gamedb](https://github.com/gamedb/gamedb) A database of Steam games.
-   [lawzava/scrape](https://github.com/lawzava/scrape) CLI for email scraping from any website.
-   [eureka101v/WeiboSpiderGo](https://github.com/eureka101v/WeiboSpiderGo) A sina weibo(chinese twitter) scrapper
-   [Go-phie/gophie](https://github.com/Go-phie/gophie) Search, Download and Stream movies from your terminal
-   [imthaghost/goclone](https://github.com/imthaghost/goclone) Clone websites to your computer within seconds.
-   [superiss/spidy](https://github.com/superiss/spidy) Crawl the web and collect expired domains.
-   [docker-slim/docker-slim](https://github.com/docker-slim/docker-slim) Optimize your Docker containers to make them smaller and better.

If you are using Colly in a project please send a pull request to add it to the list.

## Contributors

This project exists thanks to all the people who contribute. [[Contribute]](CONTRIBUTING.md).
<a href="https://github.com/gocolly/colly/graphs/contributors"><img src="https://opencollective.com/colly/contributors.svg?width=890" /></a>

## Backers

Thank you to all our backers! 🙏 [[Become a backer](https://opencollective.com/colly#backer)]

<a href="https://opencollective.com/colly#backers" target="_blank"><img src="https://opencollective.com/colly/backers.svg?width=890"></a>

## Sponsors

Support this project by becoming a sponsor. Your logo will show up here with a link to your website. [[Become a sponsor](https://opencollective.com/colly#sponsor)]

<a href="https://opencollective.com/colly/sponsor/0/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/0/avatar.svg"></a>
<a href="https://opencollective.com/colly/sponsor/1/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/1/avatar.svg"></a>
<a href="https://opencollective.com/colly/sponsor/2/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/2/avatar.svg"></a>
<a href="https://opencollective.com/colly/sponsor/3/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/3/avatar.svg"></a>
<a href="https://opencollective.com/colly/sponsor/4/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/4/avatar.svg"></a>
<a href="https://opencollective.com/colly/sponsor/5/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/5/avatar.svg"></a>
<a href="https://opencollective.com/colly/sponsor/6/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/6/avatar.svg"></a>
<a href="https://opencollective.com/colly/sponsor/7/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/7/avatar.svg"></a>
<a href="https://opencollective.com/colly/sponsor/8/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/8/avatar.svg"></a>
<a href="https://opencollective.com/colly/sponsor/9/website" target="_blank"><img src="https://opencollective.com/colly/sponsor/9/avatar.svg"></a>

## License

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fgocolly%2Fcolly.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fgocolly%2Fcolly?ref=badge_large)
