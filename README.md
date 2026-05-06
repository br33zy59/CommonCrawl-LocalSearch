# CommonCrawl-LocalSearch

A small **Go** utility for scanning locally downloaded **Common Crawl** CDX-style index files (`*.gz`). It decompresses each index in parallel, extracts the `url` field from each line, and writes every URL whose path matches your pattern to a text file (one URL per line).

Typical uses: finding historical URLs for a file extension, path segment, or other substring you care about, without querying a remote service.

## Requirements

- [Go](https://go.dev/dl/) 1.21 or newer
- Gzipped Common Crawl index files in a folder on disk (see [Common Crawl](https://commoncrawl.org/) for how to obtain them)

## Build

From the repository root:

```bash
go build -o cc-search ./src
```

On Windows, you may prefer `cc-search.exe` as the output name.

## Run

You must pass a **pattern** (`-pattern`) and the folder that contains **`*.gz` indexes** (`-indexdir`). Relative `indexdir` paths are resolved from the directory containing the **executable** (not necessarily the current working directory).

```bash
./cc-search -pattern=.pdf -indexdir=Crawl_Data/CC-MAIN-2013-20/indexes
```

Useful flags:

| Flag | Meaning |
|------|---------|
| `-mode=suffix` | URL path ends with the pattern (default) |
| `-mode=contains` | Pattern appears anywhere in the matched portion of the URL |
| `-stripquery=true` | Strip `?` and `#` before matching (default) |
| `-workers=N` | Concurrent index files (default: ¾ of logical CPU count) |

Run `cc-search -h` for the full list.

## Output

By default, URLs are written to `crawl_<pattern>_urls.txt` in the **current working directory** (for example, `crawl_pdf_urls.txt` when `-pattern=.pdf`).

## License

Add a `LICENSE` file if you publish this repository publicly.
