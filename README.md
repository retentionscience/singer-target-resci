# target-resci

This is a [Singer](https://singer.io) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## Install

Requires Python 3

```bash
› pip install target-resci
```

## Use

target-resci takes two types of input:

1. A config file containing your ReSci credentials
2. A stream of Singer-formatted data on stdin

Create config file to contain your ReSci api_key and site_id:

```json
{
  "site_id" : 1234,
  "api_key" : "asdkjqbawsdciobasdpkjnqweobdclakjsdbcakbdsac",
  "import_type": "singer_import_config"
}

# Optional, used for testing:
"api_url": "http://test-api-url/v3/import_jobs"
```


## Running
```bash
tap-some-api | target-resci --config config.json
```

where `tap-some-api` is [Singer Tap](https://singer.io).

---



