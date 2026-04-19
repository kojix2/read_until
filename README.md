# read_until

Minimal Crystal scaffolding for MinKNOW Read Until development.

Current focus:
- compile-safe ReadUntil client skeleton
- dependency chain validation: read_until -> minknow -> grpc -> proto

## Installation

1. Add the dependency to your shard.yml:

   ```yaml
   dependencies:
     read_until:
       path: ../read_until_api.cr
   ```

2. Run `shards install`

## Usage

```crystal
require "read_until"
```

The simplest compile-oriented example is in examples/basic.cr.

It wires a Minknow manager, position, connection, and ReadUntil client together
without performing real RPCs yet. That makes it a good first Ubuntu smoke test
once the real transport layer starts landing.

Run example:

```sh
crystal run examples/basic.cr
```

Expected output shape:

```text
endpoint=<host:port>
running=true
latest_read_id=<read-id>
queued_actions=<n>
```

## Bring-up Checklist

1. Install dependencies:

  ```sh
  shards install
  ```

2. Compile library and example:

  ```sh
  crystal build src/read_until.cr
  crystal build examples/basic.cr
  ```

3. Optional spec check:

  ```sh
  crystal spec spec/read_until_spec.cr
  ```

## Development

During local workspace development, read_until depends on minknow via a local
path dependency, and minknow depends on the local grpc/proto shards.

## Contributing

1. Fork it (<https://github.com/your-github-user/read_until/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [kojix2](https://github.com/your-github-user) - creator and maintainer
