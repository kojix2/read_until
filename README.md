# read_until.cr

[![Test](https://github.com/kojix2/read_until/actions/workflows/test.yml/badge.svg)](https://github.com/kojix2/read_until/actions/workflows/test.yml)
[![Lines of Code](https://img.shields.io/endpoint?url=https%3A%2F%2Ftokei.kojix2.net%2Fbadge%2Fgithub%2Fkojix2%2Fread_until%2Flines)](https://tokei.kojix2.net/github/kojix2/read_until)

Read Until sessions, signal buffers, classification filters and action tracking
for Crystal. Uses [minknow.cr](https://github.com/kojix2/minknow) for connectivity.

## Installation

Requires Crystal >= 1.20 and [libnghttp2](https://github.com/kojix2/grpc#installation).
Add to `shard.yml`, then run `shards install`:

```yaml
dependencies:
  read_until:
    github: kojix2/read_until
```

## Stream reads

```crystal
require "read_until"

manager = Minknow::Manager.new
begin
  position = manager.fetch_flow_cell_positions.first? || raise "No sequencing position"
  connection = manager.connect(position)
  client = ReadUntil::Client.new(connection)
  client.open(channels: 1..128,
    raw_data: ReadUntil::RawDataKind::Uncalibrated,
    mode: ReadUntil::StreamMode::OneChunk) do |session|
    session.each_read(batch_size: 16) do |read|
      puts "#{read.channel}: #{read.signal(Int16).size} samples"
    end
  end
ensure
  connection.try &.channel.close
  manager.channel.close
end
```

`Uncalibrated` supplies raw ADC values; the default `Calibrated` mode uses
`signal(Float32)` with the usual device formats. `OneChunk` returns the first
chunk per read; use `MultiChunk` for subsequent chunks.
`session.unblock(read, for: 100.milliseconds)` ejects a molecule;
`session.stop(read)` stops further signal delivery while sequencing continues.
`Client#open` closes the session when its block exits.

See [MinKNOW authentication](https://github.com/kojix2/minknow)
for TLS, CA and token configuration.

## Development

```sh
crystal spec
crystal run examples/basic.cr
```

`basic.cr` demonstrates a synthetic buffer without starting a live stream.

License: [MPL-2.0](LICENSE), including derived [Oxford Nanopore Read Until](https://github.com/nanoporetech/read_until_api) code.
