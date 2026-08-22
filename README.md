# InfoCenter

InfoCenter is a lightweight, in-memory pub/sub server written in Go. Publishers
send messages to topics over HTTP, and subscribers receive them in real time
using Server-Sent Events.

## Run

```sh
go run .
```

The server listens on `http://localhost:8080` by default.

## Usage

Subscribe to a topic:

```sh
curl -N http://localhost:8080/infocenter/news
```

Publish from another terminal:

```sh
curl -X POST --data 'Hello!' http://localhost:8080/infocenter/news
```

The subscriber receives:

```text
id: 1
event: msg
data: Hello!
```

Messages are delivered only to current subscribers. They are not stored or
replayed, and messages may be dropped for slow subscribers.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `8080` | HTTP server port |
| `SSE_TIMEOUT` | `30s` | Maximum SSE connection lifetime |
| `MAX_BODY_BYTES` | `1048576` | Maximum publish body size |
| `SHUTDOWN_TIMEOUT` | `5s` | Graceful shutdown timeout |
| `CHANNEL_BUFFER` | `16` | Message buffer per subscriber |

## Test

```sh
make test
```
