# Pigeon Broker

A fast, lightweight message broker designed for IoT devices

Current features:

*   Persistent Queues
*   Persistent Key Value Store

<br>

# Queues

## Push to queue

    POST /queue/publish/{topic}

- `200 OK` - Successfully published
- `500 Internal Server Error` - Failed to persist

## Consume from queue

    POST /queue/consume/{topic}

- `200 OK` - Message from queue
- `404 Not Found`

## Get queue length

    GET /queue/length/{topic}

- `200 OK` - Length of queue

## Get queues overview

    GET /queue/topics

- `200 OK` - JSON Objects of queues

<br>

# Key Value Store

## Set value

    POST /kv/{key}

- `200 OK` - Successfully inserted
- `500 Internal Server Error` - Failed to persist

## Get value

    GET /kv/{key}

- `200 OK` - Value
- `404 Not Found` - Key not found

## Delete value

    DELETE /kv/{key}

- `200 OK` - Key deleted
- `404 Not Found` - Key not found