# Wikimedia Stream → OpenSearch Data Pipeline

A real-time data pipeline that consumes the Wikimedia recent change stream, publishes events to Apache Kafka, and indexes them into OpenSearch for search and analytics.

---

## Architecture

```
Wikimedia EventStream  →  Kafka Producer  →  Kafka Broker  →  Kafka Consumer  →  OpenSearch
```

| Component | Role |
|---|---|
| **Wikimedia EventStream** | Source of real-time Wikipedia edit events via Server-Sent Events (SSE) |
| **Kafka Producer** | Reads from the Wikimedia SSE endpoint and publishes messages to a Kafka topic |
| **Kafka Broker** | Buffers and distributes messages between producer and consumer |
| **Kafka Consumer** | Subscribes to the Kafka topic and writes events into OpenSearch |
| **OpenSearch** | Stores and indexes the events for search and analytics |

---

## Tech Stack

- **Java**
- **Apache Kafka** – distributed event streaming platform
- **OpenSearch** – distributed search and analytics engine
- **Wikimedia EventStreams API** – public SSE stream of recent Wikipedia changes

---

## Getting Started

### Prerequisites

- Java 11+
- Docker & Docker Compose (for running Kafka and OpenSearch locally)
- Maven (or Gradle)

### Run Kafka & OpenSearch locally

```bash
docker-compose up -d
```

### Build the project

```bash
mvn clean install
```

### Run the Producer

```bash
mvn exec:java -Dexec.mainClass="com.yourpackage.WikimediaProducer"
```

### Run the Consumer

```bash
mvn exec:java -Dexec.mainClass="com.yourpackage.OpenSearchConsumer"
```

---

## Data Source

- **Wikimedia Recent Change Stream**: https://stream.wikimedia.org/v2/stream/recentchange
- **Live demo**: https://esjewett.github.io/wm-eventsource-demo/
- **CodePen demo**: https://codepen.io/Krinkle/pen/BwEKgW?editors=1010

Events include Wikipedia page edits, new pages, user registrations, and more — streamed in real time via the [EventStreams API](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams).

---

## Key Concepts Practiced

- Building an end-to-end **real-time data pipeline**
- Producing and consuming messages with the **Kafka Java client**
- Connecting to and indexing documents in **OpenSearch**
- Handling **Server-Sent Events (SSE)** streams in Java
- Kafka best practices: idempotent producers, consumer groups, offset management
