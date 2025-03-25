# 🗄️ Redis bull jobs -> Clickhouse tables

## Prerequisites:

- You host a Redis server (or KeyDB server)
- You have a website for which you want to trace events in Clickhouse tables, of different and changing structures.
- You host a Clickhouse database that is going to receive your events.

## How to use it ?

We now gonna introduce how to setup a Nodejs worker/processor that is gonna read the Redis queue and gonna push the events in your Clickhouse database.

### 1. In your website/app :

Use [BullMQ](https://github.com/OptimalBits/bull) to fill the events redis queue:

```typescript
import Queue from "bull";
const queue = new Queue("event-track", "redis://<redis-server>:6379");

// Trace an *event*
queue.add({
  // These two keys are required
  event_type: "<some_event_name>",
  fired_at: new Date(),
  // .. and the event content:
  some_key: "Some information to log",
  some_key_about_a_number: 23,
  some_key_about_a_boolean: false,
  some_key_about_a_date: new Date(),
  some_key_about_a_date_string: "2045-12-01",
});
```

When you inject an _event_ job in the Redis queue (like in the above code), you can choose the destination Clickhouse table name that is going to receive this event by setting the `event_type` property.

The rest of the properties will be the columns of this Clickhouse table.

In the above example, if the Clickhouse table `<some_event_name>` already exists, but if some of the columns you sent don't exist yet in the table, then the table is altered so the inserted rows will have these columns.

### 2. Host this repo

Fork/checkout the repo, setup the `.env` file (using `.env.sample` docs) to connect the Redis server and the destination Clickhouse database.

Then, just run:

```bash
nvm use
npm ci
npm run buid

# You can use a nohup, or a docker container, to run this forever:
node build/src/main.js

# CTRL+C will gracefully kill the bulking waiting events, and re-enqueue them in Redis queues if they are not done.
```

In case you want to apply some transformation to your events, override the `src/transform.ts` file. (see docker-compose..yml)

**Alternative: Host this repo with Docker**

Check the _environment_ variables in `docker-compose.yml`, and just run:

```bash
docker compose up --force-recreate --build -d
```

In case you want to kill the container, use a high timeout :
```bash
docker stop -t 600 <container>
```

### 3. We're done 🥳, track your Clickhouse database events !
