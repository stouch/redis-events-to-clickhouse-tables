import Queue from "bull";

const queue = new Queue("event-track", "redis://127.0.0.1:6379");

queue.add({
  event_type: "event_session",
  toto: true,
  zozo: "yolo",
});
